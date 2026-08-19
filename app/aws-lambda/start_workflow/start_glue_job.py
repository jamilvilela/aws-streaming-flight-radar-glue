"""Lambda invoked on a schedule to start the Glue full-load workflow.

A DMS replication task of type ``full-load-and-cdc`` does not stop after the
full load — it transitions to "Load complete, replication ongoing" and keeps
applying CDC. DMS therefore never emits the ``REPLICATION_TASK_STOPPED`` /
``FULL_LOAD_ONLY_FINISHED`` event that a full-load-only task produces, so the
previous event-driven rule never fired.

This function is invoked on a schedule (EventBridge). It queries the DMS
replication task and starts the Glue workflow once the full-load phase has
completed (``FullLoadProgressPercent == 100`` and no tables still loading).
A DynamoDB lock row (keyed by the task ARN) guarantees a single start.
"""

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")
dms = boto3.client("dms")

WORKFLOW_NAME = os.environ.get("GLUE_WORKFLOW_NAME", "glue-flight-radar-batch-workflow")
DMS_TASK_ARN = os.environ.get("DMS_REPLICATION_TASK_ARN", "")
LOCK_TABLE = os.environ.get("DMS_WORKFLOW_LOCK_TABLE", "glue-flight-radar-workflow-lock")

_FULL_LOAD_DONE = {
    "status": "ok",
    "message": "Full load already complete or workflow already started",
}


def _find_task() -> Optional[dict]:
    """Return the DMS replication task whose full-load phase must be watched."""
    if DMS_TASK_ARN:
        resp = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [DMS_TASK_ARN]}]
        )
        tasks = resp.get("ReplicationTasks", [])
        return tasks[0] if tasks else None
    tasks = dms.describe_replication_tasks().get("ReplicationTasks", [])
    if len(tasks) == 1:
        return tasks[0]
    if len(tasks) > 1:
        print("DMS_REPLICATION_TASK_ARN not set and multiple replication tasks "
              "exist — configure the variable to disambiguate")
    return None


def _full_load_complete(task: dict) -> bool:
    """True when the DMS task finished its full-load phase."""
    status = task.get("Status", "")
    stats = task.get("ReplicationTaskStats") or {}
    if status == "stopped" and "FULL_LOAD_ONLY_FINISHED" in (task.get("StopReason") or ""):
        return True
    if status == "running":
        progress = stats.get("FullLoadProgressPercent")
        tables_loading = stats.get("TablesLoading", 0)
        return progress is not None and progress == 100 and tables_loading == 0
    return False


def _acquire_lock(task_arn: str) -> bool:
    """Atomically claim the workflow start for this task. True if claimed."""
    table = boto3.resource("dynamodb").Table(LOCK_TABLE)
    try:
        table.put_item(
            Item={"task_arn": task_arn},
            ConditionExpression="attribute_not_exists(task_arn)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def lambda_handler(event: dict, context: object) -> dict:
    """Start the Glue workflow once, when the DMS full-load phase completes."""
    task = _find_task()
    if task is None:
        print("No DMS replication task found — skipping")
        return {"statusCode": 200, "body": json.dumps({"message": "no task found"})}

    task_arn = task["ReplicationTaskArn"]
    if not _full_load_complete(task):
        print(f"Full load not complete for {task_arn} — status={task.get('Status')}")
        return _FULL_LOAD_DONE

    if not _acquire_lock(task_arn):
        print(f"Workflow already started for {task_arn} — skipping")
        return _FULL_LOAD_DONE

    try:
        response = glue.start_workflow_run(Name=WORKFLOW_NAME)
        run_id = response["RunId"]
        print(f"Started Glue workflow '{WORKFLOW_NAME}' — run ID: {run_id}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Started workflow {WORKFLOW_NAME}",
                "runId": run_id,
            }),
        }
    except Exception as e:
        print(f"Failed to start Glue workflow '{WORKFLOW_NAME}': {e}")
        raise