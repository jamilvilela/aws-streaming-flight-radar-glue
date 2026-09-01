"""Lambda invoked on a schedule to start the Glue full-load workflow.

A DMS Serverless replication of type ``full-load-and-cdc`` does not stop after
the full load — it transitions to "Load complete, replication ongoing" and
keeps applying CDC. DMS therefore never emits the ``REPLICATION_TASK_STOPPED`` /
``FULL_LOAD_ONLY_FINISHED`` event that a full-load-only task produces, so the
previous event-driven rule never fired.

This function is invoked on a schedule (EventBridge). It queries the DMS
Serverless replication (``describe_replications``) and starts the Glue workflow
once the full-load phase has completed (``FullLoadProgressPercent == 100`` and
no tables still loading). A DynamoDB lock row (keyed by the replication config
ARN) guarantees a single start.
"""

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")
dms = boto3.client("dms")

WORKFLOW_NAME = os.environ.get("GLUE_WORKFLOW_NAME", "glue-flight-radar-batch-workflow")
DMS_CONFIG_ARN = os.environ.get("DMS_REPLICATION_CONFIG_ARN", "")
LOCK_TABLE = os.environ.get("DMS_WORKFLOW_LOCK_TABLE", "glue-flight-radar-workflow-lock")

_FULL_LOAD_DONE = {
    "status": "ok",
    "message": "Full load already complete or workflow already started",
}


def _find_replication() -> Optional[dict]:
    """Return the DMS Serverless replication whose full-load phase is watched.

    Returns:
        Replication dict if found, None otherwise.
    """
    if DMS_CONFIG_ARN:
        resp = dms.describe_replications(
            Filters=[{"Name": "replication-config-arn", "Values": [DMS_CONFIG_ARN]}]
        )
        replications = resp.get("Replications", [])
        return replications[0] if replications else None
    replications = dms.describe_replications().get("Replications", [])
    if len(replications) == 1:
        return replications[0]
    if len(replications) > 1:
        print("DMS_REPLICATION_CONFIG_ARN not set and multiple replications "
              "exist — configure the variable to disambiguate")
    return None


def _full_load_complete(replication: dict) -> bool:
    """Check if the DMS Serverless replication finished its full-load phase.

    Args:
        replication: Replication dict from describe_replications.

    Returns:
        True if full load is complete (100% progress, 0 tables loading).
    """
    stats = replication.get("ReplicationStats") or {}
    progress = stats.get("FullLoadProgressPercent")
    tables_loading = stats.get("TablesLoading", 0)
    return progress is not None and progress == 100 and tables_loading == 0


def _acquire_lock(config_arn: str) -> bool:
    """Atomically claim the workflow start for this replication.

    Args:
        config_arn: DMS replication config ARN.

    Returns:
        True if lock acquired, False if already held.
    """
    table = boto3.resource("dynamodb").Table(LOCK_TABLE)
    try:
        table.put_item(
            Item={"task_arn": config_arn},
            ConditionExpression="attribute_not_exists(task_arn)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def lambda_handler(event: dict, context: object) -> dict:
    """Start the Glue workflow once, when the DMS full-load phase completes.

    Args:
        event: EventBridge event.
        context: Lambda context.

    Returns:
        Response dict with statusCode and body.
    """
    replication = _find_replication()
    if replication is None:
        print("No DMS Serverless replication found — skipping")
        return {"statusCode": 200, "body": json.dumps({"message": "no replication found"})}

    config_arn = replication["ReplicationConfigArn"]
    if not _full_load_complete(replication):
        print(f"Full load not complete for {config_arn} — status={replication.get('Status')}")
        return _FULL_LOAD_DONE

    if not _acquire_lock(config_arn):
        print(f"Workflow already started for {config_arn} — skipping")
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