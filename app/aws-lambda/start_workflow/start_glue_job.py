"""Lambda triggered by EventBridge to start the Glue full-load workflow.

The workflow starts the full-load batch job via an on-demand trigger, and
a conditional trigger starts the streaming CDC job once the batch succeeds.
Native Glue sequencing avoids polling in the Lambda and concurrent writes
to the same Delta tables.
"""

import json
import os

import boto3

glue = boto3.client("glue")

WORKFLOW_NAME = os.environ.get("GLUE_WORKFLOW_NAME", "glue-flight-radar-full-load-workflow")


def lambda_handler(event: dict, context: object) -> dict:
    """Start the Glue workflow and return the workflow run ID."""
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
