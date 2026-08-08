"""Lambda triggered by EventBridge to start the Glue full-load batch job."""

import json
import os

import boto3

glue = boto3.client("glue")

JOB_NAME = os.environ.get("GLUE_JOB_NAME", "glue-flight-radar-full-load")


def lambda_handler(event: dict, context: object) -> dict:
    """Start the Glue job and return the job run ID."""
    try:
        response = glue.start_job_run(JobName=JOB_NAME)
        job_run_id = response["JobRunId"]
        print(f"Started Glue job '{JOB_NAME}' — run ID: {job_run_id}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Started {JOB_NAME}",
                "jobRunId": job_run_id,
            }),
        }
    except Exception as e:
        print(f"Failed to start Glue job '{JOB_NAME}': {e}")
        raise
