"""
aws_helper.py — Reusable AWS service helper for Glue jobs.

Provides a thin wrapper around common boto3 operations used across
both batch and streaming modes, including S3 file management, Athena
query execution, and CloudWatch metrics.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import boto3
from botocore.config import Config as BotoConfig
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class AwsHelperError(Exception):
    """Raised when an AWS API call fails."""
    pass


class AwsHelper:
    """
    Reusable utility class for AWS operations needed by Glue jobs.

    Lazily initialises boto3 clients on first access. Designed to be
    stateless and decoupled from the rest of the job pipeline.

    Usage::

        helper = AwsHelper(region="us-east-1")
        df = helper.run_athena_query(spark, "SELECT * FROM table")
        helper.move_s3_objects(
            source_bucket="src",
            source_prefix="input/",
            dest_bucket="dst",
            dest_prefix="archive/",
            pattern=r".*\\.parquet$",
        )
    """

    def __init__(
        self,
        region: Optional[str] = None,
        profile: Optional[str] = None,
        retries: int = 3,
    ) -> None:
        """
        Initialise the helper.

        Args:
            region: AWS region name. If ``None``, uses the default
                session region.
            profile: Optional AWS profile name for local development.
            retries: Max retry attempts for API calls (default 3).
        """
        self._region = region
        self._profile = profile
        self._boto_config = BotoConfig(
            retries={"max_attempts": retries, "mode": "adaptive"},
        )

        self._s3: Any = None
        self._athena: Any = None
        self._glue: Any = None
        self._sts: Any = None
        self._cloudwatch: Any = None


    @property
    def s3(self) -> Any:
        """Lazy-initialised S3 client."""
        if self._s3 is None:
            self._s3 = self._client("s3")
        return self._s3

    @property
    def athena(self) -> Any:
        """Lazy-initialised Athena client."""
        if self._athena is None:
            self._athena = self._client("athena")
        return self._athena

    @property
    def glue(self) -> Any:
        """Lazy-initialised Glue client."""
        if self._glue is None:
            self._glue = self._client("glue")
        return self._glue

    @property
    def sts(self) -> Any:
        """Lazy-initialised STS client."""
        if self._sts is None:
            self._sts = self._client("sts")
        return self._sts

    @property
    def cloudwatch(self) -> Any:
        """Lazy-initialised CloudWatch client."""
        if self._cloudwatch is None:
            self._cloudwatch = self._client("cloudwatch")
        return self._cloudwatch


    def get_account_id(self) -> str:
        """Return the current AWS account ID via STS."""
        try:
            return self.sts.get_caller_identity()["Account"]
        except Exception as exc:
            raise AwsHelperError(f"Failed to get account ID: {exc}") from exc

    def run_athena_query(
        self,
        spark: SparkSession,
        query: str,
        database: Optional[str] = None,
        workgroup: str = "primary",
        location: Optional[str] = None,
    ) -> DataFrame:
        """
        Execute an Athena SQL query and return the result as a Spark DataFrame.

        Args:
            spark: Active SparkSession.
            query: The SQL query to run.
            database: Optional database name. If provided, added as a
                ``\"database\"`` context to the query.
            workgroup: Athena workgroup (default ``\"primary\"``).
            location: Optional S3 location for query results. If not
                provided, the default workgroup output location is used.

        Returns:
            A Spark DataFrame with the query results.
        """
        import time

        try:
            kwargs: dict[str, Any] = {
                "QueryString": query,
                "WorkGroup": workgroup,
            }
            if database:
                kwargs["QueryExecutionContext"] = {"Database": database}

            if location:
                kwargs["ResultConfiguration"] = {"OutputLocation": location}

            response = self.athena.start_query_execution(**kwargs)
            query_execution_id = response["QueryExecutionId"]
            logger.info("Athena query started: %s", query_execution_id)

            max_wait = 300  # 5 minutes
            poll_interval = 2
            waited = 0

            while waited < max_wait:
                status_response = self.athena.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                state = status_response["QueryExecution"]["Status"]["State"]

                if state == "SUCCEEDED":
                    break
                elif state in ("FAILED", "CANCELLED"):
                    reason = status_response["QueryExecution"]["Status"].get(
                        "StateChangeReason", "Unknown"
                    )
                    raise AwsHelperError(
                        f"Athena query {state}: {reason}"
                    )

                time.sleep(poll_interval)
                waited += poll_interval

            if waited >= max_wait:
                raise AwsHelperError(
                    f"Athena query timed out after {max_wait}s"
                )

            result_location = status_response["QueryExecution"]["ResultConfiguration"][
                "OutputLocation"
            ]
            logger.info("Athena results at: %s", result_location)

            return spark.read.format("parquet").load(result_location)

        except AwsHelperError:
            raise
        except Exception as exc:
            raise AwsHelperError(f"Athena query failed: {exc}") from exc

    def move_s3_objects(
        self,
        source_bucket: str,
        source_prefix: str,
        dest_bucket: str,
        dest_prefix: str,
        pattern: Optional[str] = None,
        delete_source: bool = True,
    ) -> int:
        """
        Move (copy + optional delete) objects between S3 locations.

        Only objects whose key matches ``pattern`` (if provided) are
        moved. The source prefix is stripped and replaced with the
        destination prefix.

        Args:
            source_bucket: Source bucket name.
            source_prefix: Source key prefix (e.g. ``\"input/\"``).
            dest_bucket: Destination bucket name.
            dest_prefix: Destination key prefix (e.g. ``\"archive/\"``).
            pattern: Optional regex pattern. Only keys matching this
                pattern are moved (e.g. ``r\".*\\.parquet$\"``).
            delete_source: Whether to delete the source object after
                copy (default ``True``).

        Returns:
            Number of objects successfully moved.
        """
        import time

        compiled_pattern = re.compile(pattern) if pattern else None
        moved = 0

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

            for page in pages:
                for obj in page.get("Contents", []):
                    source_key = obj["Key"]

                    if compiled_pattern and not compiled_pattern.search(source_key):
                        continue

                    relative_key = source_key[len(source_prefix):].lstrip("/")
                    dest_key = f"{dest_prefix.rstrip('/')}/{relative_key}"

                    self.s3.copy_object(
                        CopySource={"Bucket": source_bucket, "Key": source_key},
                        Bucket=dest_bucket,
                        Key=dest_key,
                    )

                    if delete_source:
                        self.s3.delete_object(Bucket=source_bucket, Key=source_key)

                    moved += 1

                    logger.debug(
                        "Moved s3://%s/%s -> s3://%s/%s",
                        source_bucket,
                        source_key,
                        dest_bucket,
                        dest_key,
                    )

            logger.info(
                "Moved %d objects from s3://%s/%s to s3://%s/%s",
                moved,
                source_bucket,
                source_prefix,
                dest_bucket,
                dest_prefix,
            )
            return moved

        except Exception as exc:
            raise AwsHelperError(f"S3 move failed: {exc}") from exc

    def put_metric(
        self,
        namespace: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
        dimensions: Optional[list[dict[str, str]]] = None,
    ) -> None:
        """
        Publish a custom CloudWatch metric.

        Args:
            namespace: CloudWatch namespace (e.g. ``\"Glue/Jobs\"``).
            metric_name: Metric name.
            value: Metric value.
            unit: Unit (default ``\"Count\"``).
            dimensions: Optional list of dimension dicts, e.g.
                ``[{\"Name\": \"JobName\", \"Value\": \"my-job\"}]``.
        """
        try:
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=[
                    {
                        "MetricName": metric_name,
                        "Value": value,
                        "Unit": unit,
                        "Dimensions": dimensions or [],
                    }
                ],
            )
            logger.debug(
                "Metric %s/%s = %s %s published",
                namespace,
                metric_name,
                value,
                unit,
            )
        except Exception as exc:
            logger.warning("Failed to publish metric: %s", exc)


    def _client(self, service: str) -> Any:
        """Create a boto3 client using configured region/profile."""
        session = boto3.Session(profile_name=self._profile)
        return session.client(
            service,
            region_name=self._region,
            config=self._boto_config,
        )
