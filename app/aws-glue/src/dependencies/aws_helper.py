"""
aws_helper.py — Reusable AWS service helper for Glue jobs.

Provides a thin wrapper around common boto3 operations used across
both batch and streaming modes, including S3 file management, Athena
query execution, and CloudWatch metrics.
"""

from __future__ import annotations

import json
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

    Lazily initializes boto3 clients on first access. Designed to be
    stateless and decoupled from the rest of the job pipeline.

    Usage::

        helper = AwsHelper(region="us-east-1")
        df = helper.run_athena_query(spark, "SELECT * FROM table")
helper.move_s3_objects(
            source_bucket="src",
            source_prefix="input/",
            dest_bucket="dst",
            dest_prefix="archive/",
            pattern=r".*\.parquet$",
        )
    """

    def __init__(
        self,
        region: Optional[str] = None,
        profile: Optional[str] = None,
        retries: int = 3,
    ) -> None:
        """Initialize the helper.

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
        """Lazy-initialized S3 client."""
        if self._s3 is None:
            self._s3 = self._client("s3")
        return self._s3

    @property
    def athena(self) -> Any:
        """Lazy-initialized Athena client."""
        if self._athena is None:
            self._athena = self._client("athena")
        return self._athena

    @property
    def glue(self) -> Any:
        """Lazy-initialized Glue client."""
        if self._glue is None:
            self._glue = self._client("glue")
        return self._glue

    @property
    def sts(self) -> Any:
        """Lazy-initialized STS client."""
        if self._sts is None:
            self._sts = self._client("sts")
        return self._sts

    @property
    def cloudwatch(self) -> Any:
        """Lazy-initialized CloudWatch client."""
        if self._cloudwatch is None:
            self._cloudwatch = self._client("cloudwatch")
        return self._cloudwatch

    def get_account_id(self) -> str:
        """Return the current AWS account ID via STS.

        Returns:
            AWS account ID string.

        Raises:
            AwsHelperError: If the STS call fails.
        """
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
        """Execute an Athena SQL query and return the result as a Spark DataFrame.

        Args:
            spark: Active SparkSession.
            query: The SQL query to run.
            database: Optional database name. If provided, added as a
                ``"database"`` context to the query.
            workgroup: Athena workgroup (default ``"primary"``).
            location: Optional S3 location for query results. If not
                provided, the default workgroup output location is used.

        Returns:
            A Spark DataFrame with the query results.

        Raises:
            AwsHelperError: If the query fails or times out.
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
        """Move (copy + optional delete) objects between S3 locations.

        Only objects whose key matches ``pattern`` (if provided) are
        moved. The source prefix is stripped and replaced with the
        destination prefix.

        Args:
            source_bucket: Source bucket name.
            source_prefix: Source key prefix (e.g. ``"input/"``).
            dest_bucket: Destination bucket name.
            dest_prefix: Destination key prefix (e.g. ``"archive/"``).
            pattern: Optional regex pattern. Only keys matching this
                pattern are moved (e.g. ``r".*\.parquet$"``).
            delete_source: Whether to delete the source object after
                copy (default ``True``).

        Returns:
            Number of objects successfully moved.

        Raises:
            AwsHelperError: If the S3 operation fails.
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
        """Publish a custom CloudWatch metric.

        Args:
            namespace: CloudWatch namespace (e.g. ``"Glue/Jobs"``).
            metric_name: Metric name.
            value: Metric value.
            unit: Unit (default ``"Count"``).
            dimensions: Optional list of dimension dicts, e.g.
                ``[{"Name": "JobName", "Value": "my-job"}]``.
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

    def get_json_from_s3(self, s3_path: str) -> dict:
        """Load and parse a JSON file from S3.

        Args:
            s3_path: S3 URI like ``s3://bucket/key/config.json``.

        Returns:
            Parsed JSON as a dict.

        Raises:
            AwsHelperError: If the object is not found or cannot be parsed.
        """
        try:
            bucket, key = self._parse_s3_path(s3_path)
            if not bucket or not key:
                raise AwsHelperError(f"Invalid S3 path: {s3_path}")
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode("utf-8")
            return json.loads(content)
        except self.s3.exceptions.NoSuchKey as exc:
            raise AwsHelperError(f"S3 object not found: {s3_path}") from exc
        except json.JSONDecodeError as exc:
            raise AwsHelperError(f"Invalid JSON in {s3_path}: {exc}") from exc
        except Exception as exc:
            raise AwsHelperError(f"Failed to load JSON from S3: {exc}") from exc

    @staticmethod
    def _parse_s3_path(s3_path: str) -> tuple[str, str]:
        """Parse ``s3://bucket/key`` into ``(bucket, key)``.

        Args:
            s3_path: S3 URI.

        Returns:
            Tuple of (bucket, key).
        """
        if not s3_path.startswith("s3://"):
            return "", ""
        path = s3_path.replace("s3://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    def get_table_location(self, database: str, table: str) -> str:
        """Get the S3 location of a Glue Catalog table.

        Args:
            database: Database name in Glue Catalog.
            table: Table name in Glue Catalog.

        Returns:
            S3 location string (e.g. ``s3://bucket/path/``).

        Raises:
            AwsHelperError: If table not found or has no location.
        """
        try:
            response = self.glue.get_table(DatabaseName=database, Name=table)
            table_data = response["Table"]
            location = table_data.get("StorageDescriptor", {}).get("Location")
            if not location:
                raise AwsHelperError(
                    f"Glue Catalog table has no S3 location: {database}.{table}"
                )
            return location
        except self.glue.exceptions.EntityNotFoundException as exc:
            raise AwsHelperError(f"Glue table not found: {database}.{table}") from exc
        except Exception as exc:
            raise AwsHelperError(f"Failed to get table location: {exc}") from exc

    def update_table_partitions(self, database: str, table: str) -> None:
        """Update table partitions in Glue Catalog by discovering S3 partitions
        and creating missing ones via Glue batch_create_partition API.

        This should be called after ``DataFrame.write.save()`` when writing partitioned
        data to S3, to make the new partitions visible in the Glue Catalog.

        Args:
            database: Database name in Glue Catalog.
            table: Table name in Glue Catalog.

        Raises:
            AwsHelperError: If the partition update fails.
        """
        logger.info("Updating partitions for %s.%s via Glue Catalog API", database, table)

        try:
            # Get table metadata including partition keys and location
            table_response = self.glue.get_table(DatabaseName=database, Name=table)
            table_data = table_response["Table"]

            partition_keys = table_data.get("PartitionKeys", [])
            if not partition_keys:
                logger.info("Table %s.%s has no partition keys, skipping", database, table)
                return

            location = table_data.get("StorageDescriptor", {}).get("Location")
            if not location:
                raise AwsHelperError(f"Table {database}.{table} has no S3 location")

            # Get existing partitions from Glue
            existing_partitions = set()
            paginator = self.glue.get_paginator("get_partitions")
            for page in paginator.paginate(DatabaseName=database, TableName=table):
                for part in page.get("Partitions", []):
                    values = tuple(part["Values"])
                    existing_partitions.add(values)

            # Discover partition directories in S3
            s3_partitions = self._discover_s3_partitions(location, partition_keys)

            # Find missing partitions
            missing_partitions = s3_partitions - existing_partitions

            if not missing_partitions:
                logger.info("No new partitions to add for %s.%s", database, table)
                return

            # Create missing partitions in Glue
            partition_inputs = []
            for values in sorted(missing_partitions):
                partition_input = self._build_partition_input(table_data, values)
                partition_inputs.append(partition_input)

            # Batch create partitions (max 100 per batch)
            batch_size = 100
            for i in range(0, len(partition_inputs), batch_size):
                batch = partition_inputs[i:i + batch_size]
                self.glue.batch_create_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionInputList=batch
                )

            logger.info(
                "Added %d new partitions to %s.%s",
                len(missing_partitions), database, table
            )

        except self.glue.exceptions.EntityNotFoundException as exc:
            raise AwsHelperError(f"Glue table not found: {database}.{table}") from exc
        except AwsHelperError:
            raise
        except Exception as exc:
            raise AwsHelperError(f"Failed to update partitions: {exc}") from exc

    def _discover_s3_partitions(self, location: str, partition_keys: list) -> set:
        """Discover partition directories in S3 and return as set of value tuples.

        Args:
            location: S3 location of the table.
            partition_keys: List of partition key definitions from Glue.

        Returns:
            Set of partition value tuples.
        """
        bucket, prefix = self._parse_s3_path(location)
        if not bucket:
            return set()

        # Ensure prefix ends with /
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        s3_partitions = set()
        partition_names = [pk["Name"] for pk in partition_keys]

        # Use S3 list_objects_v2 to find partition directories
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
            for prefix_obj in page.get("CommonPrefixes", []):
                prefix_path = prefix_obj["Prefix"]
                # Extract partition values from path
                rel_path = prefix_path[len(prefix):].rstrip("/")
                if not rel_path:
                    continue

                parts = rel_path.split("/")
                values = []
                for part in parts:
                    if "=" in part:
                        _, value = part.split("=", 1)
                        values.append(value)
                    else:
                        # Handle legacy format without explicit key
                        values.append(part)

                if len(values) == len(partition_names):
                    s3_partitions.add(tuple(values))

        return s3_partitions

    def _build_partition_input(self, table_data: dict, values: tuple) -> dict:
        """Build a PartitionInput dict for Glue batch_create_partition.

        Args:
            table_data: Table metadata from Glue.
            values: Partition values tuple.

        Returns:
            PartitionInput dict for batch_create_partition.
        """
        storage_descriptor = table_data.get("StorageDescriptor", {}).copy()
        # Update location to point to the specific partition
        base_location = storage_descriptor.get("Location", "")
        if base_location:
            part_path = "/".join(f"{pk['Name']}={val}" for pk, val in zip(
                table_data.get("PartitionKeys", []), values
            ))
            storage_descriptor["Location"] = f"{base_location.rstrip('/')}/{part_path}/"

        return {
            "Values": list(values),
            "StorageDescriptor": storage_descriptor,
            "Parameters": table_data.get("Parameters", {}),
        }

    def _client(self, service: str) -> Any:
        """Create a boto3 client using configured region/profile.

        Args:
            service: AWS service name.

        Returns:
            Configured boto3 client.
        """
        session = boto3.Session(profile_name=self._profile)
        return session.client(
            service,
            region_name=self._region,
            config=self._boto_config,
        )