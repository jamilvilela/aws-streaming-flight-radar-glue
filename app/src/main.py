"""
main.py — Entry point for the Spark Streaming Mini-Batch Job.

Reads source and target configs from S3, initiliases Spark with
performance configs from --conf, and runs the pipeline in
streaming mode (forEachBatch).
No Glue-specific APIs are used.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import SparkSession

from src.processor import Processor
from src.config import Config

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)


# ── Argument parsing ───────────────────────────────────────────────


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Spark Streaming Mini-Batch Job (flights DMS CDC)",
    )
    parser.add_argument(
        "--origins_s3_path",
        required=True,
        help="S3 path to origins.json (source config)",
    )
    parser.add_argument(
        "--target_s3_path",
        required=True,
        help="S3 path to target.json (destination table config)",
    )
    parser.add_argument(
        "--conf",
        default="",
        help="Spark config key=value pairs separated by space",
    )
    return parser.parse_args(argv)


# ── Spark configs parsed from --conf ───────────────────────────────


def _parse_conf(conf_str: str) -> dict[str, str]:
    """
    Parse a --conf string like 'key=val key=val' into a dict.

    The string is space-separated; each token must be in key=value format.
    """
    configs: dict[str, str] = {}
    if not conf_str:
        return configs
    for item in conf_str.strip().split():
        if "=" in item:
            key, value = item.split("=", 1)
            configs[key.strip()] = value.strip()
        else:
            logger.warning("Ignoring malformed conf entry: %s", item)
    return configs


def _init_spark(spark_configs: dict[str, str] | None = None) -> SparkSession:
    """
    Create and configure a SparkSession with the given configs.

    Configs are applied dynamically — no values are hardcoded.
    """
    builder = SparkSession.builder \
        .appName("glue-streaming-minibatch-dms") \
        .enableHiveSupport()

    if spark_configs:
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            logger.debug("Spark config: %s = %s", key, value)

    return builder.getOrCreate()


def main() -> None:
    """Application entry point."""
    logger.info("Initialising Spark Streaming Mini-Batch Job")

    # 1. Parse arguments
    try:
        args = _parse_args()
    except Exception as exc:
        logger.error("Failed to parse arguments: %s", exc)
        sys.exit(1)

    logger.info(
        "Arguments: origins=%s target=%s",
        args.origins_s3_path,
        args.target_s3_path,
    )

    # 2. Parse Spark configs from --conf and initialise Spark dynamically
    spark_configs = _parse_conf(args.conf)
    logger.info("Parsed %d Spark configs from --conf", len(spark_configs))
    spark = _init_spark(spark_configs)
    logger.info("Spark session created — version %s", spark.version)

    # 3. Load configuration (source + target from separate S3 files)
    try:
        config = Config.from_s3(args.origins_s3_path, args.target_s3_path)
        logger.info(
            "Configuration loaded: source=%s | target=%s.%s",
            config.source.source,
            config.target.database,
            config.target.table,
        )
    except Exception as exc:
        logger.error("Failed to load configuration: %s", exc)
        sys.exit(1)

    # 4. Create pipeline processor and run in streaming mode
    processor = Processor(spark)
    _run_streaming(spark, processor, config)

    logger.info("Pipeline completed successfully")


def _run_streaming(spark: SparkSession, processor: Processor, config: Config) -> None:
    """
    Run the pipeline in streaming mode using forEachBatch.

    Each micro-batch is processed through the full Processor pipeline.
    """
    from pyspark.sql import DataFrame

    source = config.source
    target = config.target

    stream_df = processor._reader.read(source)

    def process_batch(df: DataFrame, batch_id: int) -> None:
        """Process a single micro-batch through the pipeline."""
        logger.info("Processing batch_id=%d with %d rows", batch_id, df.count())
        try:
            # Run validation + write for this batch
            valid_df, rejects_df = processor._data_quality.validate(df, target, source)

            # Write rejected + valid data
            processor._writer.write_rejects(rejects_df, target)
            processor._writer.write(valid_df, target, source)
            logger.info("Batch %d processed successfully", batch_id)
        except Exception as exc:
            logger.error("Batch %d failed: %s", batch_id, exc, exc_info=True)
            raise

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .option("checkpointLocation", source.checkpoint_location)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
