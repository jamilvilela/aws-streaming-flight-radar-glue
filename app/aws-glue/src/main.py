"""
main.py — Entry point for the Glue Job (multi-table batch + streaming).

Reads a single config.json from S3 containing all table definitions
(sources + targets), then runs in either:
- **Batch mode**: processes all tables sequentially in ``order``.
- **Streaming mode**: starts one streaming query per table concurrently.

No Glue-specific APIs are used.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from src.processor import Processor
from src.config import Config, SourceConfig, TargetConfig

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
        description="Glue Job — Multi-table Batch + Streaming DMS CDC",
    )
    parser.add_argument(
        "--config_s3_path",
        required=True,
        help="S3 path to config.json (all table definitions with sources + targets)",
    )
    parser.add_argument(
        "--conf",
        default="",
        help="Spark config key=value pairs separated by space",
    )
    parser.add_argument(
        "--mode",
        choices=["streaming", "batch"],
        default="streaming",
        help="Execution mode: 'batch' processes all tables sequentially, 'streaming' starts concurrent queries (default: streaming)",
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
        .appName("glue-flight-radar-full-load-cdc") \
        .enableHiveSupport()

    if spark_configs:
        for key, value in spark_configs.items():
            builder = builder.config(key, value)
            logger.debug("Spark config: %s = %s", key, value)

    return builder.getOrCreate()


def main() -> None:
    """Application entry point."""
    logger.info("Initialising Glue Job — multi-table batch / streaming")

    # 1. Parse arguments
    try:
        args = _parse_args()
    except Exception as exc:
        logger.error("Failed to parse arguments: %s", exc)
        sys.exit(1)

    logger.info(
        "Arguments: config=%s mode=%s",
        args.config_s3_path,
        args.mode,
    )

    # 2. Parse Spark configs from --conf and initialise Spark dynamically
    spark_configs = _parse_conf(args.conf)
    logger.info("Parsed %d Spark configs from --conf", len(spark_configs))
    spark = _init_spark(spark_configs)
    logger.info("Spark session created — version %s", spark.version)

    # 3. Load configuration (single config.json with all tables)
    try:
        config = Config.from_s3(args.config_s3_path)
        source_list = config.sources  # sorted by order
        logger.info(
            "Configuration loaded: %d table(s) to process",
            len(source_list),
        )
        for s in source_list:
            logger.info(
                "  [%d] %s → %s.%s",
                s.order, s.source, s.target.database, s.target.table,
            )
    except Exception as exc:
        logger.error("Failed to load configuration: %s", exc)
        sys.exit(1)

    # 4. Create pipeline processor
    processor = Processor(spark)

    # 5. Run in the requested mode
    if args.mode == "batch":
        _run_batch(spark, processor, source_list)
    else:
        _run_streaming(spark, processor, source_list)

    logger.info("Pipeline completed successfully")


def _run_streaming(
    spark: SparkSession,
    processor: Processor,
    sources: List[SourceConfig],
) -> None:
    """
    Start one streaming query per table, all running concurrently.

    Each table uses its own checkpoint location and CDC source path.
    The job stays alive until all queries terminate.
    """
    from pyspark.sql import DataFrame

    queries = []
    for source in sources:
        target = source.target
        logger.info(
            "Starting streaming for table %s → %s.%s",
            source.source, target.database, target.table,
        )

        stream_df = processor._reader.read(source, mode="streaming")

        # Closure captures current source + target
        def process_batch(
            df: DataFrame,
            batch_id: int,
            src: SourceConfig = source,
            tgt: TargetConfig = target,
        ) -> None:
            """Process a single micro-batch through the pipeline."""
            logger.info(
                "[%s] batch_id=%d with %d rows",
                src.source, batch_id, df.count(),
            )
            try:
                valid_df, rejects_df = processor._data_quality.validate(df, tgt, src)
                processor._writer.write_rejects(rejects_df, tgt)
                processor._writer.write(valid_df, tgt, src)
                logger.info("[%s] batch %d done", src.source, batch_id)
            except Exception as exc:
                logger.error(
                    "[%s] batch %d failed: %s",
                    src.source, batch_id, exc, exc_info=True,
                )
                raise

        query = (
            stream_df.writeStream
            .foreachBatch(process_batch)
            .outputMode("append")
            .trigger(processingTime="5 minutes")
            .option("checkpointLocation", source.checkpoint_location)
            .start()
        )
        queries.append(query)
        logger.info("Query started for %s (checkpoint: %s)", source.source, source.checkpoint_location)

    # Keep the job alive — await any termination
    for q in queries:
        q.awaitTermination()


def _run_batch(
    spark: SparkSession,
    processor: Processor,
    sources: List[SourceConfig],
) -> None:
    """
    Process all tables sequentially in batch mode.

    Each table is read, validated, written, and registered before
    moving to the next. Processing order follows the ``order`` field
    in config.json.
    """
    total = len(sources)
    logger.info("Running batch mode — %d table(s) to process", total)

    for idx, source in enumerate(sources, start=1):
        target = source.target
        logger.info(
            "[%d/%d] Processing table: %s → %s.%s",
            idx, total, source.source,
            target.database, target.table,
        )

        try:
            # Read all full-load files for this table
            raw_df = processor._reader.read(source, mode="batch")
            row_count = raw_df.count()
            logger.info(
                "[%d/%d] Read %d rows from %s",
                idx, total, row_count, source.source,
            )

            # Run full pipeline with the pre-read DataFrame
            processor.run(source, target, mode="batch", dataframe=raw_df)

            logger.info(
                "[%d/%d] Table %s completed successfully",
                idx, total, source.source,
            )
        except Exception as exc:
            logger.error(
                "[%d/%d] Table %s FAILED: %s — continuing with next table",
                idx, total, source.source, exc, exc_info=True,
            )
            # Continue with the next table so one failure doesn't
            # block the remaining tables.
            continue


if __name__ == "__main__":
    main()
