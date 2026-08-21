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

from src.dependencies.processor import Processor
from src.dependencies.config import Config, SourceConfig, TargetConfig

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """Parse command line arguments.

    Uses ``parse_known_args`` because AWS Glue (>= 4.0) injects extra
    arguments into the script (e.g. ``--internal-lib-urls``) that are not
    declared here; rejecting them would fail the job at startup.
    """
    parser = argparse.ArgumentParser(
        description="Glue Job — Multi-table Batch + Streaming CDC",
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
    args, _unknown = parser.parse_known_args(argv)
    return args


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
    Create and configure a SparkSession with Delta support.

    In AWS Glue the SparkSession is normally created by the runtime BEFORE
    the script runs, so ``builder.config(...)`` would be ignored. However,
    in this job the session is created by the script's builder (observed:
    the SparkContext is submitted with the script's appName), which means
    ``--datalake-formats=delta`` only adds the Delta JAR to the classpath
    WITHOUT configuring the session extensions.

    The Delta extensions/catalog MUST be set at session bootstrap (they
    cannot be changed at runtime via ``spark.conf.set()``), so they are
    declared on the builder here. The session uses the Hive metastore
    (``spark.sql.catalogImplementation=hive``) and points its metastore
    client at the Glue Data Catalog via the
    ``AWSGlueDataCatalogHiveClientFactory`` — this is what makes the
    ``db_raw`` tables resolvable by name (``DeltaTable.forName``). Without
    the client factory the session falls back to an embedded Derby
    metastore, which has no ``db_raw`` schema. Runtime-settable configs
    from ``--conf`` are applied afterwards via ``spark.conf.set()``.
    """
    spark = (
        SparkSession.builder
        .appName("glue-flight-radar")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .getOrCreate()
    )

    # Diagnostic: log the Delta-related session configs so bootstrap issues
    # (e.g. missing DeltaSparkSessionExtension) are visible in CloudWatch.
    for key in (
        "spark.sql.extensions",
        "spark.sql.catalog.spark_catalog",
        "spark.sql.catalogImplementation",
        "spark.hadoop.hive.metastore.client.factory.class",
        "spark.delta.logStore.class",
        "spark.master",
    ):
        try:
            logger.info("Session config %s = %s", key, spark.conf.get(key))
        except Exception:  # noqa: BLE001 - config may not be set
            logger.info("Session config %s = <not set>", key)

    if spark_configs:
        for key, value in spark_configs.items():
            try:
                spark.conf.set(key, value)
                logger.debug("Spark config: %s = %s", key, value)
            except Exception as exc:  # noqa: BLE001 - runtime config may be read-only
                logger.warning("Could not apply Spark config %s: %s", key, exc)

    return spark


def main() -> None:
    """Application entry point."""
    logger.info("Initialising Glue Job — multi-table batch / streaming")

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

    spark_configs = _parse_conf(args.conf)
    logger.info("Parsed %d Spark configs from --conf", len(spark_configs))
    spark = _init_spark(spark_configs)
    logger.info("Spark session created — version %s", spark.version)

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

    processor = Processor(spark)

    try:
        if args.mode == "batch":
            _run_batch(spark, processor, source_list)
        else:
            _run_streaming(spark, processor, source_list)
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        # Exit non-zero so the Glue run is marked FAILED instead of
        # silently continuing / succeeding.
        sys.exit(1)

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
            .queryName(f"stream-{source.source}")
            .option("checkpointLocation", source.checkpoint_location)
            .start()
        )
        queries.append(query)
        logger.info("Query started for %s (checkpoint: %s)", source.source, source.checkpoint_location)

    # Keep the job alive — await any termination (returns when ANY query
    # terminates, e.g. on failure).
    try:
        spark.streams.awaitAnyTermination()
    except Exception as exc:
        logger.error("Streaming terminated with error: %s", exc, exc_info=True)
        for q in spark.streams.active:
            q.stop()
        raise

    # Fail-fast: if any query failed, stop all remaining queries and fail
    # the job so the Glue run is marked FAILED instead of hanging.
    for q in queries:
        q_exc = q.exception()
        if q_exc is not None:
            logger.error(
                "Streaming query %s failed: %s — stopping job (fail-fast)",
                q.name, q_exc, exc_info=True,
            )
            for active in spark.streams.active:
                active.stop()
            raise RuntimeError(f"Streaming query '{q.name}' failed: {q_exc}")


def _run_batch(
    spark: SparkSession,
    processor: Processor,
    sources: List[SourceConfig],
) -> None:
    """
    Process all tables sequentially in batch mode.

    Each table is read, validated, written, and registered before
    moving to the next. Processing order follows the ``order`` field
    in config.json. On the first failure the job stops (fail-fast) so
    the Glue run is marked FAILED instead of silently continuing.
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
            logger.info("[%d/%d] Read DataFrame for %s", idx, total, source.source)

            # Run full pipeline with the pre-read DataFrame
            processor.run(source, target, mode="batch", dataframe=raw_df)

            logger.info(
                "[%d/%d] Table %s completed successfully",
                idx, total, source.source,
            )
        except Exception as exc:
            logger.error(
                "[%d/%d] Table %s FAILED: %s — stopping job (fail-fast)",
                idx, total, source.source, exc, exc_info=True,
            )
            # Fail-fast: re-raise so the job stops on the first error
            # instead of continuing with the remaining tables.
            raise


if __name__ == "__main__":
    main()
