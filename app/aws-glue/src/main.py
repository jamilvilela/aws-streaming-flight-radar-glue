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
from datetime import datetime, timezone
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
    from src.dependencies.test_rejected_records import add_test_rejected_records_arg

    parser = argparse.ArgumentParser(
        description="Glue Job — Multi-table Batch + Streaming CDC",
    )
    parser.add_argument(
        "--config_s3_path",
        required=True,
        help="S3 path to config.json (all table definitions with sources + targets)",
    )
    parser.add_argument(
        "--mode",
        choices=["streaming", "batch"],
        default="streaming",
        help="Execution mode: 'batch' processes all tables sequentially, 'streaming' starts concurrent queries (default: streaming)",
    )
    add_test_rejected_records_arg(parser)
    args, _unknown = parser.parse_known_args(argv)
    return args


def _init_spark() -> SparkSession:
    """
    Create and configure a SparkSession with Delta support.

    IMPORTANT: In AWS Glue, the SparkSession should be configured primarily
    via the job's ``--conf`` parameters (applied by the Glue runtime BEFORE
    this script runs). This function should only set configs that are NOT
    provided via job parameters, or use ``getOrCreate()`` to attach to the
    existing session created by the runtime.

    The ``--datalake-formats=delta`` job parameter adds the Delta JAR to the
    classpath. The Delta extensions and catalog MUST be set via ``--conf``:
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
      --conf spark.sql.catalogImplementation=hive
      --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory

    This function uses ``getOrCreate()`` to attach to the session created by
    the Glue runtime (which already has the ``--conf`` applied), rather than
    creating a new session that would override those configs.
    """
    spark = SparkSession.builder.appName("glue-flight-radar").getOrCreate()

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
        "Arguments: config=%s mode=%s generate_test_rejects=%s",
        args.config_s3_path,
        args.mode,
        args.generate_test_rejects,
    )

    spark = _init_spark()
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

    # Generate test rejected records if requested
    if args.generate_test_rejects:
        _generate_test_rejected_records(spark, source_list, args.test_reject_reason)

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


def _generate_test_rejected_records(
    spark: SparkSession,
    sources: List[SourceConfig],
    reject_reason: str,
) -> None:
    """Generate test rejected records for all tables via the full DataQuality pipeline.
    
    This generates test records and passes them through the full processor pipeline
    (DataQuality validation -> RejectedRecords -> Writer), so they exercise the
    complete validation pipeline including null_check, type_cast, enum_check, etc.
    """
    logger.info("Generating test rejected records via DataQuality pipeline...")
    from src.dependencies.test_rejected_records import TestRejectedRecordsGenerator
    from src.dependencies.processor import Processor

    generator = TestRejectedRecordsGenerator(spark)
    processor = Processor(spark)
    execution_id_base = f"test-rejects-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    try:
        total_records = 0
        for source in sources:
            target = source.target
            logger.info("Processing test records for %s via DataQuality pipeline...", source.source)
            
            # Generate test DataFrame
            test_df = generator.create_test_dataframe(source)
            if test_df.isEmpty():
                logger.info("  No test records for %s", source.source)
                continue
            
            execution_id = f"{execution_id_base}-{source.source}"
            
            # Run through full processor pipeline (DataQuality -> RejectedRecords -> Writer)
            processor.run(source, target, mode="batch", dataframe=test_df)
            
            count = test_df.count()
            total_records += count
            logger.info("  %s: %d test records processed via DataQuality pipeline", source.source, count)

        logger.info(
            "Test rejected records generation completed: %d total records across %d tables",
            total_records, len(sources)
        )
    except Exception as exc:
        logger.error("Failed to generate test rejected records via pipeline: %s", exc, exc_info=True)
        raise


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
            # Avoid df.count() in streaming for low latency - log batch_id only
            logger.info(
                "[%s] batch_id=%d started",
                src.source, batch_id,
            )
            try:
                valid_df, rejects_df = processor._data_quality.validate(df, tgt, src)
                
                # Write rejects to centralized table
                if not rejects_df.isEmpty():
                    reject_rule = "validation_failed"
                    reject_reason = "Record failed data quality validation"
                    try:
                        rule_col = [c for c in rejects_df.columns if c.startswith("_reject_rule")]
                        if rule_col:
                            rule_row = rejects_df.select(rule_col[0]).first()
                            if rule_row and rule_row[0]:
                                reject_rule = rule_row[0]
                    except Exception:
                        pass
                    processor._rejected_records.write(
                        rejected_df=rejects_df,
                        source=src,
                        target=tgt,
                        execution_id=f"stream-{src.source}-{batch_id}",
                        reject_rule=reject_rule,
                        reject_reason=reject_reason,
                    )
                
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
