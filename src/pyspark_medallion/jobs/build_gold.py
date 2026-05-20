from pyspark_medallion.config import Settings
from pyspark_medallion.monitoring.logging import get_logger
from pyspark_medallion.monitoring.metrics import JobMetrics, timed_stage
from pyspark_medallion.transformations.gold import build_gold_tables
from pyspark_medallion.utils.spark import create_spark_session


def run(settings: Settings) -> dict[str, int]:
    logger = get_logger(__name__)
    metrics = JobMetrics("build_gold")
    spark = create_spark_session(settings)
    try:
        with timed_stage(metrics, "gold_build"):
            counts = build_gold_tables(spark, silver_dir=settings.silver_dir, gold_dir=settings.gold_dir)
        metrics.values.update({f"{table}_rows": count for table, count in counts.items()})
        logger.info("gold build completed", extra={"event": "job_completed", "metrics": metrics.values})
        return counts
    finally:
        spark.stop()
