from pyspark_medallion.config import Settings
from pyspark_medallion.ingestion.bronze import ingest_raw_csv_to_bronze
from pyspark_medallion.monitoring.logging import get_logger
from pyspark_medallion.monitoring.metrics import JobMetrics, timed_stage
from pyspark_medallion.utils.spark import create_spark_session


def run(settings: Settings) -> dict[str, int]:
    logger = get_logger(__name__)
    metrics = JobMetrics("ingest_bronze")
    spark = create_spark_session(settings)
    try:
        with timed_stage(metrics, "bronze_ingestion"):
            counts = ingest_raw_csv_to_bronze(spark, settings.raw_dir, settings.bronze_dir)
        metrics.values.update({f"{entity}_rows": count for entity, count in counts.items()})
        logger.info("bronze ingestion completed", extra={"event": "job_completed", "metrics": metrics.values})
        return counts
    finally:
        spark.stop()
