from pyspark_medallion.config import Settings
from pyspark_medallion.monitoring.logging import get_logger
from pyspark_medallion.monitoring.metrics import JobMetrics, timed_stage
from pyspark_medallion.transformations.silver import build_all_silver
from pyspark_medallion.utils.spark import create_spark_session


def run(settings: Settings) -> dict[str, dict[str, int]]:
    logger = get_logger(__name__)
    metrics = JobMetrics("transform_silver")
    spark = create_spark_session(settings)
    try:
        with timed_stage(metrics, "silver_transform"):
            result = build_all_silver(
                spark,
                bronze_dir=settings.bronze_dir,
                silver_dir=settings.silver_dir,
                rejected_dir=settings.rejected_dir,
                report_dir=settings.quality_report_dir,
                bookmark_path=settings.bookmark_path,
            )
        for entity, entity_metrics in result.items():
            for key, value in entity_metrics.items():
                metrics.add(f"{entity}_{key}", value)
        logger.info("silver transform completed", extra={"event": "job_completed", "metrics": metrics.values})
        return result
    finally:
        spark.stop()
