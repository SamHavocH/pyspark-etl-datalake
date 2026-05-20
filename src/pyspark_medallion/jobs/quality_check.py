
from pyspark_medallion.config import Settings
from pyspark_medallion.ingestion.bronze import ENTITIES
from pyspark_medallion.monitoring.logging import get_logger
from pyspark_medallion.quality.rules import duplicate_count
from pyspark_medallion.transformations.silver import PRIMARY_KEYS
from pyspark_medallion.utils.spark import create_spark_session


def run(settings: Settings) -> dict[str, int]:
    logger = get_logger(__name__)
    spark = create_spark_session(settings)
    metrics: dict[str, int] = {}
    try:
        for entity in ENTITIES:
            df = spark.read.parquet(str(settings.silver_dir / entity))
            row_count = df.count()
            duplicate_rows = duplicate_count(df, PRIMARY_KEYS[entity])
            null_keys = df.where(" OR ".join(f"{key} IS NULL OR {key} = ''" for key in PRIMARY_KEYS[entity])).count()
            metrics[f"{entity}_rows"] = row_count
            metrics[f"{entity}_duplicate_rows"] = duplicate_rows
            metrics[f"{entity}_null_key_rows"] = null_keys
            if duplicate_rows or null_keys:
                raise ValueError(f"quality gate failed for {entity}")

        rejected_rows = 0
        if settings.rejected_dir.exists():
            rejected_rows = spark.read.option("recursiveFileLookup", "true").parquet(str(settings.rejected_dir)).count()
        metrics["rejected_rows_total"] = rejected_rows
        logger.info("quality checks completed", extra={"event": "quality_check_completed", "metrics": metrics})
        return metrics
    finally:
        spark.stop()
