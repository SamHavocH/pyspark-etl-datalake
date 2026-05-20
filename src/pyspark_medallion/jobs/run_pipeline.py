from pyspark_medallion.config import Settings
from pyspark_medallion.ingestion.synthetic import generate_ecommerce_dataset
from pyspark_medallion.jobs import build_gold, ingest_bronze, quality_check, transform_silver
from pyspark_medallion.monitoring.logging import get_logger


def run(settings: Settings) -> None:
    logger = get_logger(__name__)
    generate_ecommerce_dataset(settings.raw_dir)
    logger.info("sample data ready", extra={"event": "sample_data_generated"})
    ingest_bronze.run(settings)
    transform_silver.run(settings)
    build_gold.run(settings)
    quality_check.run(settings)
    logger.info("pipeline completed", extra={"event": "pipeline_completed"})
