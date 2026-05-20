from pathlib import Path

from pyspark.sql import SparkSession

from pyspark_medallion.config import Settings


def create_spark_session(settings: Settings) -> SparkSession:
    event_log_dir = Path(settings.data_root / "spark-events")
    event_log_dir.mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", str(settings.spark_warehouse_dir))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", str(event_log_dir))
        .getOrCreate()
    )
