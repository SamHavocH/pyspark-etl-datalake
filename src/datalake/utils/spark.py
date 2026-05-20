from pyspark.sql import SparkSession

from datalake.configs.settings import Settings


def build_spark_session(settings: Settings) -> SparkSession:
    """Create a local Spark session with deterministic settings for tests and Docker."""

    return (
        SparkSession.builder.appName(settings.project_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
