from pyspark.sql import SparkSession

def make_spark(app_name: str = "pyspark-etl-datalake") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark
