from pyspark.sql import SparkSession

def make_spark(app_name: str = "etl-automatizado"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
