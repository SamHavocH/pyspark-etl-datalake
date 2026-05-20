from pyspark.sql import types as T

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.StringType(), False),
        T.StructField("latitude", T.DoubleType(), False),
        T.StructField("longitude", T.DoubleType(), False),
        T.StructField("source_timezone", T.StringType(), True),
        T.StructField("observed_at_raw", T.StringType(), False),
        T.StructField("temperature_c", T.DoubleType(), True),
        T.StructField("relative_humidity_pct", T.DoubleType(), True),
        T.StructField("precipitation_mm", T.DoubleType(), True),
        T.StructField("wind_speed_kmh", T.DoubleType(), True),
        T.StructField("ingested_at_utc", T.TimestampType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("ingestion_date", T.StringType(), False),
    ]
)
