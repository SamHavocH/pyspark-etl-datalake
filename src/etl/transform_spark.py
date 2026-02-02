from pyspark.sql import functions as F
from pyspark.sql import types as T

# Primeiro lemos ts_utc como STRING, depois convertemos no Spark
schema = T.StructType([
    T.StructField("ts_utc_raw", T.StringType(), False),
    T.StructField("temperature_2m", T.DoubleType(), True),
    T.StructField("relative_humidity_2m", T.DoubleType(), True),
    T.StructField("precipitation", T.DoubleType(), True),
])

def transform_openmeteo_spark(spark, payload: dict):
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    hums = hourly.get("relative_humidity_2m", [])
    precs = hourly.get("precipitation", [])

    rows = []
    for i, t in enumerate(times):
        rows.append((
            t,  # "2026-01-25T00:00"
            float(temps[i]) if i < len(temps) and temps[i] is not None else None,
            float(hums[i]) if i < len(hums) and hums[i] is not None else None,
            float(precs[i]) if i < len(precs) and precs[i] is not None else None,
        ))

    df = spark.createDataFrame(rows, schema=schema)

    # Open-Meteo vem como "YYYY-MM-DDTHH:MM" (sem segundos)
    df = (
        df.withColumn("ts_utc", F.to_timestamp("ts_utc_raw", "yyyy-MM-dd'T'HH:mm"))
          .drop("ts_utc_raw")
          .withColumn("date_utc", F.to_date("ts_utc").cast("string"))
          .dropDuplicates(["ts_utc"])
          .orderBy("ts_utc")
    )

    return df
