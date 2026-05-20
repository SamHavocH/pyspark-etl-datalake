from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_weather_summary(silver_df: DataFrame) -> DataFrame:
    """Aggregate hourly weather observations into a daily analytics table."""

    return (
        silver_df.groupBy("location_id", "date_utc", "year", "month")
        .agg(
            F.count("*").alias("observation_count"),
            F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
            F.round(F.min("temperature_c"), 2).alias("min_temperature_c"),
            F.round(F.max("temperature_c"), 2).alias("max_temperature_c"),
            F.round(F.avg("relative_humidity_pct"), 2).alias("avg_relative_humidity_pct"),
            F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
            F.round(F.max("wind_speed_kmh"), 2).alias("max_wind_speed_kmh"),
        )
        .withColumn(
            "weather_profile",
            F.when(F.col("total_precipitation_mm") >= 20, F.lit("heavy_rain"))
            .when(F.col("total_precipitation_mm") > 0, F.lit("rain"))
            .when(F.col("avg_temperature_c") >= 28, F.lit("hot_dry"))
            .otherwise(F.lit("mild")),
        )
        .orderBy("location_id", "date_utc")
    )
