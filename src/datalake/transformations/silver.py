from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_silver(bronze_df: DataFrame) -> DataFrame:
    """Clean, standardize, and deduplicate hourly weather observations."""

    parsed = bronze_df.withColumn("observed_at_utc", F.to_timestamp("observed_at_raw", "yyyy-MM-dd'T'HH:mm"))
    return (
        parsed.filter(F.col("observed_at_utc").isNotNull())
        .filter(F.col("temperature_c").between(-80, 60) | F.col("temperature_c").isNull())
        .filter(F.col("relative_humidity_pct").between(0, 100) | F.col("relative_humidity_pct").isNull())
        .filter((F.col("precipitation_mm") >= 0) | F.col("precipitation_mm").isNull())
        .filter((F.col("wind_speed_kmh") >= 0) | F.col("wind_speed_kmh").isNull())
        .withColumn("date_utc", F.to_date("observed_at_utc"))
        .withColumn("year", F.year("observed_at_utc"))
        .withColumn("month", F.format_string("%02d", F.month("observed_at_utc")))
        .dropDuplicates(["location_id", "observed_at_utc"])
        .select(
            "location_id",
            "observed_at_utc",
            "date_utc",
            "year",
            "month",
            "latitude",
            "longitude",
            "temperature_c",
            "relative_humidity_pct",
            "precipitation_mm",
            "wind_speed_kmh",
            "source_timezone",
            "ingested_at_utc",
            "run_id",
        )
    )


def build_failed_records(bronze_df: DataFrame) -> DataFrame:
    """Return records rejected by silver validation with a human-readable reason."""

    parsed = bronze_df.withColumn("observed_at_utc", F.to_timestamp("observed_at_raw", "yyyy-MM-dd'T'HH:mm"))
    reason = (
        F.when(F.col("observed_at_utc").isNull(), F.lit("invalid_timestamp"))
        .when((F.col("temperature_c") < -80) | (F.col("temperature_c") > 60), F.lit("temperature_out_of_range"))
        .when(
            (F.col("relative_humidity_pct") < 0) | (F.col("relative_humidity_pct") > 100),
            F.lit("humidity_out_of_range"),
        )
        .when(F.col("precipitation_mm") < 0, F.lit("negative_precipitation"))
        .when(F.col("wind_speed_kmh") < 0, F.lit("negative_wind_speed"))
    )
    return parsed.withColumn("failure_reason", reason).filter(F.col("failure_reason").isNotNull())
