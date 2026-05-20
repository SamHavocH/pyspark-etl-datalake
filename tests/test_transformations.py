from datalake.transformations.bronze import payload_to_bronze
from datalake.transformations.gold import build_daily_weather_summary
from datalake.transformations.silver import build_failed_records, build_silver


def test_bronze_payload_keeps_source_rows(spark, payload, settings):
    bronze_df = payload_to_bronze(spark, payload, settings, "test_run")

    assert bronze_df.count() == 4
    assert "observed_at_raw" in bronze_df.columns
    assert "run_id" in bronze_df.columns


def test_silver_cleans_invalid_rows_and_deduplicates(spark, payload, settings):
    bronze_df = payload_to_bronze(spark, payload, settings, "test_run")
    silver_df = build_silver(bronze_df)
    failed_df = build_failed_records(bronze_df)

    assert silver_df.count() == 2
    assert failed_df.count() == 1
    assert silver_df.select("location_id", "observed_at_utc").distinct().count() == 2


def test_gold_daily_summary_aggregates_weather_metrics(spark, payload, settings):
    bronze_df = payload_to_bronze(spark, payload, settings, "test_run")
    silver_df = build_silver(bronze_df)

    result = build_daily_weather_summary(silver_df).collect()[0].asDict()

    assert result["observation_count"] == 2
    assert result["total_precipitation_mm"] == 1.2
    assert result["weather_profile"] == "rain"
