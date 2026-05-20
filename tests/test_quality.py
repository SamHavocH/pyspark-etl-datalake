import pytest

from datalake.monitoring.quality import raise_on_quality_failure, validate_silver


def test_quality_checks_pass_for_valid_silver(spark, payload, settings):
    from datalake.transformations.bronze import payload_to_bronze
    from datalake.transformations.silver import build_silver

    silver_df = build_silver(payload_to_bronze(spark, payload, settings, "test_run"))

    results = validate_silver(silver_df)

    assert all(result.passed for result in results)


def test_quality_checks_fail_for_empty_dataframe(spark):
    schema = """
        observed_at_utc timestamp,
        relative_humidity_pct double,
        precipitation_mm double,
        temperature_c double
    """
    df = spark.createDataFrame([], schema)

    results = validate_silver(df)

    with pytest.raises(ValueError, match="silver_not_empty"):
        raise_on_quality_failure(results)
