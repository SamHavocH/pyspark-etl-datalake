from datetime import UTC, datetime, timedelta
from hashlib import sha1

from datalake.configs.settings import Settings, get_settings
from datalake.ingestion.open_meteo import fetch_open_meteo, persist_raw_payload
from datalake.jobs.analytics import build_duckdb_serving_layer
from datalake.jobs.io import read_parquet_if_exists, write_partitioned
from datalake.monitoring.metrics import MetricsRecorder, metrics_as_log_props
from datalake.monitoring.quality import raise_on_quality_failure, validate_silver
from datalake.transformations.bronze import payload_to_bronze
from datalake.transformations.gold import build_daily_weather_summary
from datalake.transformations.silver import build_failed_records, build_silver
from datalake.utils.logging import configure_logging, props
from datalake.utils.spark import build_spark_session
from datalake.utils.state import read_state, write_state


def run_pipeline(settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    logger = configure_logging(settings.log_level, settings.logs_dir, settings.project_name)
    window_start, window_end = _resolve_window(settings)
    run_id = _build_run_id(settings.location_id, window_start.isoformat(), window_end.isoformat())
    metrics = MetricsRecorder(run_id)

    logger.info(
        "pipeline_start",
        extra=props(
            run_id=run_id,
            environment=settings.environment,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
            location_id=settings.location_id,
        ),
    )

    spark = None
    try:
        spark = build_spark_session(settings)
        spark.sparkContext.setLogLevel("WARN")

        payload = fetch_open_meteo(settings, window_start.date(), window_end.date())
        raw_path = persist_raw_payload(payload, settings.raw_dir, run_id)
        logger.info("raw_payload_persisted", extra=props(path=raw_path))

        bronze_df = payload_to_bronze(spark, payload, settings, run_id)
        bronze_count = bronze_df.count()
        metrics.set_rows("bronze", bronze_count)
        write_partitioned(bronze_df, settings.bronze_dir, ["run_id"])

        failed_df = build_failed_records(bronze_df)
        failed_count = failed_df.count()
        metrics.set_rows("failed", failed_count)
        if failed_count:
            write_partitioned(failed_df, settings.failed_records_dir / "weather_hourly", ["run_id"])

        new_silver_df = build_silver(bronze_df)
        existing_silver_df = read_parquet_if_exists(spark, settings.silver_dir)
        silver_df = (
            new_silver_df
            if existing_silver_df is None
            else existing_silver_df.unionByName(new_silver_df).dropDuplicates(["location_id", "observed_at_utc"])
        )
        silver_df = silver_df.cache()
        silver_count = silver_df.count()
        metrics.set_rows("silver", silver_count)

        quality_results = validate_silver(silver_df)
        metrics.set_quality({result.name: result.failed_rows for result in quality_results})
        raise_on_quality_failure(quality_results)
        write_partitioned(silver_df, settings.silver_dir, ["year", "month", "date_utc"])

        gold_df = build_daily_weather_summary(silver_df)
        gold_df = gold_df.cache()
        gold_count = gold_df.count()
        metrics.set_rows("gold_weather_daily_summary", gold_count)
        write_partitioned(gold_df, settings.gold_dir / "weather_daily_summary", ["year", "month"])

        build_duckdb_serving_layer(settings.duckdb_path, settings.silver_dir, settings.gold_dir)
        write_state(settings.state_file, datetime.now(UTC), run_id)

        finished = metrics.finish("success")
        metrics_path = metrics.write(settings.metrics_dir)
        logger.info("pipeline_success", extra=props(metrics_path=metrics_path, **metrics_as_log_props(finished)))
        return 0
    except Exception as exc:
        finished = metrics.finish("failed", exc)
        metrics_path = metrics.write(settings.metrics_dir)
        logger.exception("pipeline_failed", extra=props(metrics_path=metrics_path, **metrics_as_log_props(finished)))
        return 1
    finally:
        if spark is not None:
            spark.stop()


def _resolve_window(settings: Settings) -> tuple[datetime, datetime]:
    state = read_state(settings.state_file)
    end_dt = datetime.now(UTC)
    last_success = state.get("last_success_utc")
    if last_success:
        start_dt = datetime.fromisoformat(last_success) - timedelta(days=settings.overlap_days)
    else:
        start_dt = end_dt - timedelta(days=settings.days_back)
    return start_dt, end_dt


def _build_run_id(location_id: str, start: str, end: str) -> str:
    digest = sha1(f"{location_id}|{start}|{end}".encode()).hexdigest()[:12]
    return f"open_meteo_{location_id}_{digest}"
