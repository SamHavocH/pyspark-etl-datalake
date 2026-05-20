from datetime import UTC, datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from datalake.configs.settings import Settings
from datalake.transformations.schemas import BRONZE_SCHEMA


def payload_to_bronze(spark: SparkSession, payload: dict[str, Any], settings: Settings, run_id: str) -> DataFrame:
    """Convert the source JSON into a typed bronze DataFrame."""

    hourly = payload.get("hourly", {})
    timestamps = hourly.get("time", [])
    ingested_at = datetime.now(UTC)
    ingestion_date = ingested_at.date().isoformat()

    rows = []
    for index, observed_at in enumerate(timestamps):
        rows.append(
            (
                settings.location_id,
                float(payload.get("latitude", settings.latitude)),
                float(payload.get("longitude", settings.longitude)),
                payload.get("timezone", settings.timezone),
                observed_at,
                _get_float(hourly, "temperature_2m", index),
                _get_float(hourly, "relative_humidity_2m", index),
                _get_float(hourly, "precipitation", index),
                _get_float(hourly, "wind_speed_10m", index),
                ingested_at,
                run_id,
                ingestion_date,
            )
        )

    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


def _get_float(hourly: dict[str, list[Any]], key: str, index: int) -> float | None:
    values = hourly.get(key, [])
    if index >= len(values) or values[index] is None:
        return None
    return float(values[index])
