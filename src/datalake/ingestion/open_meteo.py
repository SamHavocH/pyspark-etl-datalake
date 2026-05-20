import json
from datetime import date
from pathlib import Path
from typing import Any

import requests

from datalake.configs.settings import Settings
from datalake.utils.retry import with_network_retry

HOURLY_VARIABLES = "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m"


@with_network_retry
def fetch_open_meteo(settings: Settings, start_date: date, end_date: date) -> dict[str, Any]:
    """Fetch hourly weather observations from Open-Meteo."""

    response = requests.get(
        settings.open_meteo_url,
        params={
            "latitude": settings.latitude,
            "longitude": settings.longitude,
            "hourly": HOURLY_VARIABLES,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": settings.timezone,
        },
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def persist_raw_payload(payload: dict[str, Any], raw_dir: Path, run_id: str) -> Path:
    """Store the exact source payload for replay and auditability."""

    output_dir = raw_dir / f"run_id={run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "payload.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path
