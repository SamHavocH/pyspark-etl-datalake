import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"last_success_utc": None}
    return json.loads(path.read_text(encoding="utf-8"))


def write_state(path: Path, last_success_utc: datetime, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_success_utc": last_success_utc.astimezone(UTC).isoformat(),
        "last_run_id": run_id,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
