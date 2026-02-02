import json
from pathlib import Path
from datetime import datetime, timezone

def read_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"last_success_utc": None}
    return json.loads(state_path.read_text(encoding="utf-8"))

def write_state(state_path: Path, last_success_utc: datetime) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_success_utc": last_success_utc.astimezone(timezone.utc).isoformat()}
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
