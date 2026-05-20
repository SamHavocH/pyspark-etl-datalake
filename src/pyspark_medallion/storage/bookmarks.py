import json
from datetime import UTC, datetime
from pathlib import Path


def read_bookmark(path: Path, entity: str) -> datetime | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    value = payload.get(entity)
    if value is None:
        return None
    return datetime.fromisoformat(value)


def write_bookmark(path: Path, entity: str, watermark: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    payload[entity] = watermark.astimezone(UTC).isoformat()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
