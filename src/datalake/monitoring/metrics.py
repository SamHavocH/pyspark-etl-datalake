import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any


@dataclass
class PipelineMetrics:
    run_id: str
    started_at_utc: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    finished_at_utc: str | None = None
    duration_seconds: float | None = None
    row_counts: dict[str, int] = field(default_factory=dict)
    quality_results: dict[str, int] = field(default_factory=dict)
    status: str = "running"
    error: str | None = None


class MetricsRecorder:
    def __init__(self, run_id: str) -> None:
        self.metrics = PipelineMetrics(run_id=run_id)
        self._start = perf_counter()

    def set_rows(self, layer: str, count: int) -> None:
        self.metrics.row_counts[layer] = count

    def set_quality(self, results: dict[str, int]) -> None:
        self.metrics.quality_results = results

    def finish(self, status: str, error: Exception | None = None) -> PipelineMetrics:
        self.metrics.finished_at_utc = datetime.now(UTC).isoformat()
        self.metrics.duration_seconds = round(perf_counter() - self._start, 3)
        self.metrics.status = status
        self.metrics.error = str(error) if error else None
        return self.metrics

    def write(self, metrics_dir: Path) -> Path:
        metrics_dir.mkdir(parents=True, exist_ok=True)
        output_path = metrics_dir / f"{self.metrics.run_id}.json"
        output_path.write_text(json.dumps(asdict(self.metrics), indent=2, sort_keys=True), encoding="utf-8")
        return output_path


def metrics_as_log_props(metrics: PipelineMetrics) -> dict[str, Any]:
    return {
        "run_id": metrics.run_id,
        "status": metrics.status,
        "duration_seconds": metrics.duration_seconds,
        **{f"{layer}_rows": count for layer, count in metrics.row_counts.items()},
    }
