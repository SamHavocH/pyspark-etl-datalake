from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any


@dataclass
class JobMetrics:
    job_name: str
    values: dict[str, Any] = field(default_factory=dict)

    def add(self, key: str, value: Any) -> None:
        self.values[key] = value


@contextmanager
def timed_stage(metrics: JobMetrics, stage_name: str) -> Iterator[None]:
    started = perf_counter()
    try:
        yield
    finally:
        metrics.add(f"{stage_name}_duration_seconds", round(perf_counter() - started, 3))
