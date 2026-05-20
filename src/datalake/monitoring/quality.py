from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class QualityResult:
    name: str
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def validate_silver(df: DataFrame) -> list[QualityResult]:
    """Run lightweight data quality checks without adding a heavy framework."""

    checks = {
        "silver_not_empty": 0 if df.limit(1).count() > 0 else 1,
        "observed_at_not_null": df.filter(F.col("observed_at_utc").isNull()).count(),
        "humidity_range": df.filter(
            F.col("relative_humidity_pct").isNotNull() & ~F.col("relative_humidity_pct").between(0, 100)
        ).count(),
        "precipitation_non_negative": df.filter(F.col("precipitation_mm") < 0).count(),
        "temperature_plausible": df.filter(
            F.col("temperature_c").isNotNull() & ~F.col("temperature_c").between(-80, 60)
        ).count(),
    }
    return [QualityResult(name, failed_rows) for name, failed_rows in checks.items()]


def raise_on_quality_failure(results: list[QualityResult]) -> None:
    failures = [result for result in results if not result.passed]
    if failures:
        summary = ", ".join(f"{failure.name}={failure.failed_rows}" for failure in failures)
        raise ValueError(f"Data quality failed: {summary}")
