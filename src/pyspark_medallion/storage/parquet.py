from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def write_parquet(
    df: DataFrame,
    path: Path,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = df.write.mode(mode).format("parquet").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def read_parquet_if_exists(path: Path, spark: SparkSession) -> DataFrame | None:
    if not path.exists():
        return None
    return spark.read.parquet(str(path))
