from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def write_partitioned(df: DataFrame, path: Path, partition_cols: list[str]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (df.write.mode("overwrite").option("compression", "snappy").partitionBy(*partition_cols).parquet(str(path)))


def read_parquet_if_exists(spark: SparkSession, path: Path) -> DataFrame | None:
    if not path.exists() or not any(path.rglob("*.parquet")):
        return None
    return spark.read.parquet(str(path))
