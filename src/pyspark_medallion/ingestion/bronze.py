from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pyspark_medallion.storage.parquet import write_parquet

ENTITIES = ("customers", "products", "orders", "transactions", "events")


def ingest_raw_csv_to_bronze(
    spark: SparkSession,
    raw_dir: Path,
    bronze_dir: Path,
    *,
    source_system: str = "synthetic_ecommerce",
) -> dict[str, int]:
    counts: dict[str, int] = {}
    batch_id = str(uuid4())
    for entity in ENTITIES:
        source_files = [str(path) for path in raw_dir.rglob(f"{entity}.csv")]
        if not source_files:
            counts[entity] = 0
            continue

        raw_df = spark.read.option("header", "true").csv(source_files)

        payload_cols = F.struct(*[F.col(column) for column in raw_df.columns])
        bronze_df = (
            raw_df.select(F.to_json(payload_cols).alias("payload"))
            .withColumn("source_system", F.lit(source_system))
            .withColumn("entity", F.lit(entity))
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("ingestion_date", F.to_date("ingestion_ts"))
            .withColumn("batch_id", F.lit(batch_id))
        )
        row_count = bronze_df.count()
        write_parquet(
            bronze_df,
            bronze_dir / entity,
            mode="append",
            partition_by=["ingestion_date"],
        )
        counts[entity] = row_count
    return counts


def read_bronze_entity(spark: SparkSession, bronze_dir: Path, entity: str) -> DataFrame:
    return spark.read.parquet(str(bronze_dir / entity)).where(F.col("entity") == entity)
