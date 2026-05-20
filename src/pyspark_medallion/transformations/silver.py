from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pyspark_medallion.ingestion.bronze import ENTITIES, read_bronze_entity
from pyspark_medallion.quality.rules import apply_quality_rules, write_quality_outputs
from pyspark_medallion.schemas.ecommerce import ENTITY_SCHEMAS, ENTITY_WATERMARK_COLUMNS
from pyspark_medallion.storage.bookmarks import read_bookmark, write_bookmark
from pyspark_medallion.storage.parquet import read_parquet_if_exists, write_parquet

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "transactions": ["transaction_id"],
    "events": ["event_id"],
}


def parse_bronze_payload(bronze_df: DataFrame, entity: str) -> DataFrame:
    schema = ENTITY_SCHEMAS[entity]
    parsed = (
        bronze_df.select(
            F.from_json("payload", schema).alias("record"),
            "source_system",
            "source_file",
            "ingestion_ts",
            "batch_id",
        )
        .select("record.*", "source_system", "source_file", "ingestion_ts", "batch_id")
        .transform(_normalize_strings)
        .withColumn("processed_ts", F.current_timestamp())
    )
    if entity == "orders":
        return parsed.withColumn("order_date", F.to_date("order_ts"))
    if entity == "transactions":
        return parsed.withColumn("transaction_date", F.to_date("transaction_ts"))
    if entity == "events":
        return parsed.withColumn("event_date", F.to_date("event_ts"))
    return parsed


def build_silver_entity(
    spark: SparkSession,
    *,
    bronze_dir: Path,
    silver_dir: Path,
    rejected_dir: Path,
    report_dir: Path,
    bookmark_path: Path,
    entity: str,
) -> dict[str, int]:
    bronze_df = read_bronze_entity(spark, bronze_dir, entity)
    parsed = parse_bronze_payload(bronze_df, entity)
    watermark_column = ENTITY_WATERMARK_COLUMNS[entity]
    last_watermark = read_bookmark(bookmark_path, f"silver.{entity}")
    if last_watermark is not None:
        parsed = parsed.where(F.col(watermark_column) >= F.lit(last_watermark))

    deduped = _deduplicate_latest(parsed, PRIMARY_KEYS[entity], watermark_column)
    accepted, rejected = apply_quality_rules(deduped, entity)
    quality_metrics = write_quality_outputs(
        accepted,
        rejected,
        entity=entity,
        rejected_dir=rejected_dir,
        report_dir=report_dir,
    )
    target = _merge_with_existing(spark, accepted, silver_dir / entity, PRIMARY_KEYS[entity], watermark_column)
    write_parquet(target, silver_dir / entity, mode="overwrite", partition_by=_partition_columns(entity))

    max_watermark_row = accepted.select(F.max(watermark_column)).first()
    max_watermark = max_watermark_row[0] if max_watermark_row is not None else None
    if max_watermark is not None:
        write_bookmark(bookmark_path, f"silver.{entity}", max_watermark)
    return quality_metrics


def build_all_silver(
    spark: SparkSession,
    *,
    bronze_dir: Path,
    silver_dir: Path,
    rejected_dir: Path,
    report_dir: Path,
    bookmark_path: Path,
) -> dict[str, dict[str, int]]:
    return {
        entity: build_silver_entity(
            spark,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            rejected_dir=rejected_dir,
            report_dir=report_dir,
            bookmark_path=bookmark_path,
            entity=entity,
        )
        for entity in ENTITIES
    }


def _normalize_strings(df: DataFrame) -> DataFrame:
    normalized = df
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            normalized = normalized.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", None).otherwise(F.trim(F.col(field.name))),
            )
    return normalized


def _deduplicate_latest(df: DataFrame, key_columns: list[str], watermark_column: str) -> DataFrame:
    window = Window.partitionBy(*key_columns).orderBy(F.col(watermark_column).desc(), F.col("ingestion_ts").desc())
    return df.withColumn("_row_number", F.row_number().over(window)).where("_row_number = 1").drop("_row_number")


def _merge_with_existing(
    spark: SparkSession,
    incoming: DataFrame,
    target_path: Path,
    key_columns: list[str],
    watermark_column: str,
) -> DataFrame:
    existing = read_parquet_if_exists(target_path, spark)
    if existing is None:
        return incoming
    aligned_existing = existing.select(incoming.columns)
    return _deduplicate_latest(aligned_existing.unionByName(incoming), key_columns, watermark_column)


def _partition_columns(entity: str) -> list[str]:
    if entity == "orders":
        return ["order_date"]
    if entity == "transactions":
        return ["transaction_date"]
    if entity == "events":
        return ["event_date"]
    return []
