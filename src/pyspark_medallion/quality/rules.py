from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_medallion.storage.parquet import write_parquet


@dataclass(frozen=True)
class QualityRule:
    name: str
    condition_sql: str


ENTITY_RULES: dict[str, list[QualityRule]] = {
    "customers": [
        QualityRule("missing_customer_id", "customer_id IS NULL OR customer_id = ''"),
        QualityRule("missing_email", "email IS NULL OR email = ''"),
        QualityRule("invalid_customer_timestamp", "updated_at IS NULL OR created_at IS NULL"),
    ],
    "products": [
        QualityRule("missing_product_id", "product_id IS NULL OR product_id = ''"),
        QualityRule("invalid_unit_price", "unit_price IS NULL OR unit_price <= 0"),
    ],
    "orders": [
        QualityRule("missing_order_id", "order_id IS NULL OR order_id = ''"),
        QualityRule("missing_customer_id", "customer_id IS NULL OR customer_id = ''"),
        QualityRule("invalid_quantity", "quantity IS NULL OR quantity <= 0"),
        QualityRule("invalid_order_ts", "order_ts IS NULL OR updated_at IS NULL"),
        QualityRule("invalid_order_amount", "unit_price IS NULL OR unit_price <= 0 OR discount_amount < 0"),
    ],
    "transactions": [
        QualityRule("missing_transaction_id", "transaction_id IS NULL OR transaction_id = ''"),
        QualityRule("missing_order_id", "order_id IS NULL OR order_id = ''"),
        QualityRule("invalid_transaction_ts", "transaction_ts IS NULL OR updated_at IS NULL"),
        QualityRule("invalid_transaction_amount", "amount IS NULL OR amount <= 0"),
        QualityRule("invalid_currency", "currency IS NULL OR currency <> 'BRL'"),
    ],
    "events": [
        QualityRule("missing_event_id", "event_id IS NULL OR event_id = ''"),
        QualityRule("missing_session_id", "session_id IS NULL OR session_id = ''"),
        QualityRule("invalid_event_ts", "event_ts IS NULL OR updated_at IS NULL"),
    ],
}


def apply_quality_rules(df: DataFrame, entity: str) -> tuple[DataFrame, DataFrame]:
    rules = ENTITY_RULES[entity]
    rejected = None
    invalid_predicate = None
    for rule in rules:
        failed = df.where(F.expr(rule.condition_sql)).withColumn("rejection_reason", F.lit(rule.name))
        rejected = failed if rejected is None else rejected.unionByName(failed, allowMissingColumns=True)
        predicate = F.expr(rule.condition_sql)
        invalid_predicate = predicate if invalid_predicate is None else invalid_predicate | predicate

    if rejected is None or invalid_predicate is None:
        return df, df.limit(0).withColumn("rejection_reason", F.lit(""))

    accepted = df.where(~invalid_predicate)
    return accepted, rejected.dropDuplicates()


def write_quality_outputs(
    accepted: DataFrame,
    rejected: DataFrame,
    *,
    entity: str,
    rejected_dir: Path,
    report_dir: Path,
) -> dict[str, int]:
    accepted_count = accepted.count()
    rejected_count = rejected.count()
    total_count = accepted_count + rejected_count

    if rejected_count:
        write_parquet(
            rejected.withColumn("rejected_date", F.current_date()),
            rejected_dir / entity,
            mode="append",
            partition_by=["rejected_date", "rejection_reason"],
        )

    report = accepted.sparkSession.createDataFrame(
        [(entity, total_count, accepted_count, rejected_count)],
        "entity string, total_rows long, accepted_rows long, rejected_rows long",
    ).withColumn("report_date", F.current_date())
    write_parquet(report, report_dir / entity, mode="append", partition_by=["report_date"])
    return {
        "total_rows": total_count,
        "accepted_rows": accepted_count,
        "rejected_rows": rejected_count,
    }


def duplicate_count(df: DataFrame, key_columns: list[str]) -> int:
    row = df.groupBy(*key_columns).count().where(F.col("count") > 1).select(F.sum("count") - F.count("*")).first()
    return int(row[0] or 0) if row is not None else 0
