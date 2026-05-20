from pyspark.sql import SparkSession

from pyspark_medallion.quality.rules import apply_quality_rules


def test_order_quality_rules_quarantine_invalid_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", "2026-01-01 10:00:00", 2, 10.0, 0.0, "paid", "2026-01-01 10:01:00"),
            ("O2", "", "P1", "2026-01-01 11:00:00", 1, 10.0, 0.0, "paid", "2026-01-01 11:01:00"),
            ("O3", "C1", "P1", "2026-01-01 12:00:00", -1, 10.0, 0.0, "paid", "2026-01-01 12:01:00"),
        ],
        "order_id string, customer_id string, product_id string, order_ts string, quantity int, "
        "unit_price double, discount_amount double, order_status string, updated_at string",
    )

    accepted, rejected = apply_quality_rules(df, "orders")

    assert accepted.count() == 1
    assert rejected.select("rejection_reason").distinct().count() == 2
