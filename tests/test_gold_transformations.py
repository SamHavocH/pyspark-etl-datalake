from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from pyspark_medallion.storage.parquet import write_parquet
from pyspark_medallion.transformations.gold import build_gold_tables

CUSTOMER_SCHEMA = (
    "customer_id string, email string, full_name string, city string, state string, "
    "created_at timestamp, updated_at timestamp"
)
PRODUCT_SCHEMA = (
    "product_id string, sku string, product_name string, category string, "
    "unit_price decimal(12,2), is_active boolean, updated_at timestamp"
)
ORDER_SCHEMA = (
    "order_id string, customer_id string, product_id string, order_ts timestamp, quantity int, "
    "unit_price decimal(12,2), discount_amount decimal(12,2), order_status string, "
    "updated_at timestamp, order_date date"
)
TRANSACTION_SCHEMA = (
    "transaction_id string, order_id string, transaction_ts timestamp, payment_method string, "
    "payment_status string, amount decimal(12,2), currency string, updated_at timestamp"
)
EVENT_SCHEMA = (
    "event_id string, customer_id string, session_id string, event_ts timestamp, "
    "event_type string, product_id string, device_type string, updated_at timestamp"
)


def test_gold_tables_build_business_metrics(spark: SparkSession, tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    customers = spark.createDataFrame(
        [("C1", "a@example.com", "Ada", "Sao Paulo", "SP", datetime(2026, 1, 1), datetime(2026, 1, 1))],
        CUSTOMER_SCHEMA,
    )
    products = spark.createDataFrame(
        [("P1", "SKU-1", "Keyboard", "electronics", Decimal("100.00"), True, datetime(2026, 1, 1))],
        PRODUCT_SCHEMA,
    )
    orders = spark.createDataFrame(
        [
            (
                "O1",
                "C1",
                "P1",
                datetime(2026, 1, 2, 10),
                2,
                Decimal("100.00"),
                Decimal("10.00"),
                "paid",
                datetime(2026, 1, 2, 10, 1),
                datetime(2026, 1, 2).date(),
            )
        ],
        ORDER_SCHEMA,
    )
    transactions = spark.createDataFrame(
        [
            (
                "T1",
                "O1",
                datetime(2026, 1, 2, 10, 2),
                "pix",
                "captured",
                Decimal("190.00"),
                "BRL",
                datetime(2026, 1, 2, 10, 3),
            )
        ],
        TRANSACTION_SCHEMA,
    )
    events = spark.createDataFrame(
        [("E1", "C1", "S1", datetime(2026, 1, 2, 9), "product_view", "P1", "mobile", datetime(2026, 1, 2, 9, 1))],
        EVENT_SCHEMA,
    )

    write_parquet(customers, silver_dir / "customers")
    write_parquet(products, silver_dir / "products")
    write_parquet(orders, silver_dir / "orders")
    write_parquet(transactions, silver_dir / "transactions")
    write_parquet(events, silver_dir / "events")

    counts = build_gold_tables(spark, silver_dir=silver_dir, gold_dir=gold_dir)

    assert counts["fact_orders"] == 1
    daily_sales = spark.read.parquet(str(gold_dir / "daily_sales"))
    row = daily_sales.first()
    assert row is not None
    assert row.net_revenue == Decimal("190.00")
