from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark_medallion.transformations.silver import parse_bronze_payload


def test_parse_bronze_payload_normalizes_strings_and_adds_order_date(spark: SparkSession) -> None:
    bronze = spark.createDataFrame(
        [
            (
                '{"order_id":"O1","customer_id":" C1 ","product_id":"P1","order_ts":"2026-01-01T10:00:00",'
                '"quantity":2,"unit_price":10.50,"discount_amount":1.00,"order_status":" paid ",'
                '"updated_at":"2026-01-01T10:05:00"}',
                "synthetic",
                "file.csv",
                "2026-01-01 10:10:00",
                "batch-1",
            )
        ],
        "payload string, source_system string, source_file string, ingestion_ts string, batch_id string",
    ).withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))

    parsed = parse_bronze_payload(bronze, "orders")
    row = parsed.first()

    assert row is not None
    assert row.customer_id == "C1"
    assert row.order_status == "paid"
    assert str(row.order_date) == "2026-01-01"
