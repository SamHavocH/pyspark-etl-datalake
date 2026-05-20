from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark_medallion.storage.parquet import write_parquet


def build_gold_tables(spark: SparkSession, *, silver_dir: Path, gold_dir: Path) -> dict[str, int]:
    customers = spark.read.parquet(str(silver_dir / "customers"))
    products = spark.read.parquet(str(silver_dir / "products"))
    orders = spark.read.parquet(str(silver_dir / "orders"))
    transactions = spark.read.parquet(str(silver_dir / "transactions"))
    events = spark.read.parquet(str(silver_dir / "events"))

    fact_orders = (
        orders.alias("o")
        .join(customers.select("customer_id", "city", "state"), "customer_id", "left")
        .join(products.select("product_id", "category", "product_name"), "product_id", "left")
        .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("net_amount", F.col("gross_amount") - F.col("discount_amount"))
        .withColumn("order_date", F.to_date("order_ts"))
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "order_ts",
            "order_date",
            "quantity",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "order_status",
            "city",
            "state",
            "category",
            "product_name",
        )
    )

    daily_sales = (
        fact_orders.groupBy("order_date", "state", "category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
        )
        .orderBy("order_date", "state", "category")
    )

    customer_lifetime_value = (
        fact_orders.where(F.col("order_status") != "cancelled")
        .groupBy("customer_id", "city", "state")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("net_amount"), 2).alias("lifetime_value"),
            F.max("order_ts").alias("last_order_ts"),
        )
    )

    payment_metrics = (
        transactions.join(orders.select("order_id", F.to_date("order_ts").alias("order_date")), "order_id", "left")
        .groupBy("order_date", "payment_method", "payment_status")
        .agg(
            F.countDistinct("transaction_id").alias("transactions"),
            F.round(F.sum("amount"), 2).alias("amount"),
        )
    )

    funnel_metrics = events.groupBy(F.to_date("event_ts").alias("event_date"), "event_type", "device_type").agg(
        F.count("*").alias("events"), F.countDistinct("session_id").alias("sessions")
    )

    tables = {
        "fact_orders": fact_orders,
        "daily_sales": daily_sales,
        "customer_lifetime_value": customer_lifetime_value,
        "payment_metrics": payment_metrics,
        "funnel_metrics": funnel_metrics,
    }
    counts = {}
    for name, df in tables.items():
        partitions = ["order_date"] if name in {"fact_orders", "daily_sales", "payment_metrics"} else []
        if name == "funnel_metrics":
            partitions = ["event_date"]
        write_parquet(df, gold_dir / name, mode="overwrite", partition_by=partitions)
        counts[name] = df.count()
    return counts
