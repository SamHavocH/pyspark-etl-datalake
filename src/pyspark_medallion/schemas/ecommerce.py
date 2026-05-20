from pyspark.sql import types as T

BRONZE_RAW_SCHEMA = T.StructType(
    [
        T.StructField("payload", T.StringType(), nullable=False),
        T.StructField("source_system", T.StringType(), nullable=False),
        T.StructField("entity", T.StringType(), nullable=False),
        T.StructField("source_file", T.StringType(), nullable=False),
        T.StructField("ingestion_ts", T.TimestampType(), nullable=False),
        T.StructField("ingestion_date", T.DateType(), nullable=False),
        T.StructField("batch_id", T.StringType(), nullable=False),
    ]
)

CUSTOMERS_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("email", T.StringType(), nullable=False),
        T.StructField("full_name", T.StringType(), nullable=True),
        T.StructField("city", T.StringType(), nullable=True),
        T.StructField("state", T.StringType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)

PRODUCTS_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=False),
        T.StructField("sku", T.StringType(), nullable=False),
        T.StructField("product_name", T.StringType(), nullable=False),
        T.StructField("category", T.StringType(), nullable=False),
        T.StructField("unit_price", T.DecimalType(12, 2), nullable=False),
        T.StructField("is_active", T.BooleanType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)

ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("product_id", T.StringType(), nullable=False),
        T.StructField("order_ts", T.TimestampType(), nullable=False),
        T.StructField("quantity", T.IntegerType(), nullable=False),
        T.StructField("unit_price", T.DecimalType(12, 2), nullable=False),
        T.StructField("discount_amount", T.DecimalType(12, 2), nullable=False),
        T.StructField("order_status", T.StringType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)

TRANSACTIONS_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), nullable=False),
        T.StructField("order_id", T.StringType(), nullable=False),
        T.StructField("transaction_ts", T.TimestampType(), nullable=False),
        T.StructField("payment_method", T.StringType(), nullable=False),
        T.StructField("payment_status", T.StringType(), nullable=False),
        T.StructField("amount", T.DecimalType(12, 2), nullable=False),
        T.StructField("currency", T.StringType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)

EVENTS_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("session_id", T.StringType(), nullable=False),
        T.StructField("event_ts", T.TimestampType(), nullable=False),
        T.StructField("event_type", T.StringType(), nullable=False),
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("device_type", T.StringType(), nullable=True),
        T.StructField("updated_at", T.TimestampType(), nullable=False),
    ]
)

ENTITY_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
    "events": EVENTS_SCHEMA,
}

ENTITY_WATERMARK_COLUMNS = {
    "customers": "updated_at",
    "products": "updated_at",
    "orders": "updated_at",
    "transactions": "updated_at",
    "events": "updated_at",
}
