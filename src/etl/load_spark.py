from pathlib import Path

def write_parquet_partitioned(df, processed_dir: Path):
    processed_dir.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .mode("overwrite")
        .partitionBy("date_utc")
        .parquet(str(processed_dir))
    )

def write_parquet_partitioned_dynamic(df, processed_dir: Path, spark):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        df.write
        .mode("overwrite")
        .partitionBy("date_utc")
        .parquet(str(processed_dir))
    )
