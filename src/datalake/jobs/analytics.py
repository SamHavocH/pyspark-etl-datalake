from pathlib import Path

import duckdb


def build_duckdb_serving_layer(duckdb_path: Path, silver_dir: Path, gold_dir: Path) -> None:
    """Create query-friendly DuckDB views over curated lake tables."""

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    silver_glob = str(silver_dir / "**" / "*.parquet").replace("\\", "/")
    gold_glob = str(gold_dir / "weather_daily_summary" / "**" / "*.parquet").replace("\\", "/")

    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW silver_weather_hourly AS
            SELECT * FROM read_parquet('{silver_glob}', hive_partitioning = true)
            """
        )
        connection.execute(
            f"""
            CREATE OR REPLACE VIEW gold_weather_daily_summary AS
            SELECT * FROM read_parquet('{gold_glob}', hive_partitioning = true)
            """
        )
