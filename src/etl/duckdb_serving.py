from pathlib import Path
import duckdb

def build_serving_layer(duckdb_path: Path, processed_dir: Path) -> None:
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    parquet_glob = str(processed_dir / "**/*.parquet")

    con = duckdb.connect(str(duckdb_path))

    # Cria VIEW sobre os arquivos Parquet (leitura dinâmica)
    con.execute(f"""
        CREATE OR REPLACE VIEW v_weather_hourly AS
        SELECT *
        FROM read_parquet('{parquet_glob}')
    """)

    # TABLE materializada: boa para analytics e estabilidade
    con.execute("""
        CREATE TABLE IF NOT EXISTS weather_hourly AS
        SELECT * FROM v_weather_hourly WHERE 1=0
    """)

    # Change data capture: insere apenas novos registros
    con.execute("""
        INSERT INTO weather_hourly
        SELECT v.*
        FROM v_weather_hourly v
        LEFT JOIN weather_hourly t
          ON t.ts_utc = v.ts_utc
        WHERE t.ts_utc IS NULL
    """)

    con.close()
