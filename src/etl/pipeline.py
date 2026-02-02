from asyncio.log import logger
from datetime import datetime, timedelta, timezone
from .state import read_state, write_state
from .quality import run_quality_checks
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from .duckdb_serving import build_serving_layer
from .spark import make_spark
from .extract import extract_openmeteo
from .transform_spark import transform_openmeteo_spark
from .load_spark import write_parquet_partitioned_dynamic

def run() -> int:
    load_dotenv()

    project = os.getenv("PROJECT_NAME", "pyspark-etl-datalake")
    log_level = os.getenv("LOG_LEVEL", "INFO")

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    processed_dir = Path(os.getenv("PROCESSED_DIR", str(data_dir / "processed")))
    state_path = data_dir / "state.json"

    overlap_days = int(os.getenv("OVERLAP_DAYS", "1"))

    lat = float(os.getenv("LAT", "-23.5505"))
    lon = float(os.getenv("LON", "-46.6333"))
    days_back = int(os.getenv("DAYS_BACK", "7"))

    state = read_state(state_path)
    last_success = state.get("last_success_utc")

    end_dt = datetime.now(timezone.utc)

    if last_success:
        last_dt = datetime.fromisoformat(last_success)
        start_dt = last_dt - timedelta(days=overlap_days)
    else:
        # first run fallback
        start_dt = end_dt - timedelta(days=days_back)

    start_date = start_dt.date().isoformat()
    end_date = end_dt.date().isoformat()


    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger(project)

    logger.info("incremental_window start_date=%s end_date=%s last_success=%s",
                start_date, end_date, last_success)


    spark = None
    try:
        logger.info("run_start project=%s lat=%s lon=%s days_back=%s", project, lat, lon, days_back)

        spark = make_spark(project)
        spark.sparkContext.setLogLevel("WARN")

        payload = extract_openmeteo(lat, lon, start_date, end_date)
        df = transform_openmeteo_spark(spark, payload)

        logger.info("spark_df rows=%d", df.count())
        logger.info("schema=%s", df.schema.simpleString())

        run_quality_checks(df)
        logger.info("dq_ok")

        write_parquet_partitioned_dynamic(df, processed_dir, spark)
        logger.info("parquet_written path=%s", str(processed_dir))

        write_state(state_path, datetime.now(timezone.utc))
        logger.info("state_updated path=%s", str(state_path))

        duckdb_path = Path(os.getenv("DUCKDB_PATH", str(data_dir / "warehouse.duckdb")))
        try:
            build_serving_layer(duckdb_path, processed_dir)
            logger.info("duckdb_serving ok path=%s", str(duckdb_path))
        except Exception:
            logger.exception("duckdb_serving_failed (non-fatal)")
        return 0

    except Exception:
        logger.exception("run_failed")
        return 1

    finally:
        if spark is not None:
            spark.stop()
