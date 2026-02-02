import logging
import os
from pathlib import Path
from dotenv import load_dotenv

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

    lat = float(os.getenv("LAT", "-23.5505"))
    lon = float(os.getenv("LON", "-46.6333"))
    days_back = int(os.getenv("DAYS_BACK", "7"))

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

    spark = None
    try:
        logger.info("run_start project=%s lat=%s lon=%s days_back=%s", project, lat, lon, days_back)

        spark = make_spark(project)
        spark.sparkContext.setLogLevel("WARN")

        payload = extract_openmeteo(lat, lon, days_back)
        df = transform_openmeteo_spark(spark, payload)

        logger.info("spark_df rows=%d", df.count())
        logger.info("schema=%s", df.schema.simpleString())

        write_parquet_partitioned_dynamic(df, processed_dir, spark)

        logger.info("parquet_written path=%s", str(processed_dir))
        return 0

    except Exception:
        logger.exception("run_failed")
        return 1

    finally:
        if spark is not None:
            spark.stop()
