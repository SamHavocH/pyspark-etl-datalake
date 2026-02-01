import logging
from pathlib import Path
from dotenv import load_dotenv
import os

def run() -> int:
    load_dotenv()

    project = os.getenv("PROJECT_NAME", "pyspark-etl-datalake")
    log_level = os.getenv("LOG_LEVEL", "INFO")

    Path("logs").mkdir(exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/etl.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logging.getLogger(project).info("pipeline_boot ok project=%s", project)
    return 0
