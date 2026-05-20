import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from datalake.configs.settings import Settings


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    if shutil.which("java") is None:
        pytest.skip("Java is required for PySpark tests. CI and Docker install Java 17.")

    session = (
        SparkSession.builder.appName("datalake-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def payload() -> dict:
    return json.loads(Path("tests/fixtures/open_meteo_payload.json").read_text(encoding="utf-8"))


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        logs_dir=tmp_path / "logs",
        state_file=tmp_path / "data/state/pipeline_state.json",
        metrics_dir=tmp_path / "data/metrics",
        failed_records_dir=tmp_path / "data/failed",
        duckdb_path=tmp_path / "data/warehouse.duckdb",
        spark_master="local[2]",
        spark_shuffle_partitions=2,
    )
