from collections.abc import Iterator
from os import environ
from shutil import which

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    if which("java") is None and "JAVA_HOME" not in environ:
        pytest.skip("Spark tests require Java or JAVA_HOME")

    session = (
        SparkSession.builder.appName("pyspark-medallion-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
