from functools import cached_property
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables and .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    project_name: str = "pyspark-etl-datalake"
    environment: str = "local"
    log_level: str = "INFO"

    data_dir: Path = Path("data")
    logs_dir: Path = Path("logs")
    state_file: Path = Path("data/state/pipeline_state.json")
    metrics_dir: Path = Path("data/metrics")
    failed_records_dir: Path = Path("data/failed")
    duckdb_path: Path = Path("data/warehouse.duckdb")

    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"
    location_id: str = "sao_paulo_br"
    latitude: float = -23.5505
    longitude: float = -46.6333
    timezone: str = "UTC"
    days_back: int = 7
    overlap_days: int = 1
    request_timeout_seconds: int = 30

    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 4
    output_format: str = Field(default="parquet", pattern="^(parquet)$")

    @cached_property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw" / "open_meteo"

    @cached_property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze" / "weather_hourly"

    @cached_property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver" / "weather_hourly"

    @cached_property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"


def get_settings() -> Settings:
    return Settings()
