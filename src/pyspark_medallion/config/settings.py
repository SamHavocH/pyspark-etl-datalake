from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "pyspark-medallion-platform"
    environment: str = "local"
    log_level: str = "INFO"

    data_root: Path = Path("./data")
    raw_dir: Path = Path("./data/raw")
    bronze_dir: Path = Path("./data/bronze")
    silver_dir: Path = Path("./data/silver")
    gold_dir: Path = Path("./data/gold")
    rejected_dir: Path = Path("./data/rejected_records")
    quality_report_dir: Path = Path("./data/quality_reports")
    bookmark_path: Path = Path("./data/bookmarks.json")

    spark_master: str = "local[*]"
    spark_warehouse_dir: Path = Path("./data/spark-warehouse")
    target_file_size_mb: int = Field(default=128, ge=16)

    def ensure_directories(self) -> None:
        for path in (
            self.data_root,
            self.raw_dir,
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.rejected_dir,
            self.quality_report_dir,
            self.spark_warehouse_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


@lru_cache
def load_settings() -> Settings:
    settings = Settings()
    settings.ensure_directories()
    return settings
