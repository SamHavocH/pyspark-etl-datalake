import logging
import sys
from pathlib import Path
from typing import Any


class KeyValueFormatter(logging.Formatter):
    """Small structured formatter that keeps logs readable in local runs."""

    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "props", None)
        if isinstance(extra, dict):
            base.update(extra)
        return " ".join(f"{key}={value}" for key, value in base.items())


def configure_logging(log_level: str, logs_dir: Path, logger_name: str) -> logging.Logger:
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    formatter = KeyValueFormatter()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(logs_dir / "pipeline.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


def props(**kwargs: Any) -> dict[str, Any]:
    return {"props": {key: value for key, value in kwargs.items() if value is not None}}
