from datetime import UTC, datetime
from pathlib import Path

from pyspark_medallion.storage.bookmarks import read_bookmark, write_bookmark


def test_bookmark_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "bookmarks.json"
    watermark = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)

    write_bookmark(path, "silver.orders", watermark)

    assert read_bookmark(path, "silver.orders") == watermark
    assert read_bookmark(path, "silver.events") is None
