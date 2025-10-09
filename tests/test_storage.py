import datetime as dt
import sqlite3
from pathlib import Path

from trend_monitor.analysis import Trend
from trend_monitor.sources import SourceItem
from trend_monitor.storage import SQLiteStorageConfig, SQLiteTrendStorage


def _trend(keyword: str, published: dt.datetime) -> Trend:
    item = SourceItem(
        id=f"{keyword}-{published.timestamp()}",
        title=f"{keyword.title()} insight",
        url="https://example.com/insight",
        published=published,
        summary="",
        language="en",
    )
    return Trend(keyword=keyword, score=1.0, items=[item])


def test_storage_persists_and_prunes(tmp_path: Path):
    db_path = tmp_path / "trends.sqlite"
    config = SQLiteStorageConfig(path=db_path, retention=dt.timedelta(hours=1), vacuum_every=2)
    storage = SQLiteTrendStorage(config)

    base = dt.datetime(2024, 1, 1, 0, 0, 0)
    storage.save([_trend("k0", base)], base)
    storage.save([_trend("k1", base + dt.timedelta(minutes=30))], base + dt.timedelta(minutes=30))
    storage.save([_trend("k2", base + dt.timedelta(hours=2))], base + dt.timedelta(hours=2))

    conn = sqlite3.connect(str(db_path))
    snapshots = conn.execute("SELECT generated_at FROM snapshots ORDER BY generated_at").fetchall()
    assert len(snapshots) == 1
    assert snapshots[0][0].startswith("2024-01-01T02:00:00")

    trend_rows = conn.execute(
        "SELECT t.keyword, COUNT(i.id) FROM trends t JOIN trend_items i ON t.id = i.trend_id GROUP BY t.keyword"
    ).fetchall()
    assert {row[0] for row in trend_rows} == {"k2"}
    conn.close()

    storage.close()
