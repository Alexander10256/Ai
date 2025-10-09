import datetime as dt
from pathlib import Path

from trend_monitor.analysis import Trend
from trend_monitor.sources import SourceItem
from trend_monitor.storage import StorageConfig, TrendStorage


def _trend(keyword: str, published: dt.datetime) -> Trend:
    item = SourceItem(
        id=f"{keyword}-{published.timestamp()}",
        title=f"{keyword.title()} insight",
        url="https://example.com/insight",
        published=published,
        summary="",
    )
    return Trend(keyword=keyword, score=1.0, items=[item])


def test_storage_writes_atomically_and_rotates(tmp_path: Path):
    config = StorageConfig(root=tmp_path, history_limit=2)
    storage = TrendStorage(config)

    base = dt.datetime(2024, 1, 1, 0, 0, 0)
    for offset in range(3):
        storage.save([_trend(f"k{offset}", base + dt.timedelta(seconds=offset))], base + dt.timedelta(seconds=offset))

    latest_path = tmp_path / config.latest_filename
    assert latest_path.exists()
    latest_content = latest_path.read_text(encoding="utf-8")
    assert "k2" in latest_content

    history_files = sorted(config.history_path.glob("trends_*.json"))
    assert len(history_files) == 2
    assert not (config.history_path / "trends_20240101T000000.json").exists()
