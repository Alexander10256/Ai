import datetime as dt

from trend_monitor.monitor import TrendMonitor
from trend_monitor.sources import BaseSource, SourceConfig, SourceError, SourceItem


class FlakySource(BaseSource):
    def __init__(self):
        super().__init__(SourceConfig(name="flaky", url="https://example.com/rss", max_retries=2, retry_backoff=0.0))
        self.attempts = 0

    def fetch(self):
        self.attempts += 1
        if self.attempts < 2:
            raise SourceError("temporary failure")
        now = dt.datetime.utcnow()
        return [
            SourceItem(
                id=f"flaky-{now.timestamp()}",
                title="Run faster",
                url="https://example.com/run",
                published=now,
                summary="",
            )
        ]


class DuplicateSource(BaseSource):
    def __init__(self):
        super().__init__(SourceConfig(name="dup", url="https://example.com/dup"))
        self.item = SourceItem(
            id="duplicate",
            title="Unique launch",
            url="https://example.com/launch",
            published=dt.datetime.utcnow(),
            summary="",
        )

    def fetch(self):
        return [self.item]


class AgingSource(BaseSource):
    def __init__(self):
        super().__init__(SourceConfig(name="aging", url="https://example.com/aging"))
        now = dt.datetime.utcnow()
        self._items = [
            SourceItem(
                id="old",
                title="Historic record",
                url="https://example.com/old",
                published=now - dt.timedelta(hours=2),
                summary="",
            ),
            SourceItem(
                id="fresh",
                title="Fresh record",
                url="https://example.com/fresh",
                published=now,
                summary="",
            ),
        ]
        self._returned = False

    def fetch(self):
        if self._returned:
            return []
        self._returned = True
        return list(self._items)


def test_monitor_retries_flaky_source():
    flaky = FlakySource()
    monitor = TrendMonitor([flaky], min_score=0.0, fetch_retry_attempts=1, fetch_retry_backoff=0.0)
    trends = monitor.update()
    assert any(trend.keyword == "run" for trend in trends)
    assert flaky.attempts == 2


def test_monitor_skips_duplicate_items():
    source = DuplicateSource()
    monitor = TrendMonitor([source], min_score=0.0)
    first = monitor.update()
    second = monitor.update()
    assert len(first[0].items) == 1
    assert len(second[0].items) == 1
    assert len(monitor.events) == 1
    assert len(monitor.seen_ids) == 1


def test_prune_removes_expired_items():
    monitor = TrendMonitor([AgingSource()], retention=dt.timedelta(hours=1), min_score=0.0)
    monitor.update()
    assert "old" not in monitor.seen_ids
    assert "fresh" in monitor.seen_ids
    assert len(monitor.events) == 1
