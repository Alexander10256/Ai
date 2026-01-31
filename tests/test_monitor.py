import datetime as dt

import asyncio

from trend_monitor.metrics import MetricsCollector
from trend_monitor.monitor import TrendMonitor
from trend_monitor.sources import BaseSource, FetchResult, SourceConfig, SourceError, SourceItem


class FlakySource(BaseSource):
    def __init__(self):
        super().__init__(SourceConfig(name="flaky", url="https://example.com/rss", max_retries=2, retry_backoff=0.1))
        self.attempts = 0

    async def fetch(self):
        self.attempts += 1
        if self.attempts < 2:
            raise SourceError("temporary failure")
        now = dt.datetime.utcnow()
        return FetchResult(
            items=[
                SourceItem(
                    id=f"flaky-{now.timestamp()}",
                    title="Run faster",
                    url="https://example.com/run",
                    published=now,
                    summary="",
                    language="en",
                )
            ]
        )


class DuplicateSource(BaseSource):
    def __init__(self):
        super().__init__(SourceConfig(name="dup", url="https://example.com/dup"))
        self.item = SourceItem(
            id="duplicate",
            title="Unique launch",
            url="https://example.com/launch",
            published=dt.datetime.utcnow(),
            summary="",
            language="en",
        )

    async def fetch(self):
        return FetchResult(items=[self.item])


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
                language="en",
            ),
            SourceItem(
                id="fresh",
                title="Fresh record",
                url="https://example.com/fresh",
                published=now,
                summary="",
                language="en",
            ),
        ]
        self._returned = False

    async def fetch(self):
        if self._returned:
            return FetchResult(items=[])
        self._returned = True
        return FetchResult(items=list(self._items))


def test_monitor_retries_flaky_source():
    flaky = FlakySource()
    monitor = TrendMonitor([flaky], min_score=0.0, fetch_retry_attempts=1, fetch_retry_backoff=0.1)
    trends = asyncio.run(monitor.update_async())
    assert any(trend.keyword == "run" for trend in trends)
    assert flaky.attempts == 2


def test_monitor_skips_duplicate_items():
    source = DuplicateSource()
    metrics = MetricsCollector()
    monitor = TrendMonitor([source], min_score=0.0, metrics=metrics)
    first = asyncio.run(monitor.update_async())
    second = asyncio.run(monitor.update_async())
    assert len(first[0].items) == 1
    assert len(second[0].items) == 1
    assert len(monitor.events) == 1
    assert "duplicate" in monitor._seen_by_id
    snapshot = metrics.snapshot()
    assert snapshot["fetch_success"] >= 2


def test_prune_and_dedup_ttl(tmp_path):
    source = AgingSource()
    monitor = TrendMonitor([source], retention=dt.timedelta(hours=1), dedup_ttl=dt.timedelta(minutes=30), min_score=0.0)
    asyncio.run(monitor.update_async())
    assert "old" not in monitor._seen_by_id
    assert "fresh" in monitor._seen_by_id
    # simulate time passing beyond TTL
    future = dt.datetime.utcnow() + dt.timedelta(hours=2)
    monitor._cleanup_seen(future)
    assert not monitor._seen_by_id
