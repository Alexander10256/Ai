"""Основной цикл мониторинга трендов."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import random
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from .analysis import Trend, score_trends
from .metrics import MetricsCollector, MetricsConfig
from .sources import BaseSource, FetchResult, RSSSource, SourceConfig, SourceError, SourceItem
from .storage import SQLiteStorageConfig, SQLiteTrendStorage

DEFAULT_SOURCES = [
    SourceConfig(
        name="Google Trends (US)",
        url="https://trends.google.com/trends/trendingsearches/daily/rss?geo=US",
        language="en",
        country="US",
    ),
    SourceConfig(name="Hacker News", url="https://hnrss.org/frontpage", language="en", country="US"),
    SourceConfig(name="Lenta.ru", url="https://lenta.ru/rss", language="ru", country="RU"),
]

LOGGER = logging.getLogger(__name__)


@dataclass
class Event:
    source: str
    item: SourceItem
    fingerprint: str
    seen_at: dt.datetime


class TrendMonitor:
    """Компонент для непрерывного мониторинга источников."""

    def __init__(
        self,
        sources: Iterable[BaseSource],
        *,
        retention: dt.timedelta = dt.timedelta(hours=12),
        decay_hours: float = 6.0,
        min_score: float = 0.4,
        top_k: int = 20,
        storage: SQLiteTrendStorage | None = None,
        fetch_retry_attempts: int = 3,
        fetch_retry_backoff: float = 2.0,
        fetch_concurrency: int = 5,
        dedup_ttl: dt.timedelta | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self.sources = list(sources)
        self.retention = retention
        self.decay_hours = decay_hours
        self.min_score = min_score
        self.top_k = top_k
        self.storage = storage
        self.events: deque[Event] = deque()
        self.fetch_retry_attempts = max(1, fetch_retry_attempts)
        self.fetch_retry_backoff = max(0.0, fetch_retry_backoff)
        self.fetch_concurrency = max(1, fetch_concurrency)
        self.dedup_ttl = dedup_ttl or retention
        self._seen_by_id: dict[str, dt.datetime] = {}
        self._seen_by_fp: dict[str, dt.datetime] = {}
        self.metrics = metrics or MetricsCollector.disabled()

    async def update_async(self) -> list[Trend]:
        start_time = time.perf_counter()
        now = dt.datetime.utcnow()
        semaphore = asyncio.Semaphore(self.fetch_concurrency)

        async def fetch_source(source: BaseSource) -> FetchResult:
            attempts = self.fetch_retry_attempts
            backoff = self.fetch_retry_backoff
            if hasattr(source, "config"):
                config = getattr(source, "config")
                attempts = max(1, getattr(config, "max_retries", attempts))
                backoff = max(0.0, getattr(config, "retry_backoff", backoff))

            for attempt in range(1, attempts + 1):
                try:
                    self.metrics.record_fetch_attempt(source.name)
                    async with semaphore:
                        result = await source.fetch()
                    self.metrics.record_fetch_success(source.name, not_modified=result.not_modified)
                    return result
                except SourceError as exc:
                    if attempt == attempts:
                        LOGGER.warning(
                            "%s: %s (attempt %s/%s, giving up)", source.name, exc, attempt, attempts
                        )
                        self.metrics.record_fetch_failure(source.name)
                        break
                    delay = backoff if backoff <= 1 else backoff ** (attempt - 1)
                    delay = max(0.0, delay)
                    jitter = random.uniform(0.5, 1.5)
                    wait_time = delay * jitter
                    self.metrics.record_retry(source.name)
                    LOGGER.warning(
                        "%s: %s (attempt %s/%s), retrying in %.2fs",
                        source.name,
                        exc,
                        attempt,
                        attempts,
                        wait_time,
                    )
                    if wait_time:
                        await asyncio.sleep(wait_time)
            return FetchResult(items=[], not_modified=False, headers=None)

        fetch_results = await asyncio.gather(*(fetch_source(source) for source in self.sources))

        new_events = 0
        for source, result in zip(self.sources, fetch_results):
            for item in result.items:
                fingerprint = item.fingerprint()
                if self._is_seen(item.id, fingerprint, now):
                    continue
                expiry = now + self.dedup_ttl
                if item.id:
                    self._seen_by_id[item.id] = expiry
                self._seen_by_fp[fingerprint] = expiry
                self.events.append(Event(source=source.name, item=item, fingerprint=fingerprint, seen_at=now))
                new_events += 1

        self.metrics.record_new_events(new_events)
        self._prune(now)
        self._cleanup_seen(now)

        trends = score_trends((event.item for event in self.events), now=now, decay_hours=self.decay_hours)
        filtered = [trend for trend in trends if trend.score >= self.min_score][: self.top_k]

        if self.storage:
            self.storage.save(filtered, generated_at=now)
            self.metrics.record_snapshot_saved()

        duration = time.perf_counter() - start_time
        self.metrics.record_iteration_duration(duration)
        return filtered

    def update(self) -> list[Trend]:
        return asyncio.run(self.update_async())

    def _prune(self, now: dt.datetime) -> None:
        threshold = now - self.retention
        while self.events and self.events[0].item.published < threshold:
            event = self.events.popleft()
            if event.item.id in self._seen_by_id:
                del self._seen_by_id[event.item.id]
            if event.fingerprint in self._seen_by_fp:
                del self._seen_by_fp[event.fingerprint]

    def _cleanup_seen(self, now: dt.datetime) -> None:
        for key, expiry in list(self._seen_by_id.items()):
            if expiry <= now:
                self._seen_by_id.pop(key, None)
        for key, expiry in list(self._seen_by_fp.items()):
            if expiry <= now:
                self._seen_by_fp.pop(key, None)

    def _is_seen(self, item_id: str, fingerprint: str, now: dt.datetime) -> bool:
        expiry_id = self._seen_by_id.get(item_id)
        if expiry_id and expiry_id > now:
            return True
        expiry_fp = self._seen_by_fp.get(fingerprint)
        return bool(expiry_fp and expiry_fp > now)

    def iter_trends(self, interval: dt.timedelta) -> Iterator[list[Trend]]:
        while True:
            yield self.update()
            time.sleep(interval.total_seconds())


def _build_sources(configs: Iterable[SourceConfig]) -> list[BaseSource]:
    sources: list[BaseSource] = []
    for config in configs:
        sources.append(RSSSource(config))
    return sources


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Мониторинг трендов из нескольких источников")
    parser.add_argument("--interval", type=int, default=900, help="Интервал обновления в секундах (по умолчанию 900)")
    parser.add_argument("--retention", type=int, default=12, help="Горизонт анализа в часах (по умолчанию 12)")
    parser.add_argument("--decay", type=float, default=6.0, help="Параметр экспоненциального затухания в часах")
    parser.add_argument("--min-score", type=float, default=0.4, help="Минимальный балл для тренда")
    parser.add_argument("--top", type=int, default=20, help="Количество трендов в выдаче")
    parser.add_argument("--storage", type=str, default="data/trends.sqlite", help="Путь к SQLite базе данных")
    parser.add_argument(
        "--fetch-retries",
        type=int,
        default=3,
        help="Количество попыток опроса источника перед отказом (по умолчанию 3)",
    )
    parser.add_argument(
        "--fetch-backoff",
        type=float,
        default=2.0,
        help="Базовый множитель экспоненциальной паузы между попытками (по умолчанию 2.0)",
    )
    parser.add_argument("--once", action="store_true", help="Сделать один проход и завершиться")
    parser.add_argument("--sources", type=str, help="Путь к JSON с дополнительными источниками")
    parser.add_argument("--verbose", action="store_true", help="Выводить отладочную информацию")
    parser.add_argument(
        "--fetch-concurrency",
        type=int,
        default=5,
        help="Максимальное количество одновременных запросов к источникам",
    )
    parser.add_argument(
        "--dedup-ttl",
        type=int,
        default=None,
        help="TTL дедупликации в минутах (по умолчанию равно retention)",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help="Порт для экспорта Prometheus метрик",
    )
    parser.add_argument(
        "--metrics-addr",
        type=str,
        default="0.0.0.0",
        help="Адрес для экспорта Prometheus метрик",
    )
    return parser.parse_args(argv)


def _load_additional_sources(path: str | None) -> list[SourceConfig]:
    if not path:
        return []
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    configs: list[SourceConfig] = []
    for item in payload:
        configs.append(
            SourceConfig(
                name=item["name"],
                url=item["url"],
                timeout=float(item.get("timeout", 30.0)),
                max_retries=int(item.get("max_retries", 3)),
                retry_backoff=float(item.get("retry_backoff", 2.0)),
                language=item.get("language"),
                country=item.get("country"),
            )
        )
    return configs


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    interval = dt.timedelta(seconds=args.interval)
    retention = dt.timedelta(hours=args.retention)

    storage_path = Path(os.path.abspath(args.storage))
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage = SQLiteTrendStorage(SQLiteStorageConfig(path=storage_path))

    configs = list(DEFAULT_SOURCES)
    configs.extend(_load_additional_sources(args.sources))
    sources = _build_sources(configs)

    metrics_config = MetricsConfig(
        enabled=args.metrics_port is not None,
        port=args.metrics_port,
        addr=args.metrics_addr,
    )
    metrics = MetricsCollector(metrics_config)

    dedup_ttl = retention if args.dedup_ttl is None else dt.timedelta(minutes=args.dedup_ttl)

    monitor = TrendMonitor(
        sources,
        retention=retention,
        decay_hours=args.decay,
        min_score=args.min_score,
        top_k=args.top,
        storage=storage,
        fetch_retry_attempts=args.fetch_retries,
        fetch_retry_backoff=args.fetch_backoff,
        fetch_concurrency=args.fetch_concurrency,
        dedup_ttl=dedup_ttl,
        metrics=metrics,
    )

    def handle_signal(signum, frame):  # pragma: no cover - зависит от ОС
        LOGGER.info("Получен сигнал %s, завершаем работу", signum)
        raise SystemExit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):  # pragma: no cover - зависит от ОС
        signal.signal(sig, handle_signal)

    try:
        for trends in monitor.iter_trends(interval=interval):
            _print_trends(trends)
            if args.once:
                break
    except KeyboardInterrupt:  # pragma: no cover - зависит от среды
        LOGGER.info("Остановка по Ctrl+C")

    return 0


def _print_trends(trends: Iterable[Trend]) -> None:
    now = dt.datetime.utcnow().isoformat()
    print(f"=== Топ трендов {now} UTC ===")
    for trend in trends:
        print(f"#{trend.keyword} — score {trend.score}")
        for item in trend.items[:3]:
            print(f"    • {item.title} ({item.url})")
    print()


if __name__ == "__main__":  # pragma: no cover - CLI
    sys.exit(main())
