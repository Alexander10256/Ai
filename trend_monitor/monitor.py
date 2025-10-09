"""Основной цикл мониторинга трендов."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from pathlib import Path
from dataclasses import dataclass
from typing import Iterable, Iterator

from .analysis import Trend, score_trends
from .sources import BaseSource, RSSSource, SourceConfig, SourceError, SourceItem
from .storage import StorageConfig, TrendStorage

DEFAULT_SOURCES = [
    SourceConfig(name="Google Trends (US)", url="https://trends.google.com/trends/trendingsearches/daily/rss?geo=US"),
    SourceConfig(name="Hacker News", url="https://hnrss.org/frontpage"),
    SourceConfig(name="Lenta.ru", url="https://lenta.ru/rss"),
]

LOGGER = logging.getLogger(__name__)


@dataclass
class Event:
    source: str
    item: SourceItem


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
        storage: TrendStorage | None = None,
        fetch_retry_attempts: int = 3,
        fetch_retry_backoff: float = 2.0,
    ) -> None:
        self.sources = list(sources)
        self.retention = retention
        self.decay_hours = decay_hours
        self.min_score = min_score
        self.top_k = top_k
        self.storage = storage
        self.events: deque[Event] = deque()
        self.seen_ids: set[str] = set()
        self.fetch_retry_attempts = max(1, fetch_retry_attempts)
        self.fetch_retry_backoff = max(0.0, fetch_retry_backoff)

    def update(self) -> list[Trend]:
        now = dt.datetime.utcnow()
        for source in self.sources:
            attempts = self.fetch_retry_attempts
            backoff = self.fetch_retry_backoff
            if hasattr(source, "config"):
                config = getattr(source, "config")
                attempts = max(1, getattr(config, "max_retries", attempts))
                backoff = max(0.0, getattr(config, "retry_backoff", backoff))

            items: list[SourceItem] | None = None
            for attempt in range(1, attempts + 1):
                try:
                    items = source.fetch()
                    break
                except SourceError as exc:
                    if attempt == attempts:
                        LOGGER.warning("%s: %s (attempt %s/%s, giving up)", source.name, exc, attempt, attempts)
                    else:
                        delay = backoff if backoff <= 1 else backoff ** (attempt - 1)
                        delay = max(0.0, delay)
                        LOGGER.warning(
                            "%s: %s (attempt %s/%s), retrying in %.1fs",
                            source.name,
                            exc,
                            attempt,
                            attempts,
                            delay,
                        )
                        if delay:
                            time.sleep(delay)
            if items is None:
                continue
            for item in items:
                if item.id in self.seen_ids:
                    continue
                self.seen_ids.add(item.id)
                self.events.append(Event(source=source.name, item=item))

        self._prune(now)
        trends = score_trends((event.item for event in self.events), now=now, decay_hours=self.decay_hours)
        filtered = [trend for trend in trends if trend.score >= self.min_score][: self.top_k]

        if self.storage:
            self.storage.save(filtered, generated_at=now)

        return filtered

    def _prune(self, now: dt.datetime) -> None:
        threshold = now - self.retention
        while self.events and self.events[0].item.published < threshold:
            event = self.events.popleft()
            self.seen_ids.discard(event.item.id)

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
    parser.add_argument("--storage", type=str, default="data", help="Каталог для сохранения результатов")
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

    storage_root = os.path.abspath(args.storage)
    os.makedirs(storage_root, exist_ok=True)
    storage = TrendStorage(StorageConfig(root=Path(storage_root)))

    configs = list(DEFAULT_SOURCES)
    configs.extend(_load_additional_sources(args.sources))
    sources = _build_sources(configs)

    monitor = TrendMonitor(
        sources,
        retention=retention,
        decay_hours=args.decay,
        min_score=args.min_score,
        top_k=args.top,
        storage=storage,
        fetch_retry_attempts=args.fetch_retries,
        fetch_retry_backoff=args.fetch_backoff,
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
