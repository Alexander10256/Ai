"""Prometheus-совместимые метрики для мониторинга трендов."""

from __future__ import annotations

import dataclasses
import logging
import threading
from typing import Any, Dict

try:  # pragma: no cover - внешняя зависимость может отсутствовать
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
except Exception:  # pragma: no cover - graceful fallback
    Counter = Histogram = Gauge = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)


class _NoopMetric:
    def labels(self, **_: Any) -> "_NoopMetric":
        return self

    def inc(self, *_: Any, **__: Any) -> None:  # pragma: no cover - нет логики
        return None

    def observe(self, *_: Any, **__: Any) -> None:  # pragma: no cover - нет логики
        return None

    def set(self, *_: Any, **__: Any) -> None:  # pragma: no cover - нет логики
        return None


@dataclasses.dataclass
class MetricsConfig:
    enabled: bool = False
    port: int | None = None
    addr: str = "0.0.0.0"


class MetricsCollector:
    """Инкапсулирует счётчики и экспорт Prometheus."""

    def __init__(self, config: MetricsConfig | None = None):
        self.config = config or MetricsConfig()
        self.enabled = bool(self.config.enabled or self.config.port is not None)
        self._lock = threading.Lock()
        self._snapshot: Dict[str, float] = {
            "fetch_attempts": 0,
            "fetch_success": 0,
            "fetch_not_modified": 0,
            "fetch_failures": 0,
            "fetch_retries": 0,
            "new_events": 0,
            "snapshots_saved": 0,
        }

        if self.enabled and Counter is None:
            LOGGER.warning(
                "Prometheus metrics requested but prometheus_client is not available"
            )
            self.enabled = False

        if self.enabled and self.config.port is not None and start_http_server is not None:
            start_http_server(self.config.port, addr=self.config.addr)
            LOGGER.info(
                "Started Prometheus metrics exporter on %s:%s",
                self.config.addr,
                self.config.port,
            )

        if self.enabled:
            self._fetch_attempts = Counter(
                "trend_monitor_fetch_attempts_total",
                "Количество запросов к источникам",
                labelnames=("source",),
            )
            self._fetch_success = Counter(
                "trend_monitor_fetch_success_total",
                "Количество успешных запросов",
                labelnames=("source",),
            )
            self._fetch_not_modified = Counter(
                "trend_monitor_fetch_not_modified_total",
                "Количество ответов 304",
                labelnames=("source",),
            )
            self._fetch_failures = Counter(
                "trend_monitor_fetch_failure_total",
                "Количество неудачных запросов",
                labelnames=("source",),
            )
            self._fetch_retries = Counter(
                "trend_monitor_fetch_retry_total",
                "Количество повторных попыток",
                labelnames=("source",),
            )
            self._iteration_duration = Histogram(
                "trend_monitor_iteration_duration_seconds",
                "Длительность одной итерации сбора",
            )
            self._new_events = Counter(
                "trend_monitor_new_events_total",
                "Количество новых событий",
            )
            self._snapshots_saved = Counter(
                "trend_monitor_snapshots_saved_total",
                "Количество сохранённых снимков",
            )
        else:  # fallback на no-op
            noop = _NoopMetric()
            self._fetch_attempts = noop
            self._fetch_success = noop
            self._fetch_not_modified = noop
            self._fetch_failures = noop
            self._fetch_retries = noop
            self._iteration_duration = noop
            self._new_events = noop
            self._snapshots_saved = noop

    @classmethod
    def disabled(cls) -> "MetricsCollector":
        return cls(MetricsConfig(enabled=False))

    def _inc(self, key: str, amount: float = 1.0) -> None:
        with self._lock:
            self._snapshot[key] = self._snapshot.get(key, 0.0) + amount

    def record_fetch_attempt(self, source: str) -> None:
        self._inc("fetch_attempts")
        self._fetch_attempts.labels(source=source).inc()

    def record_fetch_success(self, source: str, not_modified: bool = False) -> None:
        if not_modified:
            self._inc("fetch_not_modified")
            self._fetch_not_modified.labels(source=source).inc()
        else:
            self._inc("fetch_success")
            self._fetch_success.labels(source=source).inc()

    def record_fetch_failure(self, source: str) -> None:
        self._inc("fetch_failures")
        self._fetch_failures.labels(source=source).inc()

    def record_retry(self, source: str) -> None:
        self._inc("fetch_retries")
        self._fetch_retries.labels(source=source).inc()

    def record_iteration_duration(self, seconds: float) -> None:
        self._iteration_duration.observe(seconds)

    def record_new_events(self, count: int) -> None:
        if count <= 0:
            return
        self._inc("new_events", count)
        self._new_events.inc(count)

    def record_snapshot_saved(self) -> None:
        self._inc("snapshots_saved")
        self._snapshots_saved.inc()

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._snapshot)


__all__ = ["MetricsCollector", "MetricsConfig"]
