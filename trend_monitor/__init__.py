"""Публичный интерфейс пакета trend_monitor."""

from .analysis import Trend, detect_language, extract_keywords, score_trends
from .metrics import MetricsCollector, MetricsConfig
from .monitor import TrendMonitor
from .sources import FetchResult, RSSSource, SourceConfig, SourceItem
from .storage import SQLiteStorageConfig, SQLiteTrendStorage

__all__ = [
    "Trend",
    "TrendMonitor",
    "RSSSource",
    "SourceConfig",
    "SourceItem",
    "FetchResult",
    "SQLiteTrendStorage",
    "SQLiteStorageConfig",
    "MetricsCollector",
    "MetricsConfig",
    "detect_language",
    "extract_keywords",
    "score_trends",
]
