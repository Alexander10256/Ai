"""Публичный интерфейс пакета trend_monitor."""

from .analysis import Trend, detect_language, extract_keywords, score_trends
from .metrics import MetricsCollector, MetricsConfig
from .monitor import TrendMonitor
from .sources import FetchResult, RSSSource, SourceConfig, SourceItem, VideoPageSource
from .storage import SQLiteStorageConfig, SQLiteTrendStorage
from .video import VideoMetadata, parse_video_metadata

__all__ = [
    "Trend",
    "TrendMonitor",
    "RSSSource",
    "SourceConfig",
    "SourceItem",
    "VideoPageSource",
    "FetchResult",
    "SQLiteTrendStorage",
    "SQLiteStorageConfig",
    "MetricsCollector",
    "MetricsConfig",
    "detect_language",
    "extract_keywords",
    "score_trends",
    "VideoMetadata",
    "parse_video_metadata",
]
