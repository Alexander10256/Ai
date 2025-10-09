"""Utilities for long-running мониторинг трендов."""

from .monitor import TrendMonitor
from .sources import RSSSource, SourceConfig

__all__ = ["TrendMonitor", "RSSSource", "SourceConfig"]
