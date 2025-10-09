"""Модуль для сохранения истории трендов."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from pathlib import Path
from typing import Iterable

from .analysis import Trend


@dataclasses.dataclass
class StorageConfig:
    root: Path
    history_dir: str = "history"
    latest_filename: str = "latest.json"

    @property
    def history_path(self) -> Path:
        return self.root / self.history_dir

    @property
    def latest_path(self) -> Path:
        return self.root / self.latest_filename


class TrendStorage:
    """Простое файловое хранилище для результатов."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.config.history_path.mkdir(parents=True, exist_ok=True)

    def save(self, trends: Iterable[Trend], generated_at: dt.datetime) -> None:
        payload = {
            "generated_at": generated_at.isoformat(),
            "trends": [
                {
                    "keyword": trend.keyword,
                    "score": trend.score,
                    "items": [
                        {
                            "title": item.title,
                            "url": item.url,
                            "published": item.published.isoformat(),
                            "summary": item.summary,
                        }
                        for item in trend.items
                    ],
                }
                for trend in trends
            ],
        }
        latest_path = self.config.latest_path
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        timestamp = generated_at.strftime("%Y%m%dT%H%M%S")
        history_path = self.config.history_path / f"trends_{timestamp}.json"
        history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


__all__ = ["TrendStorage", "StorageConfig"]
