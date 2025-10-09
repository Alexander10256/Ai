"""Модуль для сохранения истории трендов."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable

from .analysis import Trend


@dataclasses.dataclass
class StorageConfig:
    root: Path
    history_dir: str = "history"
    latest_filename: str = "latest.json"
    history_limit: int | None = 100

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
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
        latest_path = self.config.latest_path
        _atomic_write(latest_path, serialized)

        timestamp = generated_at.strftime("%Y%m%dT%H%M%S")
        history_path = self.config.history_path / f"trends_{timestamp}.json"
        _atomic_write(history_path, serialized)

        if self.config.history_limit is not None:
            history_files = sorted(self.config.history_path.glob("trends_*.json"))
            excess = len(history_files) - self.config.history_limit
            for old_file in history_files[:excess]:
                try:
                    old_file.unlink()
                except FileNotFoundError:
                    continue


__all__ = ["TrendStorage", "StorageConfig"]


def _atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_file: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            handle.write(data)
            tmp_file = handle.name
        os.replace(tmp_file, path)
        tmp_file = None
    finally:
        if tmp_file is not None:
            try:
                os.unlink(tmp_file)
            except FileNotFoundError:
                pass
