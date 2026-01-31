"""SQLite-хранилище для результатов мониторинга."""

from __future__ import annotations

import dataclasses
import datetime as dt
import sqlite3
from pathlib import Path
from typing import Iterable

from .analysis import Trend


@dataclasses.dataclass
class SQLiteStorageConfig:
    path: Path
    retention: dt.timedelta | None = dt.timedelta(days=7)
    vacuum_every: int = 500


class SQLiteTrendStorage:
    """ACID-хранилище трендов на SQLite."""

    def __init__(self, config: SQLiteStorageConfig):
        self.config = config
        self._conn = sqlite3.connect(str(self.config.path))
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._init_schema()
        self._save_counter = 0

    def close(self) -> None:
        self._conn.close()

    def save(self, trends: Iterable[Trend], generated_at: dt.datetime) -> None:
        snapshot_id: int | None = None
        with self._conn:
            cursor = self._conn.execute(
                "INSERT INTO snapshots(generated_at) VALUES (?)",
                (generated_at.replace(microsecond=0).isoformat(),),
            )
            snapshot_id = int(cursor.lastrowid)
            for trend in trends:
                trend_cursor = self._conn.execute(
                    "INSERT INTO trends(snapshot_id, keyword, score) VALUES (?, ?, ?)",
                    (snapshot_id, trend.keyword, float(trend.score)),
                )
                trend_id = int(trend_cursor.lastrowid)
                for item in trend.items:
                    self._conn.execute(
                        """
                        INSERT INTO trend_items(trend_id, title, url, published, summary)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            trend_id,
                            item.title,
                            item.url,
                            item.published.replace(microsecond=0).isoformat(),
                            item.summary,
                        ),
                    )

            if self.config.retention is not None:
                threshold = (generated_at - self.config.retention).replace(microsecond=0).isoformat()
                self._conn.execute(
                    "DELETE FROM snapshots WHERE generated_at < ?",
                    (threshold,),
                )

        self._save_counter += 1
        if self.config.vacuum_every and self._save_counter % self.config.vacuum_every == 0:
            with self._conn:
                self._conn.execute("VACUUM")

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    generated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
                    keyword TEXT NOT NULL,
                    score REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS trend_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trend_id INTEGER NOT NULL REFERENCES trends(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    url TEXT,
                    published TEXT NOT NULL,
                    summary TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_trends_snapshot ON trends(snapshot_id);
                CREATE INDEX IF NOT EXISTS idx_trend_items_trend ON trend_items(trend_id);
                """
            )


__all__ = ["SQLiteTrendStorage", "SQLiteStorageConfig"]
