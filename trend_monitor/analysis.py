"""Логика анализа и выделения трендов."""

from __future__ import annotations

import collections
import dataclasses
import datetime as dt
import math
import re
from typing import Iterable

from .sources import SourceItem

_WORD_RE = re.compile(r"[\w\-']{3,}", re.UNICODE)
_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "have",
    "your",
    "about",
    "что",
    "это",
    "как",
    "так",
    "она",
    "они",
    "или",
    "если",
    "чтобы",
    "когда",
    "будет",
    "тренд",
    "trend",
    "новое",
}


@dataclasses.dataclass
class Trend:
    keyword: str
    score: float
    items: list[SourceItem]


def extract_keywords(text: str) -> list[str]:
    tokens = [token.lower() for token in _WORD_RE.findall(text.lower())]
    return [token for token in tokens if token not in _STOPWORDS and len(token) > 2]


def score_trends(items: Iterable[SourceItem], now: dt.datetime, decay_hours: float = 6.0) -> list[Trend]:
    weight_by_keyword: collections.Counter[str] = collections.Counter()
    items_by_keyword: dict[str, list[SourceItem]] = collections.defaultdict(list)
    decay = decay_hours * 3600
    for item in items:
        summary = item.summary or ""
        text = f"{item.title}. {summary}"
        keywords = extract_keywords(text)
        if not keywords:
            continue
        age = (now - item.published).total_seconds()
        weight = math.exp(-max(age, 0) / decay)
        for keyword in keywords:
            weight_by_keyword[keyword] += weight
            if item not in items_by_keyword[keyword]:
                items_by_keyword[keyword].append(item)

    trends = [Trend(keyword=k, score=round(score, 3), items=items_by_keyword[k]) for k, score in weight_by_keyword.most_common()]
    return trends
