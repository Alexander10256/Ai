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

_TITLE_WEIGHT = 1.0
_SUMMARY_WEIGHT = 0.6

_EN_SUFFIXES = (
    "ing",
    "ers",
    "er",
    "ed",
    "s",
)
_RU_SUFFIXES = (
    "ами",
    "ями",
    "ов",
    "ев",
    "ых",
    "их",
    "ым",
    "им",
    "ах",
    "ях",
    "ый",
    "ий",
    "ое",
    "ая",
    "ые",
    "ие",
    "ую",
)


@dataclasses.dataclass
class Trend:
    keyword: str
    score: float
    items: list[SourceItem]


def extract_keywords(text: str) -> list[str]:
    raw_tokens = [token.lower() for token in _WORD_RE.findall(text.lower())]
    normalized = [_normalize_token(token) for token in raw_tokens]
    return [token for token in normalized if token and token not in _STOPWORDS and len(token) > 2]


def _normalize_token(token: str) -> str:
    token = token.strip("-'\"")
    if not token:
        return ""
    if token.endswith("'s"):
        token = token[:-2]
    elif token.endswith("'"):
        token = token[:-1]
    if token.endswith("ies") and len(token) > 4:
        token = token[:-3] + "y"
    if token.endswith("sses") and len(token) > 4:
        token = token[:-2]
    for suffix in _EN_SUFFIXES:
        if token.endswith(suffix) and len(token) - len(suffix) >= 3:
            token = token[: -len(suffix)]
            break
    for suffix in _RU_SUFFIXES:
        if token.endswith(suffix) and len(token) - len(suffix) >= 3:
            token = token[: -len(suffix)]
            break
    if len(token) > 3 and token.endswith("nn"):
        token = token[:-1]
    return token


def score_trends(
    items: Iterable[SourceItem],
    now: dt.datetime,
    decay_hours: float = 6.0,
    *,
    title_weight: float = _TITLE_WEIGHT,
    summary_weight: float = _SUMMARY_WEIGHT,
) -> list[Trend]:
    weight_by_keyword: collections.Counter[str] = collections.Counter()
    items_by_keyword: dict[str, list[SourceItem]] = collections.defaultdict(list)
    decay_seconds = max(decay_hours, 0.0) * 3600
    for item in items:
        title_keywords = extract_keywords(item.title)
        summary_keywords = extract_keywords(item.summary or "")
        if not title_keywords and not summary_keywords:
            continue
        age = max((now - item.published).total_seconds(), 0.0)
        if decay_seconds:
            base_weight = math.exp(-age / decay_seconds)
        else:
            base_weight = 1.0
        for keyword in title_keywords:
            weight_by_keyword[keyword] += base_weight * max(title_weight, 0.0)
            if item not in items_by_keyword[keyword]:
                items_by_keyword[keyword].append(item)
        for keyword in summary_keywords:
            weight_by_keyword[keyword] += base_weight * max(summary_weight, 0.0)
            if item not in items_by_keyword[keyword]:
                items_by_keyword[keyword].append(item)

    trends = [Trend(keyword=k, score=round(score, 3), items=items_by_keyword[k]) for k, score in weight_by_keyword.most_common()]
    return trends
