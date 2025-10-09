"""Логика анализа и выделения трендов."""

from __future__ import annotations

import collections
import dataclasses
import datetime as dt
import math
import re
import unicodedata
from functools import lru_cache
from typing import Iterable

from .sources import SourceItem

try:  # pragma: no cover - внешняя зависимость может отсутствовать
    import snowballstemmer
except Exception:  # pragma: no cover - graceful fallback
    snowballstemmer = None  # type: ignore[assignment]

_WORD_RE = re.compile(r"[\w\-']{3,}", re.UNICODE)

_STOPWORDS = {
    "en": {
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
        "into",
        "after",
        "will",
        "trend",
        "news",
    },
    "ru": {
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
        "которые",
        "также",
        "тренд",
        "новости",
    },
}
_DEFAULT_STOPWORDS = set().union(*_STOPWORDS.values()) | {"новое", "new"}

_EN_SUFFIXES = (
    "ingly",
    "ously",
    "ations",
    "ation",
    "ingly",
    "ingly",
    "ments",
    "ment",
    "ings",
    "ing",
    "ers",
    "er",
    "ed",
    "ies",
    "s",
)
_RU_SUFFIXES = (
    "иями",
    "ями",
    "ами",
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
    "ии",
    "ую",
    "ешь",
    "ешься",
    "ете",
    "етеся",
)


@dataclasses.dataclass
class Trend:
    keyword: str
    score: float
    items: list[SourceItem]


def detect_language(text: str) -> str:
    if not text:
        return "other"
    latin = 0
    cyrillic = 0
    for char in text:
        lower = char.lower()
        if "a" <= lower <= "z":
            latin += 1
        elif "а" <= lower <= "я" or lower == "ё":
            cyrillic += 1
    if cyrillic and cyrillic >= latin * 1.2:
        return "ru"
    if latin and latin >= cyrillic * 1.2:
        return "en"
    return "other"


def extract_keywords(text: str, language: str | None = None) -> list[str]:
    if not text:
        return []
    language = language or detect_language(text)
    tokens = [token.lower() for token in _WORD_RE.findall(text.lower())]
    stopwords = _STOPWORDS.get(language, _DEFAULT_STOPWORDS)
    normalized = [_normalize_token(token, language) for token in tokens]
    return [token for token in normalized if token and token not in stopwords and len(token) > 2]


def score_trends(
    items: Iterable[SourceItem],
    now: dt.datetime,
    decay_hours: float = 6.0,
    *,
    title_weight: float = 1.0,
    summary_weight: float = 0.6,
) -> list[Trend]:
    weight_by_keyword: collections.Counter[str] = collections.Counter()
    items_by_keyword: dict[str, list[SourceItem]] = collections.defaultdict(list)
    decay_seconds = max(decay_hours, 0.0) * 3600
    for item in items:
        language = item.language or detect_language(f"{item.title} {(item.summary or '')}")
        title_keywords = extract_keywords(item.title, language)
        summary_keywords = extract_keywords(item.summary or "", language)
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


@lru_cache(maxsize=8)
def _get_stemmer(language: str):  # pragma: no cover - зависит от внешней библиотеки
    if snowballstemmer is None:
        return None
    try:
        return snowballstemmer.stemmer(language)
    except Exception:
        return None


def _normalize_token(token: str, language: str) -> str:
    token = unicodedata.normalize("NFKC", token.strip("-'\""))
    if not token:
        return ""
    if language in {"en", "ru"}:
        stemmer = _get_stemmer(language)
        if stemmer is not None:
            return stemmer.stemWord(token)
    if language == "en":
        return _normalize_en(token)
    if language == "ru":
        return _normalize_ru(token)
    return token


def _normalize_en(token: str) -> str:
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
    if len(token) > 3 and token.endswith("nn"):
        token = token[:-1]
    return token


def _normalize_ru(token: str) -> str:
    for suffix in _RU_SUFFIXES:
        if token.endswith(suffix) and len(token) - len(suffix) >= 3:
            token = token[: -len(suffix)]
            break
    return token.rstrip("ьй")


__all__ = ["Trend", "detect_language", "extract_keywords", "score_trends"]
