"""Парсер HTML-страниц видео для извлечения метаданных и активности."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import re
from html.parser import HTMLParser
from typing import Any

LOGGER = logging.getLogger(__name__)

_JSON_LD_RE = re.compile(
    r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)


@dataclasses.dataclass(frozen=True)
class VideoMetadata:
    """Структурированные данные о видео."""

    title: str
    description: str | None
    url: str | None
    upload_date: dt.datetime | None
    author_name: str | None
    author_url: str | None
    view_count: int | None
    like_count: int | None
    comment_count: int | None
    keywords: tuple[str, ...]
    language: str | None


def parse_video_metadata(html: str) -> VideoMetadata | None:
    """Извлекает метаданные видео из HTML страницы."""

    json_data = _extract_video_object(html)
    meta = _extract_meta_tags(html)

    title = _first_non_empty(
        (
            json_data.get("name") if json_data else None,
            meta.get("og:title"),
            meta.get("twitter:title"),
            meta.get("title"),
        )
    )
    if not title:
        return None

    description = _first_non_empty(
        (
            json_data.get("description") if json_data else None,
            meta.get("description"),
            meta.get("og:description"),
        )
    )

    url = _first_non_empty(
        (
            json_data.get("url") if json_data else None,
            _extract_url(json_data.get("mainEntityOfPage")) if json_data else None,
            meta.get("og:url"),
            meta.get("twitter:url"),
        )
    ) if json_data else _first_non_empty((meta.get("og:url"), meta.get("twitter:url")))

    upload_date = None
    if json_data:
        upload_date = _parse_date(json_data.get("uploadDate")) or _parse_date(json_data.get("datePublished"))
    if not upload_date:
        upload_date = _parse_date(meta.get("uploaddate")) or _parse_date(meta.get("article:published_time"))

    author_name, author_url = None, None
    if json_data:
        author_name, author_url = _extract_author(json_data.get("author"))
    if not author_name:
        author_name = _first_non_empty((meta.get("author"), meta.get("og:video:actor")))

    view_count = None
    like_count = None
    comment_count = None
    if json_data:
        view_count = _to_int(json_data.get("viewCount"))
        like_count = _to_int(json_data.get("likeCount"))
        comment_count = _to_int(json_data.get("commentCount"))
        stats = json_data.get("interactionStatistic")
        if stats:
            view_count = view_count or _extract_interaction_count(stats, "watch")
            like_count = like_count or _extract_interaction_count(stats, "like")
            comment_count = comment_count or _extract_interaction_count(stats, "comment")

    if view_count is None:
        view_count = _to_int(meta.get("interactioncount")) or _to_int(meta.get("og:video:views"))
    if like_count is None:
        like_count = _to_int(meta.get("og:video:likes"))
    if comment_count is None:
        comment_count = _to_int(meta.get("commentcount"))

    keywords: tuple[str, ...] = ()
    if json_data:
        keywords = _normalize_keywords(json_data.get("keywords"))
    if not keywords:
        keywords = _normalize_keywords(meta.get("keywords")) or _normalize_keywords(meta.get("og:video:tag"))

    language = None
    if json_data:
        language = _normalize_language(json_data.get("inLanguage"))
    if not language:
        language = _normalize_language(meta.get("og:locale"))

    return VideoMetadata(
        title=title,
        description=description,
        url=url,
        upload_date=upload_date,
        author_name=author_name,
        author_url=author_url,
        view_count=view_count,
        like_count=like_count,
        comment_count=comment_count,
        keywords=keywords,
        language=language,
    )


def _extract_video_object(html: str) -> dict[str, Any] | None:
    for match in _JSON_LD_RE.finditer(html):
        raw = match.group(1).strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            LOGGER.debug("JSON-LD parse error", exc_info=True)
            continue
        for candidate in _iter_video_objects(data):
            return candidate
    return None


def _iter_video_objects(node: Any):
    if isinstance(node, dict):
        node_type = node.get("@type")
        if _is_video_type(node_type):
            yield node
        for value in node.values():
            yield from _iter_video_objects(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_video_objects(item)


def _is_video_type(node_type: Any) -> bool:
    if isinstance(node_type, str):
        return node_type.lower().endswith("videoobject")
    if isinstance(node_type, list):
        return any(_is_video_type(item) for item in node_type)
    return False


class _MetaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.meta: dict[str, str] = {}
        self._in_title = False
        self._title_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "meta":
            mapping = {name.lower(): (value or "") for name, value in attrs}
            key = mapping.get("name") or mapping.get("property") or mapping.get("itemprop")
            content = mapping.get("content")
            if key and content:
                key_lower = key.lower()
                if key_lower not in self.meta:
                    self.meta[key_lower] = content.strip()
        elif tag.lower() == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_parts.append(data.strip())

    def data(self) -> dict[str, str]:
        result = dict(self.meta)
        if self._title_parts and "title" not in result:
            result["title"] = " ".join(part for part in self._title_parts if part)
        return result


def _extract_meta_tags(html: str) -> dict[str, str]:
    parser = _MetaParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:  # pragma: no cover - HTMLParser устойчив к ошибкам
        LOGGER.debug("Meta parsing error", exc_info=True)
    return parser.data()


def _extract_author(author_data: Any) -> tuple[str | None, str | None]:
    if isinstance(author_data, list):
        for item in author_data:
            name, url = _extract_author(item)
            if name:
                return name, url
        return None, None
    if isinstance(author_data, dict):
        name = author_data.get("name")
        url = _extract_url(author_data.get("url"))
        return name, url
    if isinstance(author_data, str):
        return author_data, None
    return None, None


def _extract_url(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("@id") or value.get("url")
    return None


def _extract_interaction_count(data: Any, interaction: str) -> int | None:
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return None
    target = interaction.lower()
    for entry in data:
        if not isinstance(entry, dict):
            continue
        interaction_type = entry.get("interactionType")
        type_name = _interaction_type_name(interaction_type)
        if not type_name:
            continue
        if target == "watch" and "watch" in type_name:
            return _to_int(entry.get("userInteractionCount") or entry.get("interactionCount"))
        if target == "like" and "like" in type_name:
            return _to_int(entry.get("userInteractionCount") or entry.get("interactionCount"))
        if target == "comment" and "comment" in type_name:
            return _to_int(entry.get("userInteractionCount") or entry.get("interactionCount"))
    return None


def _interaction_type_name(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("@type", "@id", "name"):
            result = value.get(key)
            if isinstance(result, str):
                return result.lower()
    if isinstance(value, str):
        return value.lower()
    return None


def _first_non_empty(candidates: tuple[str | None, ...]) -> str | None:
    for candidate in candidates:
        if isinstance(candidate, str):
            stripped = candidate.strip()
            if stripped:
                return stripped
    return None


def _parse_date(value: Any) -> dt.datetime | None:
    if not value:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, (int, float)):
        return dt.datetime.utcfromtimestamp(float(value))
    if isinstance(value, str):
        value = value.strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt_value = dt.datetime.strptime(value, fmt)
                if dt_value.tzinfo:
                    dt_value = dt_value.astimezone(dt.timezone.utc).replace(tzinfo=None)
                return dt_value
            except ValueError:
                continue
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        digits = re.findall(r"\d+", value)
        if digits:
            try:
                return int("".join(digits))
            except ValueError:
                return None
    return None


def _normalize_keywords(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in re.split(r",|;|\|", value) if item.strip()]
        return tuple(dict.fromkeys(items))
    if isinstance(value, list):
        items = []
        for item in value:
            if not isinstance(item, str):
                continue
            cleaned = item.strip()
            if cleaned and cleaned not in items:
                items.append(cleaned)
        return tuple(items)
    return ()


def _normalize_language(value: Any) -> str | None:
    if not value or not isinstance(value, str):
        return None
    value = value.strip().lower()
    if not value:
        return None
    if "-" in value:
        value = value.split("-", 1)[0]
    if "_" in value:
        value = value.split("_", 1)[0]
    return value or None

