"""Модули источников данных для мониторинга трендов."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import hashlib
import logging
from collections.abc import Mapping
from typing import Any
import xml.etree.ElementTree as ET

from .video import VideoMetadata, parse_video_metadata

try:  # pragma: no cover - httpx не обязателен во время импорта
    import httpx
except Exception:  # pragma: no cover - graceful fallback
    httpx = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SourceConfig:
    """Конфигурация источника, используемая для сериализации."""

    name: str
    url: str
    interval: dt.timedelta | None = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 2.0
    language: str | None = None
    country: str | None = None
    kind: str = "rss"
    extra: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class SourceItem:
    """Единица контента, полученная из источника."""

    id: str
    title: str
    url: str
    published: dt.datetime
    summary: str | None = None
    language: str | None = None

    def fingerprint(self) -> str:
        base = "|".join(
            (
                self.id or "",
                self.url or "",
                self.title or "",
                self.published.isoformat(),
                self.language or "",
            )
        )
        digest = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()
        return f"sha1:{digest}"


class SourceError(RuntimeError):
    """Ошибка при запросе источника."""


class BaseSource:
    """Абстракция источника данных."""

    def __init__(self, config: SourceConfig):
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    async def fetch(self) -> "FetchResult":  # pragma: no cover - интерфейс
        raise NotImplementedError


@dataclasses.dataclass
class FetchResult:
    items: list[SourceItem]
    not_modified: bool = False
    headers: Mapping[str, Any] | None = None


class RSSSource(BaseSource):
    """Источник, основанный на RSS/Atom ленте."""

    USER_AGENT = "TrendMonitor/1.1 (+https://example.com/trend-monitor)"

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._last_modified: str | None = None
        self._last_etag: str | None = None

    async def fetch(self) -> FetchResult:
        headers = {"User-Agent": self.USER_AGENT}
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified
        if self._last_etag:
            headers["If-None-Match"] = self._last_etag

        if httpx is not None:
            request_coro = self._fetch_httpx(headers)
        else:  # pragma: no cover - используется только при отсутствии httpx
            request_coro = asyncio.to_thread(self._fetch_sync, headers)

        raw_xml, response_headers, not_modified = await request_coro
        if not_modified:
            return FetchResult(items=[], not_modified=True, headers=response_headers)

        self._last_modified = response_headers.get("Last-Modified")
        self._last_etag = response_headers.get("ETag")

        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError as exc:
            raise SourceError(f"Unable to parse feed {self.config.url}: {exc}") from exc

        channel = root.find("channel")
        if channel is not None:
            entries = channel.findall("item")
        else:
            entries = root.findall("{http://www.w3.org/2005/Atom}entry")

        items: list[SourceItem] = []
        for entry in entries:
            item = self._parse_entry(entry)
            if item:
                items.append(item)

        LOGGER.debug("Fetched %s items from %s", len(items), self.name)
        return FetchResult(items=items, not_modified=False, headers=response_headers)

    async def _fetch_httpx(
        self, headers: Mapping[str, str]
    ) -> tuple[str, Mapping[str, Any], bool]:
        timeout = httpx.Timeout(self.config.timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.get(self.config.url, headers=headers)
            except httpx.RequestError as exc:
                raise SourceError(f"Network error for {self.config.url}: {exc}") from exc

        if response.status_code == 304:
            return "", response.headers, True
        if response.status_code >= 400:
            raise SourceError(f"HTTP error {response.status_code} for {self.config.url}")
        content_type = response.headers.get("Content-Type", "application/xml")
        charset = "utf-8"
        if "charset=" in content_type:
            charset = content_type.split("charset=")[-1]
        raw_xml = response.content.decode(charset, errors="replace")
        return raw_xml, response.headers, False

    def _fetch_sync(
        self, headers: Mapping[str, str]
    ) -> tuple[str, Mapping[str, Any], bool]:  # pragma: no cover
        import urllib.error
        import urllib.request

        request = urllib.request.Request(self.config.url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout) as response:
                if response.status == 304:
                    return "", response.headers, True
                content_type = response.headers.get("Content-Type", "application/xml")
                charset = "utf-8"
                if "charset=" in content_type:
                    charset = content_type.split("charset=")[-1]
                payload = response.read().decode(charset, errors="replace")
                return payload, response.headers, False
        except urllib.error.HTTPError as exc:
            if exc.code == 304:
                return "", exc.headers, True
            raise SourceError(f"HTTP error {exc.code} for {self.config.url}") from exc
        except urllib.error.URLError as exc:
            raise SourceError(f"Network error for {self.config.url}: {exc}") from exc

    def _parse_entry(self, entry: ET.Element) -> SourceItem | None:
        ns = "{http://www.w3.org/2005/Atom}"
        guid = entry.findtext("guid") or entry.findtext(f"{ns}id")
        if not guid:
            link = entry.find(f"{ns}link")
            if link is not None:
                guid = link.get("href")
            else:
                guid = entry.findtext("link")

        title = entry.findtext("title") or entry.findtext(f"{ns}title") or "(без названия)"
        link_el = entry.find("link")
        url = ""
        if link_el is not None and link_el.text:
            url = link_el.text.strip()
        else:
            atom_link = entry.find(f"{ns}link")
            if atom_link is not None:
                url = atom_link.get("href", "")

        published_text = (
            entry.findtext("pubDate")
            or entry.findtext(f"{ns}updated")
            or entry.findtext(f"{ns}published")
        )
        published = _parse_datetime(published_text)

        summary = (
            entry.findtext("description")
            or entry.findtext(f"{ns}summary")
            or entry.findtext(f"{ns}content")
        )

        guid = guid.strip() if guid else ""
        if not guid:
            fingerprint = "|".join(
                (
                    url.strip(),
                    title.strip(),
                    published.isoformat(),
                )
            )
            guid = f"sha1:{hashlib.sha1(fingerprint.encode('utf-8', errors='ignore')).hexdigest()}"

        return SourceItem(
            id=guid,
            title=title.strip(),
            url=url.strip(),
            published=published,
            summary=summary,
            language=self.config.language,
        )


def _parse_datetime(raw: str | None) -> dt.datetime:
    if not raw:
        return dt.datetime.utcnow()
    raw = raw.strip()
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return dt.datetime.strptime(raw, fmt).astimezone(dt.timezone.utc).replace(tzinfo=None)
        except ValueError:
            continue
    LOGGER.debug("Не удалось распарсить дату %s, используется текущее время", raw)
    return dt.datetime.utcnow()


class VideoPageSource(BaseSource):
    """Источник, извлекающий активность из HTML-страницы видео."""

    USER_AGENT = "TrendMonitor/1.1 (+https://example.com/trend-monitor)"

    async def fetch(self) -> FetchResult:
        headers = {"User-Agent": self.USER_AGENT}
        if httpx is not None:
            html, response_headers = await self._fetch_httpx(headers)
        else:  # pragma: no cover - используется только при отсутствии httpx
            html, response_headers = await asyncio.to_thread(self._fetch_sync, headers)

        metadata = parse_video_metadata(html)
        if not metadata:
            raise SourceError(f"Не удалось извлечь данные видео со страницы {self.config.url}")

        now = dt.datetime.utcnow()
        max_description = 280
        limit = self.config.extra.get("summary_description_limit") if self.config.extra else None
        if isinstance(limit, (int, float)):
            max_description = max(0, int(limit))
        summary = _format_video_summary(metadata, max_description=max_description)
        published = now
        if self.config.extra.get("use_upload_date_as_published") and metadata.upload_date:
            published = metadata.upload_date
        language = metadata.language or self.config.language
        url = metadata.url or self.config.url
        title = metadata.title or self.config.name

        fingerprint_source = "|".join(
            (
                url,
                metadata.upload_date.isoformat() if metadata.upload_date else "",
                str(metadata.view_count or 0),
                str(metadata.like_count or 0),
                str(metadata.comment_count or 0),
            )
        )
        item_id = f"video:{hashlib.sha1(fingerprint_source.encode('utf-8', errors='ignore')).hexdigest()}"

        return FetchResult(
            items=[
                SourceItem(
                    id=item_id,
                    title=title,
                    url=url,
                    published=published,
                    summary=summary,
                    language=language,
                )
            ],
            not_modified=False,
            headers=response_headers,
        )

    async def _fetch_httpx(self, headers: Mapping[str, str]) -> tuple[str, Mapping[str, Any]]:
        timeout = httpx.Timeout(self.config.timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.get(self.config.url, headers=headers)
            except httpx.RequestError as exc:  # pragma: no cover - зависит от httpx
                raise SourceError(f"Network error for {self.config.url}: {exc}") from exc

        if response.status_code >= 400:
            raise SourceError(f"HTTP error {response.status_code} for {self.config.url}")
        charset = _detect_charset(response.headers.get("Content-Type"))
        html = response.content.decode(charset, errors="replace")
        return html, response.headers

    def _fetch_sync(self, headers: Mapping[str, str]) -> tuple[str, Mapping[str, Any]]:  # pragma: no cover
        import urllib.error
        import urllib.request

        request = urllib.request.Request(self.config.url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout) as response:
                if response.status >= 400:
                    raise SourceError(f"HTTP error {response.status} for {self.config.url}")
                charset = _detect_charset(response.headers.get("Content-Type"))
                payload = response.read().decode(charset, errors="replace")
                return payload, response.headers
        except urllib.error.HTTPError as exc:
            raise SourceError(f"HTTP error {exc.code} for {self.config.url}") from exc
        except urllib.error.URLError as exc:
            raise SourceError(f"Network error for {self.config.url}: {exc}") from exc


def _detect_charset(content_type: str | None) -> str:
    if not content_type:
        return "utf-8"
    lower = content_type.lower()
    if "charset=" in lower:
        charset = lower.split("charset=")[-1].split(";")[0].strip()
        if charset:
            return charset
    return "utf-8"


def _format_video_summary(metadata: VideoMetadata, *, max_description: int = 280) -> str | None:
    parts: list[str] = []
    if metadata.author_name:
        if metadata.author_url:
            parts.append(f"Автор: {metadata.author_name} ({metadata.author_url})")
        else:
            parts.append(f"Автор: {metadata.author_name}")

    metrics: list[str] = []
    if metadata.view_count is not None:
        metrics.append(f"просмотры {_format_number(metadata.view_count)}")
    if metadata.like_count is not None:
        metrics.append(f"лайки {_format_number(metadata.like_count)}")
    if metadata.comment_count is not None:
        metrics.append(f"комментарии {_format_number(metadata.comment_count)}")
    if metrics:
        parts.append("Метрики: " + ", ".join(metrics))

    if metadata.upload_date:
        parts.append("Загружено: " + metadata.upload_date.strftime("%Y-%m-%d %H:%M"))

    if metadata.keywords:
        parts.append("Теги: " + ", ".join(metadata.keywords[:5]))

    if metadata.description:
        description = metadata.description.strip()
        if max_description and len(description) > max_description:
            cutoff = max(0, max_description - 3)
            description = description[:cutoff].rstrip() + "…"
        parts.append(description)

    if not parts:
        return None
    return " | ".join(parts)


def _format_number(value: int) -> str:
    return f"{value:,}".replace(",", " ")
