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
