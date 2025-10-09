"""Модули источников данных для мониторинга трендов."""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SourceConfig:
    """Конфигурация источника, используемая для сериализации."""

    name: str
    url: str
    interval: dt.timedelta | None = None


@dataclasses.dataclass(frozen=True)
class SourceItem:
    """Единица контента, полученная из источника."""

    id: str
    title: str
    url: str
    published: dt.datetime
    summary: str | None = None


class SourceError(RuntimeError):
    """Ошибка при запросе источника."""


class BaseSource:
    """Абстракция источника данных."""

    def __init__(self, config: SourceConfig):
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    def fetch(self) -> list[SourceItem]:
        raise NotImplementedError


class RSSSource(BaseSource):
    """Источник, основанный на RSS/Atom ленте."""

    USER_AGENT = "TrendMonitor/1.0 (+https://example.com/trend-monitor)"

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._last_modified: str | None = None
        self._last_etag: str | None = None

    def fetch(self) -> list[SourceItem]:
        request = urllib.request.Request(self.config.url, headers={"User-Agent": self.USER_AGENT})
        if self._last_modified:
            request.add_header("If-Modified-Since", self._last_modified)
        if self._last_etag:
            request.add_header("If-None-Match", self._last_etag)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status == 304:
                    return []
                self._last_modified = response.headers.get("Last-Modified")
                self._last_etag = response.headers.get("ETag")
                content_type = response.headers.get("Content-Type", "application/xml")
                charset = "utf-8"
                if "charset=" in content_type:
                    charset = content_type.split("charset=")[-1]
                raw_xml = response.read().decode(charset, errors="replace")
        except urllib.error.HTTPError as exc:  # pragma: no cover - зависит от сети
            if exc.code == 304:
                return []
            raise SourceError(f"HTTP error {exc.code} for {self.config.url}") from exc
        except urllib.error.URLError as exc:  # pragma: no cover - зависит от сети
            raise SourceError(f"Network error for {self.config.url}: {exc}") from exc

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
        return items

    def _parse_entry(self, entry: ET.Element) -> SourceItem | None:
        ns = "{http://www.w3.org/2005/Atom}"
        guid = entry.findtext("guid") or entry.findtext(f"{ns}id")
        if not guid:
            link = entry.find(f"{ns}link")
            if link is not None:
                guid = link.get("href")
            else:
                guid = entry.findtext("link")
        if not guid:
            return None

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

        return SourceItem(id=guid.strip(), title=title.strip(), url=url.strip(), published=published, summary=summary)


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
