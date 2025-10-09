import asyncio

import pytest

from trend_monitor.sources import RSSSource, SourceConfig


class FakeResponse:
    def __init__(self, status_code, headers, content):
        self.status_code = status_code
        self.headers = headers
        self.content = content


class FakeAsyncClient:
    def __init__(self, responses):
        self._responses = responses
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, headers):
        self.requests.append(headers)
        if not self._responses:
            pytest.fail("Unexpected HTTP call")
        return self._responses.pop(0)


class FakeHTTPX:
    class RequestError(Exception):
        pass

    class Timeout:
        def __init__(self, timeout):
            self.timeout = timeout

    def __init__(self, responses):
        self.responses = responses
        self.client = FakeAsyncClient(self.responses)

    def AsyncClient(self, timeout):  # noqa: N802 - имитация API httpx
        return self.client


def test_rsssource_uses_conditional_requests(monkeypatch):
    sample_feed = b"""<?xml version='1.0'?><rss><channel><item><guid>1</guid><title>Test</title><link>https://example.com/1</link><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item></channel></rss>"""
    responses = [
        FakeResponse(200, {"ETag": "abc", "Last-Modified": "Mon"}, sample_feed),
        FakeResponse(304, {}, b""),
    ]
    fake_httpx = FakeHTTPX(responses)

    import trend_monitor.sources as sources

    monkeypatch.setattr(sources, "httpx", fake_httpx)

    source = RSSSource(SourceConfig(name="test", url="https://example.com/rss"))

    first = asyncio.run(source.fetch())
    assert len(first.items) == 1
    assert source._last_etag == "abc"
    assert source._last_modified == "Mon"

    second = asyncio.run(source.fetch())
    assert second.not_modified
    assert not second.items

    # Проверяем, что второй запрос был условным
    assert fake_httpx.client.requests[-1]["If-None-Match"] == "abc"
