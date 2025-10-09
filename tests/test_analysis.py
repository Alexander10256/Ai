import datetime as dt

import pytest

from trend_monitor.analysis import detect_language, extract_keywords, score_trends
from trend_monitor.sources import SourceItem


def _item(keyword: str, *, summary: str = "", published: dt.datetime | None = None, language: str | None = None) -> SourceItem:
    if published is None:
        published = dt.datetime.utcnow()
    return SourceItem(
        id=f"{keyword}-{published.timestamp()}",
        title=f"{keyword.title()} breaking news",
        url="https://example.com/article",
        published=published,
        summary=summary,
        language=language,
    )


def test_detect_language_handles_cyrillic_and_latin():
    assert detect_language("новости технологии") == "ru"
    assert detect_language("latest tech news") == "en"
    assert detect_language("12345 !!!") == "other"


def test_extract_keywords_normalizes_forms():
    text = "Running runner's CATS stories"
    keywords = extract_keywords(text, language="en")
    assert "run" in keywords
    assert "cat" in keywords
    assert all(token not in {"running", "cats"} for token in keywords)


def test_extract_keywords_respects_language_stopwords():
    text = "Это новые новости про технологии"
    keywords = extract_keywords(text)
    assert "новости" not in keywords
    assert "технолог" in keywords  # после стемминга


def test_score_trends_prioritizes_title_keywords():
    now = dt.datetime.utcnow()
    title_item = _item("run", published=now, language="en")
    summary_item = _item("update", summary="Running tips", published=now, language="en")

    trends = score_trends([title_item, summary_item], now)
    run_trend = next(trend for trend in trends if trend.keyword == "run")

    assert run_trend.score == pytest.approx(1.6, rel=1e-3)
    assert run_trend.items == [title_item, summary_item]
