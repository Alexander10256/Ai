import datetime as dt

import pytest

from trend_monitor.analysis import extract_keywords, score_trends
from trend_monitor.sources import SourceItem


def _item(keyword: str, *, summary: str = "", published: dt.datetime | None = None) -> SourceItem:
    if published is None:
        published = dt.datetime.utcnow()
    return SourceItem(
        id=f"{keyword}-{published.timestamp()}",
        title=f"{keyword.title()} breaking news",
        url="https://example.com/article",
        published=published,
        summary=summary,
    )


def test_extract_keywords_normalizes_forms():
    text = "Running runner's CATS stories"
    keywords = extract_keywords(text)
    assert "run" in keywords
    assert "cat" in keywords
    assert all(token not in {"running", "cats"} for token in keywords)


def test_score_trends_prioritizes_title_keywords():
    now = dt.datetime.utcnow()
    title_item = _item("run", published=now)
    summary_item = _item("update", summary="Running tips", published=now)

    trends = score_trends([title_item, summary_item], now)
    run_trend = next(trend for trend in trends if trend.keyword == "run")

    assert run_trend.score == pytest.approx(1.6, rel=1e-3)
    assert run_trend.items == [title_item, summary_item]
