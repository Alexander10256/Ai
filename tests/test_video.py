import datetime as dt

from trend_monitor.video import VideoMetadata, parse_video_metadata


def test_parse_video_metadata_from_jsonld():
    html = """
    <html>
      <head>
        <title>Fallback Title</title>
        <script type=\"application/ld+json\">
        {
          \"@context\": \"https://schema.org\",
          \"@type\": \"VideoObject\",
          \"name\": \"Amazing discovery\",
          \"description\": \"A great video about new trends\",
          \"uploadDate\": \"2024-05-01T12:34:56Z\",
          \"inLanguage\": \"en-US\",
          \"url\": \"https://example.com/watch?v=42\",
          \"author\": {\"@type\": \"Person\", \"name\": \"Researcher\", \"url\": \"https://example.com/u/researcher\"},
          \"keywords\": [\"innovation\", \"trend\", \"video\"],
          \"interactionStatistic\": [
            {\"@type\": \"InteractionCounter\", \"interactionType\": {\"@type\": \"WatchAction\"}, \"userInteractionCount\": 1337},
            {\"@type\": \"InteractionCounter\", \"interactionType\": {\"@type\": \"LikeAction\"}, \"userInteractionCount\": 250},
            {\"@type\": \"InteractionCounter\", \"interactionType\": {\"@type\": \"CommentAction\"}, \"userInteractionCount\": 17}
          ]
        }
        </script>
      </head>
    </html>
    """

    metadata = parse_video_metadata(html)
    assert isinstance(metadata, VideoMetadata)
    assert metadata.title == "Amazing discovery"
    assert metadata.author_name == "Researcher"
    assert metadata.author_url == "https://example.com/u/researcher"
    assert metadata.url == "https://example.com/watch?v=42"
    assert metadata.language == "en"
    assert metadata.keywords == ("innovation", "trend", "video")
    assert metadata.view_count == 1337
    assert metadata.like_count == 250
    assert metadata.comment_count == 17
    assert metadata.upload_date == dt.datetime(2024, 5, 1, 12, 34, 56)


def test_parse_video_metadata_with_meta_fallback():
    html = """
    <html>
      <head>
        <meta property=\"og:title\" content=\"Meta title\" />
        <meta property=\"og:description\" content=\"Meta description\" />
        <meta property=\"og:url\" content=\"https://example.com/video\" />
        <meta property=\"article:published_time\" content=\"2024-06-10T08:00:00Z\" />
        <meta name=\"interactionCount\" content=\"UserPlays:1024\" />
        <meta name=\"keywords\" content=\"alpha, beta; gamma|delta\" />
        <meta property=\"og:locale\" content=\"ru_RU\" />
      </head>
    </html>
    """

    metadata = parse_video_metadata(html)
    assert metadata is not None
    assert metadata.title == "Meta title"
    assert metadata.description == "Meta description"
    assert metadata.url == "https://example.com/video"
    assert metadata.language == "ru"
    assert metadata.view_count == 1024
    assert metadata.keywords == ("alpha", "beta", "gamma", "delta")
    assert metadata.upload_date == dt.datetime(2024, 6, 10, 8, 0)
