from youtube_trends.infrastructure.youtube_client import YouTubeClient
from config.youtube_settings import YouTubeSettings


def test_youtube_client_builds_url(monkeypatch):
    settings = YouTubeSettings(api_key="fake", base_url="https://example.com")
    client = YouTubeClient(settings)

    called = {}

    def fake_get(url, params, timeout):
        called["url"] = url
        called["params"] = params

        class R:
            def raise_for_status(self): ...
            def json(self): return {"items": []}

        return R()

    import requests
    monkeypatch.setattr(requests, "get", fake_get)

    client.get_regions()

    assert "i18nRegions" in called["url"]
    assert called["params"]["key"] == "fake"
