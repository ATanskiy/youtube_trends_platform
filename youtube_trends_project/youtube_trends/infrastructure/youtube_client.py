from typing import Any, Dict, List

import requests

from config.youtube_settings import YouTubeSettings


class YouTubeClient:
    """Low-level HTTP client for YouTube Data API v3."""

    def __init__(self, settings: YouTubeSettings):
        self._base_url = settings.base_url
        self._api_key = settings.api_key

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._base_url}/{path}"
        params = {**params, "key": self._api_key}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_trending_videos(self, region_code: str, max_results: int = 50) -> List[Dict[str, Any]]:
        data = self._get(
            "videos",
            {
                "part": "snippet,contentDetails,statistics",
                "chart": "mostPopular",
                "regionCode": region_code,
                "maxResults": max_results,
            },
        )
        return data.get("items", [])

    def get_video_categories(self, region_code: str) -> List[Dict[str, Any]]:
        data = self._get(
            "videoCategories",
            {
                "part": "snippet",
                "regionCode": region_code,
            },
        )
        return data.get("items", [])

    def get_regions(self) -> List[Dict[str, Any]]:
        data = self._get(
            "i18nRegions",
            {
                "part": "snippet",
            },
        )
        return data.get("items", [])

    def get_comments(self, video_id: str, max_results: int = 50) -> List[Dict[str, Any]]:
        data = self._get(
            "commentThreads",
            {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": max_results,
                "order": "time",
            },
        )
        return data.get("items", [])
