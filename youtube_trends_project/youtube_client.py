from __future__ import annotations
import logging
from typing import Dict, Any, Iterable, Optional
import requests
import settings

logger = logging.getLogger(__name__)

class YouTubeClient:
    BASE_URL = settings.YOUTUBE_BASE_URL
    # "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()

    def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = params.copy()
        params["key"] = self.api_key
        url = f"{self.BASE_URL}/{path}"

        logger.debug("YouTube GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_regions(self, part: str = "snippet") -> Iterable[Dict[str, Any]]:
        data = self._get("i18nRegions", {"part": part})
        for item in data.get("items", []):
            yield item

    def get_video_categories(self, region_code: str = "US", part: str = "snippet") -> Iterable[Dict[str, Any]]:
        data = self._get("videoCategories", {"part": part, "regionCode": region_code})
        for item in data.get("items", []):
            yield item

    def get_videos(self, region_code: str = "US", chart: str = "mostPopular", max_results: int = 50,
                   part: str = "snippet,contentDetails,statistics") -> Iterable[Dict[str, Any]]:
        params = {"part": part, "chart": chart, "regionCode": region_code, "maxResults": max_results}
        data = self._get("videos", params)
        for item in data.get("items", []):
            yield item

    def get_comments(self, video_id: str, part: str = "snippet", max_results: int = 100) -> Iterable[Dict[str, Any]]:
        page_token = None
        while True:
            params = {"part": part, "videoId": video_id, "maxResults": max_results}
            if page_token:
                params["pageToken"] = page_token

            try:
                data = self._get("commentThreads", params)
            except requests.HTTPError as e:
                logger.warning("Failed to fetch comments for video %s: %s", video_id, e)
                break

            for item in data.get("items", []):
                yield item

            page_token = data.get("nextPageToken")
            if not page_token:
                break
