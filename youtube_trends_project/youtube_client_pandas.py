import logging
from typing import Optional, List
import requests
import pandas as pd
import settings

# Set the display width to a specific number of characters
pd.set_option('display.width', 1000) 

# Set the maximum column width for individual columns
pd.set_option('display.max_colwidth', 50) 

logger = logging.getLogger(__name__)

class YouTubeClientPandas:
    BASE_URL = settings.YOUTUBE_BASE_URL

    def __init__(self, api_key: str, session: Optional[requests.Session] = None):
        self.api_key = api_key
        self.session = session or requests.Session()

    def _get(self, path: str, params: dict) -> dict:
        params = params.copy()
        params["key"] = self.api_key
        url = f"{self.BASE_URL}/{path}"
        logger.debug("YouTube GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    # -----------------------------
    # Pandas DataFrame methods
    # -----------------------------

    def get_regions_df(self) -> pd.DataFrame:
        data = self._get("i18nRegions", {"part": "snippet"})
        items = data.get("items", [])
        df = pd.json_normalize(items)
        return df

    def get_video_categories_df(self, region: str) -> pd.DataFrame:                
        data = self._get("videoCategories", {"part": "snippet", "regionCode": region})
        items = data.get("items", [])
        df = pd.json_normalize(items)            
        return df

    def get_videos_df(self, region_code="US", chart="mostPopular", max_results=50) -> pd.DataFrame:
        params = {
            "part": "snippet,contentDetails,statistics",
            "chart": chart,
            "regionCode": region_code,
            "maxResults": max_results
        }
        data = self._get("videos", params)
        items = data.get("items", [])
        df = pd.json_normalize(items)
        return df

    def get_comments_df(self, video_id: str, max_results=100) -> pd.DataFrame:
        all_items: List[dict] = []
        page_token: Optional[str] = None

        while True:
            params = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": max_results
            }
            if page_token:
                params["pageToken"] = page_token

            try:
                data = self._get("commentThreads", params)
            except requests.HTTPError as e:
                logger.warning("Failed to fetch comments for video %s: %s", video_id, e)
                break

            items = data.get("items", [])
            all_items.extend(items)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        df = pd.json_normalize(all_items)
        return df
