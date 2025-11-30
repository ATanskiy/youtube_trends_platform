from pydantic import BaseSettings, Field


class YouTubeSettings(BaseSettings):
    api_key: str = Field(..., env="YOUTUBE_API_KEY")
    base_url: str = Field("https://www.googleapis.com/youtube/v3", env="YOUTUBE_BASE_URL")