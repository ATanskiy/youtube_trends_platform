from datetime import datetime
from typing import List

from config.kafka_settings import KafkaSettings
from youtube_trends.domain.video import Video
from youtube_trends.infrastructure.youtube_client import YouTubeClient
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer
from youtube_trends.services.base_ingestion_service import BaseIngestionService


class VideoIngestionService(BaseIngestionService):
    def __init__(
        self,
        client: YouTubeClient,
        producer: AbstractKafkaProducer,
        kafka_settings: KafkaSettings,
    ) -> None:
        super().__init__(client, producer, kafka_settings)

    def ingest_for_region(self, region_code: str, max_results: int = 50) -> None:
        raw_items = self._client.get_trending_videos(region_code, max_results=max_results)
        videos: List[Video] = []

        for item in raw_items:
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            videos.append(
                Video(
                    video_id=item["id"],
                    title=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    channel_id=snippet.get("channelId", ""),
                    channel_title=snippet.get("channelTitle", ""),
                    category_id=snippet.get("categoryId", ""),
                    published_at=datetime.fromisoformat(snippet["publishedAt"].replace("Z", "+00:00")),
                    region_code=region_code,
                    view_count=int(stats.get("viewCount", 0)) if stats.get("viewCount") else None,
                    like_count=int(stats.get("likeCount", 0)) if stats.get("likeCount") else None,
                    comment_count=int(stats.get("commentCount", 0)) if stats.get("commentCount") else None,
                )
            )

        self._send_batch(self._kafka_settings.topic_videos, videos)
