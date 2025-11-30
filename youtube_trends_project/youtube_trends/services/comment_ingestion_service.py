from datetime import datetime
from typing import List

from config.kafka_settings import KafkaSettings
from youtube_trends.domain.comment import Comment
from youtube_trends.infrastructure.youtube_client import YouTubeClient
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer
from youtube_trends.services.base_ingestion_service import BaseIngestionService


class CommentIngestionService(BaseIngestionService):
    def __init__(
        self,
        client: YouTubeClient,
        producer: AbstractKafkaProducer,
        kafka_settings: KafkaSettings,
    ) -> None:
        super().__init__(client, producer, kafka_settings)

    def ingest_for_video(self, video_id: str, max_results: int = 50) -> None:
        raw_items = self._client.get_comments(video_id, max_results=max_results)
        comments: List[Comment] = []

        for item in raw_items:
            top = item["snippet"]["topLevelComment"]["snippet"]
            comments.append(
                Comment(
                    comment_id=item["id"],
                    video_id=video_id,
                    author=top.get("authorDisplayName", ""),
                    text=top.get("textDisplay", ""),
                    like_count=int(top.get("likeCount", 0)),
                    published_at=datetime.fromisoformat(top["publishedAt"].replace("Z", "+00:00")),
                )
            )

        self._send_batch(self._kafka_settings.topic_comments, comments)
