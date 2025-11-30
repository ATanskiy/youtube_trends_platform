from typing import List

from config.kafka_settings import KafkaSettings
from youtube_trends.domain.category import Category
from youtube_trends.infrastructure.youtube_client import YouTubeClient
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer
from youtube_trends.services.base_ingestion_service import BaseIngestionService


class CategoryIngestionService(BaseIngestionService):
    def __init__(
        self,
        client: YouTubeClient,
        producer: AbstractKafkaProducer,
        kafka_settings: KafkaSettings,
    ) -> None:
        super().__init__(client, producer, kafka_settings)

    def ingest_for_region(self, region_code: str) -> None:
        raw_items = self._client.get_video_categories(region_code)
        categories: List[Category] = []

        for item in raw_items:
            snippet = item["snippet"]
            categories.append(
                Category(
                    category_id=item["id"],
                    title=snippet.get("title", ""),
                    assignable=bool(snippet.get("assignable", False)),
                )
            )

        self._send_batch(self._kafka_settings.topic_categories, categories)
