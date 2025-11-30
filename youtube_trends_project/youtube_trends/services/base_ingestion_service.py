from typing import Iterable

from config.kafka_settings import KafkaSettings
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer
from youtube_trends.infrastructure.youtube_client import YouTubeClient


class BaseIngestionService:
    """Base class for ingestion services."""

    def __init__(
        self,
        client: YouTubeClient,
        producer: AbstractKafkaProducer,
        kafka_settings: KafkaSettings,
    ) -> None:
        self._client = client
        self._producer = producer
        self._kafka_settings = kafka_settings

    def _send_batch(self, topic: str, models: Iterable) -> None:
        for m in models:
            d = m.to_dict()
            key = (
                d.get("video_id")
                or d.get("category_id")
                or d.get("region_code")
                or d.get("comment_id")
            )
            self._producer.send(topic, key=key, value=d)
