from typing import List

from config.kafka_settings import KafkaSettings
from youtube_trends.domain.region import Region
from youtube_trends.infrastructure.youtube_client import YouTubeClient
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer
from youtube_trends.services.base_ingestion_service import BaseIngestionService


class RegionIngestionService(BaseIngestionService):
    def __init__(
        self,
        client: YouTubeClient,
        producer: AbstractKafkaProducer,
        kafka_settings: KafkaSettings,
    ) -> None:
        super().__init__(client, producer, kafka_settings)

    def ingest_all(self) -> None:
        raw_items = self._client.get_regions()
        regions: List[Region] = []

        for item in raw_items:
            snippet = item["snippet"]
            regions.append(
                Region(
                    region_code=item["id"],
                    name=snippet.get("name", ""),
                )
            )

        self._send_batch(self._kafka_settings.topic_regions, regions)
