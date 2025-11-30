from config.app_settings import AppSettings, get_settings
from youtube_trends.infrastructure.youtube_client import YouTubeClient
from youtube_trends.infrastructure.kafka.json_kafka_producer import JsonKafkaProducer
from youtube_trends.services.video_ingestion_service import VideoIngestionService
from youtube_trends.services.category_ingestion_service import CategoryIngestionService
from youtube_trends.services.region_ingestion_service import RegionIngestionService
from youtube_trends.services.comment_ingestion_service import CommentIngestionService


class AppContainer:
    """Simple dependency injection container."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings or get_settings()

        # Low-level components
        self.youtube_client = YouTubeClient(self.settings.youtube)
        self.kafka_producer = JsonKafkaProducer(self.settings.kafka)

        # Services
        self.video_ingestion = VideoIngestionService(
            client=self.youtube_client,
            producer=self.kafka_producer,
            kafka_settings=self.settings.kafka,
        )
        self.category_ingestion = CategoryIngestionService(
            client=self.youtube_client,
            producer=self.kafka_producer,
            kafka_settings=self.settings.kafka,
        )
        self.region_ingestion = RegionIngestionService(
            client=self.youtube_client,
            producer=self.kafka_producer,
            kafka_settings=self.settings.kafka,
        )
        self.comment_ingestion = CommentIngestionService(
            client=self.youtube_client,
            producer=self.kafka_producer,
            kafka_settings=self.settings.kafka,
        )
