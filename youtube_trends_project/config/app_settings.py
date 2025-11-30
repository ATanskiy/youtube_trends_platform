from functools import lru_cache
from pydantic import BaseSettings

from config.youtube_settings import YouTubeSettings
from config.kafka_settings import KafkaSettings
from config.spark_settings import SparkSettings


class AppSettings(BaseSettings):
    youtube: YouTubeSettings = YouTubeSettings()
    kafka: KafkaSettings = KafkaSettings()
    spark: SparkSettings = SparkSettings()


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings()
