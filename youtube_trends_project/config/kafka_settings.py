from pydantic import BaseSettings, Field


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    topic_videos: str = Field("youtube.videos", env="KAFKA_TOPIC_VIDEOS")
    topic_categories: str = Field("youtube.categories", env="KAFKA_TOPIC_CATEGORIES")
    topic_regions: str = Field("youtube.regions", env="KAFKA_TOPIC_REGIONS")
    topic_comments: str = Field("youtube.comments", env="KAFKA_TOPIC_COMMENTS")
