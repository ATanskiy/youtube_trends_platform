import json
from typing import Dict, Any
from kafka import KafkaProducer
import logging
from settings import (
    KAFKA_TOPIC_REGIONS,
    KAFKA_TOPIC_CATEGORIES,
    KAFKA_TOPIC_VIDEOS,
    KAFKA_TOPIC_COMMENTS,
    KAFKA_BOOTSTRAP_SERVERS,    
)

logger = logging.getLogger(__name__)

class KafkaProducerService:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),       
            key_serializer=lambda v: str(v).encode("utf-8") if v else None,
            linger_ms=10,
            retries=5,
        )

    def send(self, topic: str, key: str, value: Dict[str, Any]) -> None:
        logger.debug("Producing to %s key=%s value=%s", topic, key, value)
        self.producer.send(topic, key=key, value=value)

    def flush(self):
        self.producer.flush()

class YouTubeKafkaProducer:
    def __init__(self, producer_service: KafkaProducerService):
        self.producer_service = producer_service
        self.topic_regions = KAFKA_TOPIC_REGIONS
        self.topic_categories = KAFKA_TOPIC_CATEGORIES
        self.topic_videos = KAFKA_TOPIC_VIDEOS
        self.topic_comments = KAFKA_TOPIC_COMMENTS

    def send_region(self, region_obj: Dict[str, Any]):
        key = region_obj.get("id") or region_obj.get("snippet", {}).get("gl")
        self.producer_service.send(self.topic_regions, key, region_obj) 

    def send_category(self, category_obj: Dict[str, Any]):
        key = category_obj.get("id")
        self.producer_service.send(self.topic_categories, key, category_obj)

    def send_video(self, video_obj: Dict[str, Any]):
        key = video_obj.get("id")
        self.producer_service.send(self.topic_videos, key, video_obj)

    def send_comment(self, comment_obj: Dict[str, Any]):
        key = (
            comment_obj.get("id")
            or comment_obj.get("snippet", {}).get("topLevelComment", {}).get("id")
        )
        self.producer_service.send(self.topic_comments, key, comment_obj)
