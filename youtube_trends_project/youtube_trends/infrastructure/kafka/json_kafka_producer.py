import json
from typing import Any, Dict, Optional

from kafka import KafkaProducer

from config.kafka_settings import KafkaSettings
from youtube_trends.infrastructure.kafka.abstract_kafka_producer import AbstractKafkaProducer


class JsonKafkaProducer(AbstractKafkaProducer):
    """Concrete Kafka producer that serializes values as JSON."""

    def __init__(self, settings: KafkaSettings):
        self._producer = KafkaProducer(
            bootstrap_servers=settings.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        )

    def send(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        self._producer.send(topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.close()
