from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class AbstractKafkaProducer(ABC):
    """Interface for Kafka producers."""

    @abstractmethod
    def send(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> None:
        ...
