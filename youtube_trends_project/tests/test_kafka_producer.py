from youtube_trends.infrastructure.kafka.json_kafka_producer import JsonKafkaProducer
from config.kafka_settings import KafkaSettings


def test_kafka_producer_serialization(monkeypatch):
    sent = {}

    class FakeProducer:
        def __init__(self, *_, **__):
            ...
        def send(self, topic, key, value):
            sent["topic"] = topic
            sent["key"] = key
            sent["value"] = value
        def flush(self): ...
        def close(self): ...

    import kafka
    monkeypatch.setattr(kafka, "KafkaProducer", FakeProducer)

    settings = KafkaSettings(bootstrap_servers="localhost:9092")
    producer = JsonKafkaProducer(settings)

    producer.send("test.topic", "k1", {"a": 1})

    assert sent["topic"] == "test.topic"
    assert sent["key"] == b"k1"
