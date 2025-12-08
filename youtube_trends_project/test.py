from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka_broker:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(5):
    data = {"message": f"hello kafka #{i}"}
    producer.send("test_topic", value=data)
    print(f"Sent: {data}")
    time.sleep(1)

producer.flush()
print("Done.")