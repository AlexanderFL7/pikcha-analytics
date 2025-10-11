from kafka import KafkaConsumer
import json

c = KafkaConsumer(
    "purchases",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset='earliest',
    consumer_timeout_ms=3000,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for i, msg in enumerate(c):
    print(msg.value)
    if i >= 4:
        break
