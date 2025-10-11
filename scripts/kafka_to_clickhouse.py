# scripts/kafka_to_clickhouse.py
import json, traceback
from kafka import KafkaConsumer
from clickhouse_connect import get_client

def main():
    ch = get_client(host="clickhouse", port=8123, username="default", password="", database="piccha")
    print("Connected to ClickHouse:", ch.command("SELECT version()"))

    consumer = KafkaConsumer(
    "customers","products","purchases","stores",
    bootstrap_servers="kafka:9092",
        group_id="piccha-consumer-reattach",   # новый group id
        auto_offset_reset='earliest',          # прочитать с начала
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )

    for msg in consumer:
        topic = msg.topic
        payload = msg.value
        try:
            raw_json_str = json.dumps(payload, ensure_ascii=False)
            ch.insert(table=f"piccha.raw_{topic}", data=[(topic, raw_json_str)], column_names=["source", "raw_json"])
            print(f"Insert success -> raw_{topic}")
        except Exception:
            print(f"Insert error for topic {topic}")
            print("Payload:", payload)
            traceback.print_exc()

    consumer.close()
    print("Finished")

if __name__ == "__main__":
    main()
