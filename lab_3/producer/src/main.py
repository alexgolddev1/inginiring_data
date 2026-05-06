import csv
import json
import os
import time
from typing import Dict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def build_producer() -> KafkaProducer:
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "broker1:29092,broker2:29093").split(",")
    max_attempts = int(os.getenv("PRODUCER_CONNECT_ATTEMPTS", "30"))
    retry_delay_seconds = int(os.getenv("PRODUCER_CONNECT_DELAY_SECONDS", "5"))

    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as error:
            last_error = error
            print(
                f"Kafka brokers are not ready yet. Attempt {attempt}/{max_attempts}. "
                f"Retrying in {retry_delay_seconds}s...",
                flush=True,
            )
            time.sleep(retry_delay_seconds)

    raise RuntimeError("Could not connect to Kafka brokers after repeated attempts") from last_error


def normalize_row(row: Dict[str, str]) -> Dict[str, object]:
    normalized = {key: value.strip() for key, value in row.items()}
    trip_duration = normalized.get("tripduration", "0").replace(",", "")
    birth_year = normalized.get("birthyear", "")

    return {
        "event_type": "divvy_trip_created",
        "trip_id": normalized["trip_id"],
        "start_time": normalized["start_time"],
        "end_time": normalized["end_time"],
        "bike_id": normalized["bikeid"],
        "trip_duration_seconds": float(trip_duration or 0),
        "from_station": {
            "id": normalized["from_station_id"],
            "name": normalized["from_station_name"],
        },
        "to_station": {
            "id": normalized["to_station_id"],
            "name": normalized["to_station_name"],
        },
        "user_type": normalized["usertype"],
        "gender": normalized["gender"],
        "birth_year": int(float(birth_year)) if birth_year else None,
    }


def main() -> None:
    csv_path = os.getenv("CSV_PATH", "/app/data/Divvy_Trips_2019_Q4.csv")
    topic_one = os.getenv("TOPIC_ONE", "Topic1")
    topic_two = os.getenv("TOPIC_TWO", "Topic2")
    delay_ms = int(os.getenv("PRODUCER_DELAY_MS", "0"))

    producer = build_producer()
    sent_messages = 0

    with open(csv_path, newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        for row in reader:
            event = normalize_row(row)
            key = event["trip_id"]

            producer.send(topic_one, key=key, value=event)
            producer.send(topic_two, key=key, value=event)
            sent_messages += 1

            if sent_messages % 10000 == 0:
                print(f"Sent {sent_messages} records to {topic_one} and {topic_two}", flush=True)

            if delay_ms > 0:
                time.sleep(delay_ms / 1000)

    producer.flush()
    producer.close()
    print(f"Completed publishing {sent_messages} CSV rows to both topics.", flush=True)


if __name__ == "__main__":
    main()
