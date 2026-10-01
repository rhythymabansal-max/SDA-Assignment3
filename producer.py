import csv
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# Kafka connection
KAFKA_BROKER = "localhost:9092"
TOPIC = "user_activity"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8")
)

print(f"Connected to Kafka at {KAFKA_BROKER}")
print(f"Streaming data to topic: {TOPIC}")
print("-" * 60)

try:
    with open("sample_data.csv", "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Convert CSV fields into appropriate data types
            message = {
                "user_id": row["user_id"],
                "event_type": row["event_type"],
                "content_id": row["content_id"],
                "content_title": row["content_title"],
                "platform": row["platform"],
                "session_id": row["session_id"],
                "search_query": row["search_query"] or None,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            # Use user_id as the Kafka message key
            key = message["user_id"]

            # Send message to Kafka
            future = producer.send(TOPIC, key=key, value=message)

            # Wait for confirmation that Kafka received it
            future.get(timeout=10)

            print("Sent:", json.dumps(message))

            # Simulate real-time streaming
            time.sleep(1)

finally:
    producer.flush()
    producer.close()
    print("-" * 60)
    print("All messages sent. Producer closed.")
