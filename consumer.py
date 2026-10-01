import json
import os
from datetime import datetime

from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv


# Load .env
load_dotenv()

# -----------------------------------------
# Kafka configuration
# -----------------------------------------

KAFKA_BROKER = "localhost:9092"
TOPIC = "user_activity"


# -----------------------------------------
# PostgreSQL configuration
# -----------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL not found. Check your .env file."
    )


# -----------------------------------------
# Connect to PostgreSQL
# -----------------------------------------

print("Connecting to Neon PostgreSQL...")

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("Connected to Neon PostgreSQL.")


# -----------------------------------------
# Connect to Kafka
# -----------------------------------------

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sda-assignment3",
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)

print(f"Connected to Kafka at {KAFKA_BROKER}")
print(f"Listening to topic: {TOPIC}")
print("-" * 60)


# -----------------------------------------
# Consume messages
# -----------------------------------------

try:

    for message in consumer:

        data = message.value

        user_id = data.get("user_id")
        event_type = data.get("event_type")
        content_id = data.get("content_id")
        content_title = data.get("content_title")
        platform = data.get("platform")
        session_id = data.get("session_id")
        search_query = data.get("search_query")
        timestamp = data.get("timestamp")

        # Convert ISO timestamp to Python datetime
        if timestamp:
            timestamp = datetime.fromisoformat(timestamp)

        insert_query = """
            INSERT INTO user_activity (
                user_id,
                event_type,
                content_id,
                content_title,
                platform,
                session_id,
                search_query,
                timestamp
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        cursor.execute(
            insert_query,
            (
                user_id,
                event_type,
                content_id,
                content_title,
                platform,
                session_id,
                search_query,
                timestamp
            )
        )

        conn.commit()

        print("Consumed and stored:")
        print(json.dumps(data, indent=2))
        print("-" * 60)


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


except Exception as e:

    print(f"\nERROR: {e}")
    conn.rollback()


finally:

    cursor.close()
    conn.close()
    consumer.close()

    print("Connections closed.")
