# SDA Assignment 3 – Real-Time User Activity Dashboard

## 1. Project Overview

This project implements a real-time streaming data pipeline for a Media & Entertainment use case. User activity events are generated through a Kafka producer, consumed by a Kafka consumer, stored in PostgreSQL, and visualized using Grafana Cloud.

The dashboard provides real-time visibility into user engagement across event types, content titles, and platforms.

## 2. Architecture

Producer (producer.py)
        ↓
Kafka Broker (localhost:9092)
        ↓
Kafka Topic: user_activity
        ↓
Consumer (consumer.py)
        ↓
Neon PostgreSQL
        ↓
Grafana Cloud
        ↓
Real-Time Dashboard

## 3. Technologies Used

- Python
- Apache Kafka
- Kafka-Python
- PostgreSQL
- Neon PostgreSQL
- Grafana Cloud
- SQL
- Docker

## 4. Kafka Topic and Data

Kafka topic:

user_activity

The streaming data contains the following fields:

- user_id
- event_type
- content_id
- content_title
- platform
- session_id
- search_query
- timestamp

The producer reads records from sample_data.csv, converts them into JSON messages, and sends them to the Kafka topic.

## 5. Consumer and Data Flow

The consumer.py program connects to the Kafka broker and consumes messages from the user_activity topic.

Each Kafka message is converted from JSON into structured fields and inserted into the user_activity table in Neon PostgreSQL.

The consumer therefore acts as the bridge between the Kafka streaming layer and the database used by the dashboard.

## 6. Database

The PostgreSQL table used for storing consumed events is:

user_activity

The database contains fields for user information, event type, content information, platform, session information, search queries, and event timestamps.

## 7. Grafana Dashboard

Dashboard name:

**SDA Assignment 3 – Real-Time User Activity Dashboard**

The dashboard contains four visualizations:

### Event Type Distribution
A bar chart showing the number of events for each activity type such as views, clicks, and searches.

### Content Engagement
A horizontal bar chart showing the number of recorded engagements for each content title.

### Platform Activity
A pie chart showing the distribution of user activity across Mobile App, Website, and Smart TV.

### Activity Over Time
A time-series visualization showing how user activity changes over time.

## 8. Business Insights

The dashboard provides real-time visibility into user activity across event types, content titles, and platforms. View events represent the largest activity category in the current dataset, indicating that content consumption is a major part of the observed user journey. Comedy Nights and The Last Horizon show the highest engagement among the available content titles. The platform analysis shows that the Mobile App contributes the largest share of observed activity, followed by Website and Smart TV.

These insights can support content promotion and recommendation decisions by highlighting content with higher engagement. Platform-level activity can also help teams focus engagement initiatives on the channels generating more user activity. Monitoring activity over time enables the organization to identify changes in user behaviour as new streaming events are generated, allowing decisions to be made using more current information rather than relying only on historical reports.

## 9. How to Run

### Start Kafka

Make sure the Kafka Docker container is running:

docker start kafka

### Start the Consumer

Open a terminal and run:

python consumer.py

### Start the Producer

Open another terminal and run:

python producer.py

The producer sends user activity events to the user_activity Kafka topic. The consumer reads these events and stores them in PostgreSQL.

### View Dashboard

Open the Grafana Cloud dashboard to monitor the consumed data through the configured PostgreSQL data source.

## 10. Project Files

- producer.py – Produces streaming user activity events to Kafka.
- consumer.py – Consumes Kafka events and stores them in PostgreSQL.
- sample_data.csv – Input data used by the producer.
- dashboard.json – Exported Grafana dashboard configuration.
- equirements.txt – Python dependencies.
- kafka_messages.png – Supporting evidence of Kafka message flow.

## 11. Security

Database credentials and environment variables are stored locally in .env.

The .env file must not be uploaded to GitHub.

