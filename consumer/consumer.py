#!/usr/bin/env python3
"""
Kafka → TimescaleDB JSONB Consumer
------------------------------------
Stores each Kafka message as:
  ts   (timestamp)
  topic (Kafka topic)
  data (JSONB)
"""

import os
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json
from confluent_kafka import Consumer, KafkaException


# ===========================
#  CONFIG
# ===========================
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres@localhost:5432/logsdb"
)
GROUP_ID = "jsonb-log-consumer"


# ===========================
#  DATABASE HELPERS
# ===========================
def get_conn():
    return psycopg2.connect(POSTGRES_DSN)


def setup_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS logs (
        ts TIMESTAMPTZ NOT NULL,
        topic TEXT NOT NULL,
        data JSONB NOT NULL
    );
    """

    hypertable = """
    SELECT create_hypertable('logs', 'ts', if_not_exists => TRUE);
    """

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()

    cur.execute(hypertable)
    conn.commit()

    cur.close()
    conn.close()
    print("✅ JSONB hypertable ready: logs")


def insert_log(topic, payload):
    conn = get_conn()
    cur = conn.cursor()

    sql = """
    INSERT INTO logs (ts, topic, data)
    VALUES (%s, %s, %s);
    """

    try:
        cur.execute(sql, (datetime.now(timezone.utc), topic, Json(payload)))
        conn.commit()
    except Exception as e:
        print("❌ DB insert failed:", e)
    finally:
        cur.close()
        conn.close()


# ===========================
#  KAFKA CONSUMER LOGIC
# ===========================
def run_consumer():
    setup_table()

    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)

    # subscribe to all non-internal topics
    md = consumer.list_topics(timeout=5)
    topics = [t for t in md.topics.keys() if not t.startswith("_")]

    print("\n📡 Subscribing to topics:", topics)
    consumer.subscribe(topics)
    print("🔥 JSONB consumer running...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # Decode JSON safely
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                payload = {"raw": msg.value().decode("utf-8")}

            insert_log(msg.topic(), payload)
            print(f"✔ Stored log from topic '{msg.topic()}'")

    except KeyboardInterrupt:
        print("👋 Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
