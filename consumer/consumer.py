#!/usr/bin/env python3
"""
Kafka → TimescaleDB Structured Consumer (Safe Extract Version)
--------------------------------------------------------------
Stores logs in:
  - structured columns
  - full JSON (payload)
Handles uppercase/lowercase keys automatically.
"""

import os
import json
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
load_dotenv()

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
POSTGRES_DSN = os.getenv("POSTGRES_DSN")
GROUP_ID = os.getenv("GROUP_ID", "structured-log-consumer")

# ===========================
#  DATABASE HELPERS
# ===========================
def get_conn():
    return psycopg2.connect(POSTGRES_DSN)


def setup_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS logs (
        time              TIMESTAMPTZ NOT NULL,
        event_type        TEXT,
        event_id          UUID,
        process_name      TEXT,
        process_state     TEXT,
        severity          INT,
        severity_reason   TEXT,
        payload           JSONB
    );
    """

    hypertable = """
    SELECT create_hypertable('logs', 'time', if_not_exists => TRUE);
    """

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()

    cur.execute(hypertable)
    conn.commit()

    cur.close()
    conn.close()
    print("✅ Structured hypertable ready: logs")



# ===========================
#  SAFE JSON KEY GETTER
# ===========================
def safe_get(obj, *keys):
    """
    Case-insensitive nested getter.
    Example:
      safe_get(payload, "event", "PROCESS_NAME")
    Works even if keys are lowercase, uppercase, or mixed.
    """
    if not isinstance(obj, dict):
        return None

    for k in keys:
        found = False
        for actual_key in obj.keys():
            if actual_key.lower() == k.lower():
                obj = obj[actual_key]
                found = True
                break
        if not found:
            return None
    return obj



# ===========================
#  INSERT EVENT
# ===========================
def insert_event(payload):
    conn = get_conn()
    cur = conn.cursor()

    # 🔥 Extract values safely (uppercase/lowercase doesn't matter)
    event_type      = safe_get(payload, "routing", "event_type")
    event_id        = safe_get(payload, "routing", "event_id")
    process_name    = safe_get(payload, "event", "PROCESS_NAME")
    process_state   = safe_get(payload, "event", "PROCESS_STATE")
    severity        = safe_get(payload, "event", "SEVERITY") or safe_get(payload, "routing", "severity")
    severity_reason = safe_get(payload, "event", "SEVERITY_REASON") or safe_get(payload, "routing", "severity_reason")

    sql = """
    INSERT INTO logs (
        time, event_type, event_id, process_name,
        process_state, severity, severity_reason, payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    try:
        cur.execute(sql, (
            datetime.now(timezone.utc),
            event_type,
            event_id,
            process_name,
            process_state,
            severity,
            severity_reason,
            Json(payload)
        ))
        conn.commit()
        print("✔ Inserted structured event →", process_name, severity)

    except Exception as e:
        print("❌ DB insert failed:", e)

    finally:
        cur.close()
        conn.close()



# ===========================
#  KAFKA CONSUMER
# ===========================
def run_consumer():
    setup_table()

    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)

    # Subscribe to all user topics
    md = consumer.list_topics(timeout=5)
    topics = [t for t in md.topics.keys() if not t.startswith("_")]

    print("\n📡 Subscribing to topics:", topics)
    consumer.subscribe(topics)
    print("🔥 Structured consumer running...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # Decode JSON
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                payload = {"raw": msg.value().decode("utf-8")}

            # Debug print (optional)
            # print(json.dumps(payload, indent=2))

            insert_event(payload)

    except KeyboardInterrupt:
        print("👋 Stopping consumer...")

    finally:
        consumer.close()



if __name__ == "__main__":
    run_consumer()

