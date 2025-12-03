# backend/app.py
import os
import time
import json
import threading
import asyncio
from datetime import datetime
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

# Config from env
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@timescaledb:5432/logsdb")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Global admin + DB connection factory
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

def get_pg_conn():
    return psycopg2.connect(POSTGRES_DSN)

# Ensure table + hypertable exists
def ensure_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS logs (
      id BIGSERIAL PRIMARY KEY,
      topic TEXT NOT NULL,
      ts TIMESTAMPTZ NOT NULL,
      key TEXT,
      value JSONB,
      partition INT,
      offset BIGINT
    );
    """
    hypertable_sql = "SELECT create_hypertable('logs', 'ts', if_not_exists => TRUE);"
    conn = get_pg_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.execute(hypertable_sql)
    finally:
        conn.close()

ensure_tables()

# Background consumer thread that subscribes to topics and writes to DB.
class KafkaIngestor(threading.Thread):
    def __init__(self, bootstrap):
        super().__init__(daemon=True)
        self.bootstrap = bootstrap
        self.running = True
        # consumer config: unique group to avoid interfering with others
        self.conf = {
            "bootstrap.servers": self.bootstrap,
            "group.id": "ingestor-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        }
        self.consumer = Consumer(self.conf)
        self.topics = []
        self.refresh_interval = 30  # seconds

    def refresh_topics(self):
        try:
            md = admin.list_topics(timeout=5)
            topics = [t for t in md.topics.keys() if not t.startswith('__')]
            return topics
        except Exception as e:
            print("topic refresh error:", e)
            return self.topics

    def run(self):
        last_refresh = 0
        while self.running:
            # refresh topics periodically
            if time.time() - last_refresh > self.refresh_interval:
                new_topics = self.refresh_topics()
                if new_topics and set(new_topics) != set(self.topics):
                    self.topics = new_topics
                    try:
                        # subscribe to updated list
                        self.consumer.subscribe(self.topics)
                        print("consumer subscribed to topics:", self.topics)
                    except Exception as e:
                        print("consumer subscribe error:", e)
                last_refresh = time.time()

            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print("Kafka error:", msg.error())
                continue

            # build payload
            try:
                val_decoded = msg.value().decode("utf-8") if msg.value() else None
            except Exception:
                val_decoded = None

            row = {
                "topic": msg.topic(),
                "ts": datetime.utcnow(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": None,
                "partition": msg.partition(),
                "offset": msg.offset()
            }
            # attempt JSON parse
            try:
                row["value"] = json.loads(val_decoded) if val_decoded else None
            except Exception:
                # store raw string if not json
                row["value"] = val_decoded

            # store in DB
            try:
                conn = get_pg_conn()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO logs (topic, ts, key, value, partition, offset) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",
                            (row["topic"], row["ts"], row["key"], Json(row["value"]), row["partition"], row["offset"])
                        )
                        new_id = cur.fetchone()[0]
                conn.close()
            except Exception as e:
                print("db insert error:", e)
                continue

            # broadcast to any websocket clients (via global broadcaster)
            Broadcaster.broadcast_new({
                "type": "row",
                "row": {
                    "id": new_id,
                    "topic": row["topic"],
                    "ts": row["ts"].isoformat(),
                    "key": row["key"],
                    "value": row["value"],
                    "partition": row["partition"],
                    "offset": row["offset"]
                }
            })

    def stop(self):
        self.running = False
        try:
            self.consumer.close()
        except:
            pass

# Simple websocket manager to send live rows
class Broadcaster:
    # store set of websockets and their subscribed topic
    clients = set()  # set of (websocket, topic_or_none)

    @classmethod
    async def register(cls, websocket: WebSocket):
        cls.clients.add((websocket, None))

    @classmethod
    def unregister(cls, websocket: WebSocket):
        cls.clients = {(ws, t) for (ws, t) in cls.clients if ws != websocket}

    @classmethod
    def set_topic(cls, websocket: WebSocket, topic: str):
        cls.clients = {(ws, t) if ws != websocket else (ws, topic) for (ws, t) in cls.clients}

    @classmethod
    def broadcast_new(cls, payload: dict):
        # run in event loop tasks to send to matching clients
        loop = asyncio.get_event_loop()
        for (ws, topic) in list(cls.clients):
            try:
                if topic is None or payload["row"]["topic"] == topic:
                    # schedule coroutine
                    loop.create_task(ws.send_json(payload))
            except Exception:
                cls.unregister(ws)

ingestor = KafkaIngestor(BOOTSTRAP)
ingestor.start()

# REST: list topics and count
@app.get("/api/topics")
async def list_topics():
    try:
        md = admin.list_topics(timeout=5)
        topics = [t for t in md.topics.keys() if not t.startswith("__")]
        return JSONResponse({"count": len(topics), "topics": topics})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# REST: query logs (topic required)
@app.get("/api/logs")
async def get_logs(topic: str = Query(...), limit: int = 100, since: str = None):
    # since: ISO timestamp string optional
    try:
        conn = get_pg_conn()
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if since:
                    cur.execute(
                        "SELECT * FROM logs WHERE topic=%s AND ts >= %s ORDER BY ts DESC LIMIT %s",
                        (topic, since, limit)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM logs WHERE topic=%s ORDER BY ts DESC LIMIT %s",
                        (topic, limit)
                    )
                rows = cur.fetchall()
        conn.close()
        return JSONResponse(rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket: clients send {"type":"subscribe","topic":"topic-name"} to subscribe live
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    await Broadcaster.register(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                obj = json.loads(data)
            except:
                await websocket.send_json({"type": "error", "error": "invalid json"})
                continue
            if obj.get("type") == "subscribe":
                topic = obj.get("topic")
                # set client's topic
                Broadcaster.set_topic(websocket, topic)
                await websocket.send_json({"type": "info", "message": f"subscribed to {topic}"})
            elif obj.get("type") == "unsubscribe":
                Broadcaster.set_topic(websocket, None)
                await websocket.send_json({"type": "info", "message": "unsubscribed"})
            else:
                await websocket.send_json({"type": "error", "error": "unknown command"})
    except WebSocketDisconnect:
        Broadcaster.unregister(websocket)
    except Exception:
        Broadcaster.unregister(websocket)
