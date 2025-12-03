#!/usr/bin/env python3
"""
Simple CLI producer:
- Send a single JSON message
- Or send many JSON lines from a file or stdin
Usage:
  # single message
  python producer.py --topic my-logs --msg '{"timestamp":"...","service":"auth",...}'

  # from file (one JSON object per line)
  python producer.py --topic my-logs --file logs.jsonl

  # from stdin
  cat logs.jsonl | python producer.py --topic my-logs
"""
import argparse
import sys
import json
from confluent_kafka import Producer
import os
from dotenv import load_dotenv
load_dotenv()

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for {msg.topic()} [{msg.partition()}] offset {msg.offset()}: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def create_producer(bootstrap):
    conf = {"bootstrap.servers": bootstrap}
    return Producer(conf)

def produce_line(producer, topic, line, key=None):
    producer.produce(topic, value=line.encode("utf-8"), key=key.encode("utf-8") if key else None, callback=delivery_report)
    producer.poll(0)  # serve delivery callbacks

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--topic", required=True)
    p.add_argument("--msg", help="Single JSON message string")
    p.add_argument("--file", help="File with JSON lines (one JSON object per line)")
    p.add_argument("--bootstrap", default=BOOTSTRAP)
    p.add_argument("--key", help="optional key for the message")
    args = p.parse_args()

    producer = create_producer(args.bootstrap)

    if args.msg:
        # validate JSON
        try:
            json.loads(args.msg)
        except Exception as e:
            print("Invalid JSON:", e); sys.exit(2)
        produce_line(producer, args.topic, args.msg, args.key)
    else:
        # read from file or stdin line-by-line
        fh = open(args.file, "r") if args.file else sys.stdin
        count = 0
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            # optionally validate
            try:
                json.loads(raw)
            except Exception as e:
                print("Skipping invalid JSON line:", e)
                continue
            produce_line(producer, args.topic, raw, args.key)
            count += 1
        if args.file:
            fh.close()
        print(f"Queued {count} messages for send")

    # flush outstanding messages
    producer.flush(timeout=10)
    print("Done")

if __name__ == "__main__":
    main()
