import psycopg2
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DSN = os.getenv("POSTGRES_DSN")


def fetch_logs_grouped():
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    # fetch data grouped by event_type
    sql = """
        SELECT event_type, time, process_name, severity, payload->'event'->>'SEVERITY_REASON'
        FROM logs
        where severity >= 4
        ORDER BY event_type, time DESC;
    """
    cur.execute(sql)

    rows = cur.fetchall()
    current_event_type = None

    for event_type, t, process_name, severity, payload in rows:

        # Print header when group changes
        if event_type != current_event_type:
            current_event_type = event_type
            print("\n==============================")
            print(f" EVENT TYPE: {event_type}")
            print("==============================")

        time_str = t.strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n  event type: {event_type}")
        print(f"  Time: {time_str}")
        print(f"  Process: {process_name}")
        print(f"  Severity: {severity}")
        print("->Payload JSON:")
        print(json.dumps(payload, indent=2))

    cur.close()
    conn.close()


if __name__ == "__main__":
    fetch_logs_grouped()
