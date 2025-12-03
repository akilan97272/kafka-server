import psycopg2
import json
from datetime import datetime

POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/logsdb"


def fetch_logs_grouped():
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    # fetch data grouped by topic
    sql = """
        SELECT topic, ts, value
        FROM logs
        ORDER BY topic, ts DESC;
    """
    cur.execute(sql)

    rows = cur.fetchall()

    current_topic = None

    for topic, ts, value in rows:
        # Print topic header when topic changes
        if topic != current_topic:
            current_topic = topic
            print("\n==============================")
            print(f" TOPIC: {topic}")
            print("==============================")

        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n  Time: {ts_str}")
        print("📝 JSON:")
        print(json.dumps(value, indent=2))

    cur.close()
    conn.close()


if __name__ == "__main__":
    fetch_logs_grouped()
