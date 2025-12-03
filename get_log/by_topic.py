import psycopg2
import json

POSTGRES_DSN = "postgresql://postgres:postgres@localhost:5432/logsdb"


def get_logs_by_topic(topic, limit=20):
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    sql = """
        SELECT ts, data->'routing'->>'event_id'
        FROM logs
        WHERE topic = %s
        AND (data->'routing'->>'severity')::int >= 3
        ORDER BY ts DESC
        LIMIT %s;
    """

    cur.execute(sql, (topic, limit))
    rows = cur.fetchall()

    for ts, data in rows:
        print(f"\n[{ts}]")
        print("event_id: ",json.dumps(data, indent=2))

    cur.close()
    conn.close()


# Example usage:
if __name__ == "__main__":
    get_logs_by_topic("new_template")
