import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

def get_high_severity_events(event_type=None, limit=20):
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()

    # Base query
    sql = """
        SELECT time, event_id, process_name, severity
        FROM hazard_process_events
        WHERE severity >= 3
    """

    params = []

    # Optional filter by event_type (PROCESS, FILE, NETWORK, etc.)
    if event_type:
        sql += " AND event_type = %s"
        params.append(event_type)

    # Order + limit
    sql += " ORDER BY time DESC LIMIT %s"
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()

    # Pretty print output
    for time, event_id, process_name, severity in rows:
        print("\n==========================")
        print(f"Time:        {time}")
        print(f"Event ID:    {event_id}")
        print(f"Process:     {process_name}")
        print(f"Severity:    {severity}")
        print("==========================")

    cur.close()
    conn.close()


# Example usage:
if __name__ == "__main__":
    get_high_severity_events(event_type="PROCESS")
