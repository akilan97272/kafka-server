import psycopg
import os
import dotenv
import ujson as json

dotenv.load_dotenv()
DSN = os.getenv("POSTGRES_DSN")

async def write_event(event):
    async with await psycopg.AsyncConnection.connect(DSN) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO enriched_events (
                    event_id,
                    source,
                    host,
                    ip,
                    message,
                    initial_severity,
                    revised_severity,
                    confidence,
                    threat_data,
                    created_at
                )
                VALUES (
                    %(event_id)s,
                    %(source)s,
                    %(host)s,
                    %(ip)s,
                    %(message)s,
                    %(initial_severity)s,
                    %(revised_severity)s,
                    %(confidence)s,
                    %(threat_data)s,
                    NOW()
                )
            """, {
                **event,
                "threat_data": json.dumps(event)
            })
