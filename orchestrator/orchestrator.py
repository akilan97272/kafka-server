import asyncio
import ujson as json
from aiokafka import AIOKafkaConsumer

from enrichers.otx import enrich_otx
from enrichers.misp import enrich_misp
from utils.db import write_event
from utils.severity import calculate_severity
import os
import dotenv
dotenv.load_dotenv()
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
TOPIC = "new_template"

async def process_event(event):
    otx_result = await enrich_otx(event)
    misp_result = await enrich_misp(event)

    revised, confidence = calculate_severity(event, otx_result, misp_result)

    event["otx"] = otx_result
    event["misp"] = misp_result
    event["revised_severity"] = revised
    event["confidence"] = confidence

    await write_event(event)


async def run():
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode())
    )

    await consumer.start()
    print("[Orchestrator] Running...")

    try:
        async for msg in consumer:
            asyncio.create_task(process_event(msg.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
