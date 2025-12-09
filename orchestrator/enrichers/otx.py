import aiohttp
from dotenv import load_dotenv
import os
load_dotenv()

OTX_KEY = os.getenv("OTX_API_KEY")

async def enrich_otx(event):
    ip = event.get("ip")
    if not ip:
        return {}

    url = f"https://otx.alienvault.com/api/v1/indicators/IPv4/{ip}/general"
    headers = {"X-OTX-API-KEY": OTX_KEY}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=5) as resp:
                if resp.status == 200:
                    return await resp.json()
    except:
        return {}

    return {}
