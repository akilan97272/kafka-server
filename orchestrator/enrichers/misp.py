from pymisp import ExpandedPyMISP
import dotenv
import os
dotenv.load_dotenv()
MISP_URL = os.getenv("MISP_URL")
MISP_KEY = os.getenv("MISP_KEY")

misp = ExpandedPyMISP(MISP_URL, MISP_KEY, False)

async def enrich_misp(event):
    ip = event.get("ip")
    if not ip:
        return {}

    try:
        result = misp.search(controller='attributes', type_attribute='ip-src', value=ip)
        return result if result else {}
    except:
        return {}
