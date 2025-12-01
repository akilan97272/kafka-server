import asyncio
import json
import queue
import threading
import os
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import requests
from confluent_kafka.admin import AdminClient, NewTopic

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

KSQL_URL = os.getenv('KSQL_URL', 'http://ksqldb-server:8088/query')
BOOTSTRAP = os.getenv('BOOTSTRAP_SERVERS', 'kafka-kraft:9092')

# Admin client for topics
admin = AdminClient({'bootstrap.servers': BOOTSTRAP})

# Simple websocket manager
clients = set()
ksql_queue = queue.Queue()

def ksql_reader(q):
    payload = {"ksql": "SELECT * FROM new_template_stream EMIT CHANGES;", "streamsProperties": {"ksql.streams.auto.offset.reset":"earliest"}}
    try:
        with requests.post(KSQL_URL, json=payload, stream=True) as resp:
            resp.raise_for_status()
            for raw in resp.iter_lines(decode_unicode=True):
                if raw:
                    try:
                        obj = json.loads(raw)
                    except:
                        continue
                    if 'row' in obj:
                        q.put({'type':'row','row':obj['row']})
    except Exception as e:
        q.put({'type':'error','error': str(e)})

@app.on_event('startup')
async def on_start():
    thread = threading.Thread(target=ksql_reader, args=(ksql_queue,), daemon=True)
    thread.start()
    asyncio.create_task(broadcaster())

async def broadcaster():
    loop = asyncio.get_running_loop()
    while True:
        item = await loop.run_in_executor(None, ksql_queue.get)
        dead = []
        for ws in list(clients):
            try:
                await ws.send_json(item)
            except Exception:
                dead.append(ws)
        for d in dead:
            clients.remove(d)

@app.websocket('/ws')
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except Exception:
        clients.discard(ws)

# REST: list topics
@app.get('/api/topics')
async def list_topics():
    md = admin.list_topics(timeout=5)
    topics = []
    for t in md.topics.values():
        topics.append({'name': t.topic, 'partitions': len(t.partitions)})
    return JSONResponse(topics)

# REST: create topic
@app.post('/api/topics')
async def create_topic(body: dict):
    name = body.get('name')
    if not name: raise HTTPException(status_code=400, detail='name required')
    newt = NewTopic(name, num_partitions=1, replication_factor=1)
    fs = admin.create_topics([newt])
    # wait for result
    for topic, f in fs.items():
        try:
            f.result(10)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    return JSONResponse({'ok': True})

# REST: run ksql query (non-streaming simple call)
@app.post('/api/ksql')
async def run_ksql(body: dict):
    ksql = body.get('ksql')
    if not ksql: raise HTTPException(status_code=400, detail='ksql required')
    resp = requests.post(KSQL_URL, json={'ksql': ksql})
    try:
        return JSONResponse(resp.json())
    except Exception:
        return JSONResponse({'raw': resp.text})
    
@app.delete("/api/topics/{name}")
async def delete_topic(name: str):
    try:
        admin.delete_topics([name])
        return {"deleted": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

