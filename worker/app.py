from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio, os
import httpx
import kv_pb2, kv_pb2_grpc
import grpc
from storage import MongoStorage
from grpc_server import serve_grpc
import aioredis
from urllib.parse import urlparse

NODE_ID = os.environ.get("NODE_ID") or os.getenv("HOSTNAME", "worker")
CONTROLLER_ADDR = os.environ.get("CONTROLLER_ADDR", "http://controller:8000")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://mongo:27017")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8001"))

storage = MongoStorage(mongo_uri=MONGO_URL)

class PutReq(BaseModel):
    value: str
    version: int = 1

async def replication_worker(redis_client, node_id):
    STREAM_KEY = "replicate_stream"
    last_id = "0-0"

    while True:
        try:
            msgs = await redis_client.xread({STREAM_KEY: last_id}, block=5000, count=1)
            if not msgs:
                await asyncio.sleep(0.05)
                continue

            for stream, items in msgs:
                for msg_id, fields in items:
                    last_id = msg_id

                    key = fields.get("key")
                    value = fields.get("value")
                    version = int(fields.get("version", 1))
                    target_grpc = fields.get("target_grpc")
                    target_node = fields.get("target_node")

                    if not key or not value or not target_grpc:
                        continue

                    host = target_grpc.split(":")[0]
                    if host == node_id:
                        continue

                    print(f"[{node_id}] Async replicating {key} → {target_grpc}")

                    try:
                        async with grpc.aio.insecure_channel(target_grpc) as chan:
                            stub = kv_pb2_grpc.ReplicatorStub(chan)
                            msg = kv_pb2.KeyValue(key=key, value=value, version=version)
                            req = kv_pb2.ReplicateRequest(kv=msg, from_node=node_id)
                            resp = await stub.Replicate(req, timeout=5)
                            if resp.ok:
                                print(f"[{node_id}] Async → COMPLETE")
                            else:
                                print(f"[{node_id}] Async → FAILED: {resp.message}")
                    except Exception as e:
                        print(f"[{node_id}] Async replicate error:", e)

        except Exception as e:
            print("Replication worker exception:", e)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    tasks = []

    grpc_task = loop.create_task(
        serve_grpc("0.0.0.0", GRPC_PORT, storage, NODE_ID)
    )
    tasks.append(grpc_task)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis_client

    repl_task = loop.create_task(replication_worker(redis_client, NODE_ID))
    tasks.append(repl_task)

    async def register():
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    http_addr = f"http://{NODE_ID}:{HTTP_PORT}"
                    grpc_addr = f"{NODE_ID}:{GRPC_PORT}"
                    await client.post(f"{CONTROLLER_ADDR}/register",
                                      json={"node_id": NODE_ID,
                                            "http_addr": http_addr,
                                            "grpc_addr": grpc_addr})
                    print(f"[{NODE_ID}] Registered with controller")
                    break
                except Exception as e:
                    print(f"[{NODE_ID}] Register failed → retrying:", e)
                    await asyncio.sleep(2)

    await register()

    async def heartbeat():
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    await client.post(f"{CONTROLLER_ADDR}/heartbeat",
                                      json={"node_id": NODE_ID})
                except Exception:
                    pass
                await asyncio.sleep(2)

    hb_task = loop.create_task(heartbeat())
    tasks.append(hb_task)

    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

        await redis_client.close()


app = FastAPI(lifespan=lifespan)

@app.get("/kv/{key}")
async def get_kv(key: str):
    doc = await storage.get(key)
    if not doc:
        raise HTTPException(status_code=404, detail="not found")
    return doc


@app.put("/kv/{key}")
async def put_kv(key: str, body: PutReq):
    await storage.put(key, body.value, body.version)

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{CONTROLLER_ADDR}/mapping", params={"key": key})
            mapping = resp.json()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"mapping failed: {e}")

    nodes = mapping.get("nodes", [])
    if not nodes:
        return {"ok": True}

    # normalize
    replicas = []
    for n in nodes:
        replicas.append({
            "node_id": n["node_id"],
            "grpc": n.get("grpc")
        })

    if len(replicas) > 1:
        r1 = replicas[1]
        if r1["node_id"] != NODE_ID:
            try:
                async with grpc.aio.insecure_channel(r1["grpc"]) as chan:
                    stub = kv_pb2_grpc.ReplicatorStub(chan)
                    req = kv_pb2.ReplicateRequest(
                        kv=kv_pb2.KeyValue(key=key, value=body.value, version=body.version),
                        from_node=NODE_ID
                    )
                    resp = await stub.Replicate(req, timeout=5)
                    if not resp.ok:
                        raise HTTPException(500, f"Synchronous replica {r1['node_id']} failed: {resp.message}")
            except Exception as e:
                raise HTTPException(500, f"Sync replication failed to {r1['node_id']}: {e}")

    if len(replicas) > 2:
        r2 = replicas[2]
        if r2["node_id"] != NODE_ID:
            await app.state.redis.xadd(
                "replicate_stream",
                {
                    "key": key,
                    "value": body.value,
                    "version": str(body.version),
                    "target_grpc": r2["grpc"],
                    "target_node": r2["node_id"]
                }
            )

    return {"ok": True}
