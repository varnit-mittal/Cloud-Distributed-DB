# worker/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio, os
import httpx
import kv_pb2, kv_pb2_grpc
import grpc
from storage import MongoStorage
from grpc_server import serve_grpc
from merkle import compute_merkle_root
import aioredis
from urllib.parse import urlparse

NODE_ID = os.environ.get("NODE_ID", None) or os.getenv("HOSTNAME","worker1")  # in k8s pod hostname used
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
            messages = await redis_client.xread({STREAM_KEY: last_id}, block=5000, count=1)
            if not messages:
                await asyncio.sleep(0.05)
                continue
            for stream_name, msgs in messages:
                for msg_id, fields in msgs:
                    def get_field(fld):
                        return fields.get(fld) or fields.get(fld.encode())
                    key = get_field("key")
                    value = get_field("value")
                    version_s = get_field("version")
                    target_grpc = get_field("target_grpc") or get_field("target")
                    target_node = get_field("target_node")
                    last_id = msg_id
                    if not key or not value or not target_grpc:
                        continue
                    try:
                        version = int(version_s) if version_s is not None else 1
                    except Exception:
                        version = 1
                    # normalize
                    if target_grpc.startswith("http://") or target_grpc.startswith("https://"):
                        parsed = urlparse(target_grpc)
                        default_grpc_port = GRPC_PORT
                        addr = f"{parsed.hostname}:{parsed.port or default_grpc_port}"
                    else:
                        addr = target_grpc if ":" in target_grpc else f"{target_grpc}:{GRPC_PORT}"
                    if (target_node and target_node == node_id) or addr.split(":")[0] == node_id:
                        continue
                    print(f"[{node_id}] Async replicating {key} -> {addr} (v={version})")
                    try:
                        async with grpc.aio.insecure_channel(addr) as chan:
                            stub = kv_pb2_grpc.ReplicatorStub(chan)
                            kv_msg = kv_pb2.KeyValue(key=key, value=value, version=version)
                            req = kv_pb2.ReplicateRequest(kv=kv_msg, from_node=node_id)
                            resp = await stub.Replicate(req, timeout=5)
                            if resp.ok:
                                print(f"[{node_id}] Async replication to {addr} complete.")
                            else:
                                print(f"[{node_id}] Async replication FAILED: {resp.message}")
                    except Exception as e:
                        print(f"[{node_id}] Async replication error to {addr}:", e)
        except Exception as e:
            print("Replication worker exception:", e)
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    background_tasks=[]

    # start gRPC server
    grpc_task = loop.create_task(serve_grpc(host="0.0.0.0", port=GRPC_PORT, storage=storage, node_id=NODE_ID))
    background_tasks.append(grpc_task)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis_client

    repl_task = loop.create_task(replication_worker(redis_client, NODE_ID))
    background_tasks.append(repl_task)

    async def register():
        async with httpx.AsyncClient() as client:
            http_addr = f"http://{NODE_ID}:{HTTP_PORT}"
            grpc_addr = f"{NODE_ID}:{GRPC_PORT}"
            try:
                await client.post(f"{CONTROLLER_ADDR}/register", json={"node_id": NODE_ID, "http_addr": http_addr, "grpc_addr": grpc_addr})
            except Exception as e:
                print("Controller register error:", e)
    await register()

    async def heartbeat_loop():
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    await client.post(f"{CONTROLLER_ADDR}/heartbeat", json={"node_id": NODE_ID})
                except Exception:
                    pass
                await asyncio.sleep(2)
    hb_task = loop.create_task(heartbeat_loop())
    background_tasks.append(hb_task)

    try:
        yield
    finally:
        for t in background_tasks:
            t.cancel()
        for t in background_tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        try:
            await redis_client.close()
        except Exception:
            pass

app = FastAPI(lifespan=lifespan)

@app.get("/kv/{key}")
async def get_kv(key: str):
    v = await storage.get(key)
    if not v:
        raise HTTPException(status_code=404, detail="not found")
    return v

@app.put("/kv/{key}")
async def put_kv(key: str, body: PutReq):
    # primary write
    await storage.put(key, body.value, body.version)
    # mapping
    async with httpx.AsyncClient() as client:
        mapping = None
        try:
            r = await client.get(f"{CONTROLLER_ADDR}/mapping", params={"key": key})
            mapping = r.json()
        except Exception as e:
            print("Failed mapping:", e)
            mapping = None
    if mapping:
        nodes = mapping.get("nodes", [])
        # normalize to dicts (if external)
        normalized=[]
        from urllib.parse import urlparse
        for n in nodes:
            if isinstance(n, dict):
                normalized.append(n)
            elif isinstance(n, str):
                if n.startswith("http"):
                    p = urlparse(n)
                    normalized.append({"node_id": p.hostname, "http": n, "grpc": f"{p.hostname}:{GRPC_PORT}"})
                else:
                    if ":" in n:
                        h,port = n.split(":",1)
                        normalized.append({"node_id": h, "http": f"http://{h}:{HTTP_PORT}", "grpc": n})
                    else:
                        normalized.append({"node_id": n, "http": f"http://{n}:{HTTP_PORT}", "grpc": f"{n}:{GRPC_PORT}"})
        replica1 = normalized[1] if len(normalized)>1 else None
        replica2 = normalized[2] if len(normalized)>2 else None

        # sync replicate to replica1 if not self
        if replica1 and replica1.get("node_id") != NODE_ID:
            target = replica1.get("grpc")
            try:
                async with grpc.aio.insecure_channel(target) as chan:
                    stub = kv_pb2_grpc.ReplicatorStub(chan)
                    kv_msg = kv_pb2.KeyValue(key=key, value=body.value, version=body.version)
                    req = kv_pb2.ReplicateRequest(kv=kv_msg, from_node=NODE_ID)
                    resp = await stub.Replicate(req, timeout=5)

                    # If replica1 fails → PUT MUST FAIL (per assignment)
                    if not resp.ok:
                        raise HTTPException(
                            status_code=500,
                            detail=f"PUT failed: synchronous replica {replica1['node_id']} rejected write"
                        )

            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"PUT failed: could not write to replica1 ({replica1['node_id']}): {e}"
                )

        if replica2 and replica2.get("node_id") != NODE_ID:
            rcli = app.state.redis
            try:
                await rcli.xadd("replicate_stream", {
                    "key": key, "value": body.value, "version": str(body.version),
                    "target_grpc": replica2.get("grpc"),
                    "target_node": replica2.get("node_id")
                })
            except Exception as e:
                print("failed to push replicate job:", e)
    return {"ok": True}
