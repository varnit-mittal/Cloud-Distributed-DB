# worker/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import asyncio, os
import httpx
import kv_pb2, kv_pb2_grpc
import grpc
from storage import RedisStorage
from grpc_server import serve_grpc
from merkle import compute_merkle_root
import json

app = FastAPI()
NODE_ID = os.environ.get("NODE_ID", "worker1")
CONTROLLER_ADDR = os.environ.get("CONTROLLER_ADDR", "http://controller:8000")  # in docker-compose use service name
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))

storage = RedisStorage(url=REDIS_URL)

class PutReq(BaseModel):
    value: str
    version: int = 1

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan handler: registers with controller, starts gRPC server and heartbeat loop,
    and ensures clean cancellation on shutdown.
    """
    loop = asyncio.get_event_loop()
    background_tasks = []

    async def register_with_controller():
        async with httpx.AsyncClient() as client:
            try:
                await client.post(f"{CONTROLLER_ADDR}/register", json={"node_id": NODE_ID, "addr": f"http://{NODE_ID}:8001"})
            except Exception as e:
                print("Controller register error:", e)

    # run registration once (don't block startup if it fails)
    await register_with_controller()

    # start gRPC server as a background task
    grpc_task = loop.create_task(serve_grpc(host="0.0.0.0", port=GRPC_PORT, storage=storage))
    background_tasks.append(grpc_task)

    # heartbeat background task
    async def heartbeat_loop():
        async with httpx.AsyncClient() as client:
            while True:
                try:
                    await client.post(f"{CONTROLLER_ADDR}/heartbeat", json={"node_id": NODE_ID})
                except Exception:
                    # swallow network/controller errors; controller may not be ready yet
                    pass
                await asyncio.sleep(2)

    hb_task = loop.create_task(heartbeat_loop())
    background_tasks.append(hb_task)

    try:
        yield
    finally:
        # cancel background tasks cleanly
        for t in background_tasks:
            t.cancel()
        for t in background_tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as e:
                # log unexpected errors during shutdown
                print("Error while shutting down background task:", e)

app = FastAPI(lifespan=lifespan)

@app.get("/kv/{key}")
async def get_kv(key: str):
    v = await storage.get(key)
    if not v:
        raise HTTPException(status_code=404, detail="not found")
    return v

@app.put("/kv/{key}")
async def put_kv(key: str, body: PutReq):
    # write locally
    await storage.put(key, body.value, body.version)
    # Ask controller for mapping to find replicas
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{CONTROLLER_ADDR}/mapping", params={"key": key})
            mapping = r.json()
        except Exception as e:
            mapping = None
    # replicate synchronously to one replica (replica1)
    if mapping:
        nodes = mapping.get("nodes", [])
        # first node is primary (this), second is replica1, third is replica2
        replica1 = nodes[1] if len(nodes) > 1 else None
        replica2 = nodes[2] if len(nodes) > 2 else None
        # call replica1 via gRPC
        if replica1:
            target_host = replica1.replace("http://", "").split(":")[0]
            target_port = 50051
            try:
                async with grpc.aio.insecure_channel(f"{target_host}:{target_port}") as chan:
                    stub = kv_pb2_grpc.ReplicatorStub(chan)
                    kv_msg = kv_pb2.KeyValue(key=key, value=body.value, version=body.version)
                    req = kv_pb2.ReplicateRequest(kv=kv_msg, from_node=NODE_ID)
                    resp = await stub.Replicate(req, timeout=2)
                    if not resp.ok:
                        print("Replica1 failed:", resp.message)
            except Exception as e:
                print("gRPC replicate error:", e)
        # push background replication job to Redis Stream to replicate to replica2
        if replica2:
            # push job to a stream key "replicate_stream"
            rcli = storage.client
            await rcli.xadd("replicate_stream", {"key": key, "value": body.value, "version": str(body.version), "target": replica2})
    return {"ok": True}
