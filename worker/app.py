# worker/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import os
import httpx
import kv_pb2, kv_pb2_grpc
import grpc
from storage import MongoStorage
from grpc_server import serve_grpc
from merkle import compute_merkle_root
import aioredis
import hashlib
from urllib.parse import urlparse

# Configuration from env
NODE_ID = os.environ.get("NODE_ID", "worker1")
CONTROLLER_ADDR = os.environ.get("CONTROLLER_ADDR", "http://controller:8000")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
MONGO_URL = os.environ.get("MONGO_URL", "mongodb://mongo:27017")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8001"))

# persistent Mongo storage (MONGO_URL expected to point to this worker's mongo)
storage = MongoStorage(mongo_uri=MONGO_URL)

class PutReq(BaseModel):
    value: str
    version: int = 1


async def replication_worker(redis_client, node_id):
    """
    Consume Redis Stream 'replicate_stream' and perform async gRPC replication.
    Expects redis_client to be an aioredis client (decode_responses=True).
    Stream messages should contain:
      - key, value, version
      - target_grpc (preferred), OR target (legacy http://host:port)
      - target_node (optional)
    """
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
                    # fields is a dict of str->str (decode_responses=True)
                    def get_field(fld):
                        return fields.get(fld) or fields.get(fld.encode() if isinstance(list(fields.keys())[0], bytes) else fld)

                    key = get_field("key")
                    value = get_field("value")
                    version_s = get_field("version")
                    target_grpc = get_field("target_grpc") or get_field("target")  # accept legacy "target"
                    target_node = get_field("target_node")

                    # Advance last_id in any case to avoid processing the same message repeatedly
                    last_id = msg_id

                    if not key or not value or not target_grpc:
                        # malformed message, skip
                        continue

                    try:
                        version = int(version_s) if version_s is not None else 1
                    except Exception:
                        version = 1

                    # If target_grpc looks like an http url, convert to host and use default GRPC_PORT
                    if target_grpc.startswith("http://") or target_grpc.startswith("https://"):
                        parsed = urlparse(target_grpc)
                        target_host = parsed.hostname
                        target_port = parsed.port or GRPC_PORT
                        target_grpc_addr = f"{target_host}:{target_port}"
                    else:
                        # target_grpc maybe "worker2:50052" or "worker2" (no port)
                        if ":" in target_grpc:
                            target_grpc_addr = target_grpc
                        else:
                            target_grpc_addr = f"{target_grpc}:{GRPC_PORT}"

                    # Skip if target is self (by node_id) or resolved host equals own NODE_ID
                    target_host_candidate = target_grpc_addr.split(":")[0]
                    if (target_node and target_node == node_id) or (target_host_candidate == node_id):
                        # nothing to do
                        continue

                    print(f"[{node_id}] Async replicating {key} -> {target_grpc_addr} (v={version})")

                    try:
                        async with grpc.aio.insecure_channel(target_grpc_addr) as chan:
                            stub = kv_pb2_grpc.ReplicatorStub(chan)
                            kv_msg = kv_pb2.KeyValue(key=key, value=value, version=version)
                            req = kv_pb2.ReplicateRequest(kv=kv_msg, from_node=node_id)
                            resp = await stub.Replicate(req, timeout=5)
                            if resp.ok:
                                print(f"[{node_id}] Async replication to {target_grpc_addr} complete.")
                            else:
                                print(f"[{node_id}] Async replication FAILED: {resp.message}")
                    except Exception as e:
                        print(f"[{node_id}] Async replication error to {target_grpc_addr}:", e)

        except Exception as e:
            # safe logging and backoff
            print("Replication worker exception:", e)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    background_tasks = []

    # start gRPC server (serves replication RPCs)
    grpc_task = loop.create_task(serve_grpc(host="0.0.0.0", port=GRPC_PORT, storage=storage))
    background_tasks.append(grpc_task)

    # Redis client for streams (async)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis_client

    # start replication worker (after redis client exists)
    repl_task = loop.create_task(replication_worker(redis_client, NODE_ID))
    background_tasks.append(repl_task)

    # register once with controller (include both http & grpc addresses)
    async def register():
        async with httpx.AsyncClient() as client:
            try:
                http_addr = f"http://{NODE_ID}:{HTTP_PORT}"
                grpc_addr = f"{NODE_ID}:{GRPC_PORT}"  # reachable inside docker network
                await client.post(
                    f"{CONTROLLER_ADDR}/register",
                    json={"node_id": NODE_ID, "http_addr": http_addr, "grpc_addr": grpc_addr}
                )
            except Exception as e:
                print("Controller register error:", e)

    await register()

    # heartbeat
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
        # close redis
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
    # write locally (Mongo) - primary
    await storage.put(key, body.value, body.version)

    # Ask controller for mapping to find replicas
    async with httpx.AsyncClient() as client:
        mapping = None
        try:
            r = await client.get(f"{CONTROLLER_ADDR}/mapping", params={"key": key})
            mapping = r.json()
        except Exception as e:
            print("Failed to fetch mapping from controller:", e)
            mapping = None

    if mapping:
        # controller may return nodes as list of dicts or list of http-url strings
        nodes = mapping.get("nodes", []) if isinstance(mapping, dict) else mapping or []
        # normalize nodes into dicts with node_id/http/grpc when possible
        normalized = []
        for n in nodes:
            if isinstance(n, dict):
                normalized.append(n)
            elif isinstance(n, str):
                # string could be "http://worker2:8001" or "worker2"
                if n.startswith("http://") or n.startswith("https://"):
                    parsed = urlparse(n)
                    node_id = parsed.hostname
                    http_addr = n
                    grpc_addr = f"{node_id}:{GRPC_PORT}"
                    normalized.append({"node_id": node_id, "http": http_addr, "grpc": grpc_addr})
                else:
                    # plain hostname or "host:port"
                    if ":" in n:
                        host, port = n.split(":", 1)
                        normalized.append({"node_id": host, "http": f"http://{host}:{HTTP_PORT}", "grpc": n})
                    else:
                        normalized.append({"node_id": n, "http": f"http://{n}:{HTTP_PORT}", "grpc": f"{n}:{GRPC_PORT}"})
            else:
                # unknown format - skip
                continue

        replica1 = normalized[1] if len(normalized) > 1 else None
        replica2 = normalized[2] if len(normalized) > 2 else None

        # synchronous replicate to replica1 via gRPC (if not self)
        if replica1 and replica1.get("node_id") != NODE_ID:
            target_grpc = replica1.get("grpc")
            if target_grpc:
                try:
                    async with grpc.aio.insecure_channel(target_grpc) as chan:
                        stub = kv_pb2_grpc.ReplicatorStub(chan)
                        kv_msg = kv_pb2.KeyValue(key=key, value=body.value, version=body.version)
                        req = kv_pb2.ReplicateRequest(kv=kv_msg, from_node=NODE_ID)
                        resp = await stub.Replicate(req, timeout=5)
                        if not resp.ok:
                            print("Replica1 failed:", resp.message)
                except Exception as e:
                    print("gRPC replicate error to replica1:", e)
            else:
                print("Replica1 missing grpc addr, skipping sync replication.")
        else:
            # replica1 is missing or is self
            pass

        # push background replication job to Redis Stream for replica2 (if not self)
        if replica2 and replica2.get("node_id") != NODE_ID:
            rcli = app.state.redis
            try:
                await rcli.xadd(
                    "replicate_stream",
                    {
                        "key": key,
                        "value": body.value,
                        "version": str(body.version),
                        "target_grpc": replica2.get("grpc"),
                        "target_node": replica2.get("node_id"),
                    },
                )
            except Exception as e:
                print("failed to push replicate job:", e)
        else:
            # replica2 missing or self
            pass

    return {"ok": True}
