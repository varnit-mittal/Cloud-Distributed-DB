from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import os
from controller import Controller

ETCD_HOST = os.environ.get("ETCD_HOST", "etcd.kv-system.svc.cluster.local")
ETCD_PORT = int(os.environ.get("ETCD_PORT", "2379"))
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis.kv-system.svc.cluster.local:6379/0")

ctrl = Controller(etcd_host=ETCD_HOST, etcd_port=ETCD_PORT, redis_url=REDIS_URL)

class RegisterReq(BaseModel):
    node_id: str
    http_addr: str | None = None
    grpc_addr: str | None = None
    addr: str | None = None  


class HeartbeatReq(BaseModel):
    node_id: str

@asynccontextmanager
async def lifespan(app: FastAPI):

    stop_event = asyncio.Event()

    async def monitor():
        print("Controller Monitor Started")
        while not stop_event.is_set():
            try:
                alive = ctrl.get_alive_workers()
                all_workers = list(ctrl.workers.keys())
                dead = set(all_workers) - set(alive.keys())

                if dead:
                    print("Controller detected dead workers:", dead)
                    for d in dead:
                        try:
                            ctrl.handle_worker_failure(d)
                        except Exception as e:
                            print("Error handling worker failure:", e)

                await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print("Monitor error:", e)
                await asyncio.sleep(5)

    task = asyncio.create_task(monitor())

    try:
        yield
    finally:
        stop_event.set()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan, title="Distributed KV Controller")

@app.post("/register")
async def register(req: RegisterReq):
    http_addr = req.http_addr or req.addr
    try:
        ctrl.register_worker(
            node_id=req.node_id,
            http_addr=http_addr,
            grpc_addr=req.grpc_addr
        )
        return {"ok": True, "workers": ctrl.workers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/heartbeat")
async def heartbeat(req: HeartbeatReq):
    ctrl.heartbeat(req.node_id)
    return {"ok": True}


@app.get("/mapping")
async def mapping(key: str):
    try:
        return ctrl.mapping_for_key(key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/nodes")
async def nodes():
    return {
        "workers": ctrl.workers,
        "alive": ctrl.get_alive_workers()
    }


@app.post("/rebalance")
async def rebalance(req: dict):
    node = req.get("node")
    if not node:
        raise HTTPException(status_code=400, detail="Missing 'node' field.")

    try:
        ctrl.handle_worker_failure(node)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
