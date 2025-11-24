# controller/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from controller import Controller
from typing import Optional

# instantiate controller (shared)
ctrl = Controller()

class RegisterReq(BaseModel):
    node_id: str
    # new preferred fields:
    http_addr: Optional[str] = None
    grpc_addr: Optional[str] = None
    # legacy field (for backward compatibility)
    addr: Optional[str] = None

class HeartbeatReq(BaseModel):
    node_id: str

# Lifespan handler: starts a background monitor task and cancels it on shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()

    async def monitor():
        try:
            while not stop_event.is_set():
                try:
                    alive = ctrl.get_alive_workers()
                    dead = set(ctrl.workers.keys()) - set(alive.keys())
                    if dead:
                        # you can replace print with structured logging
                        print("Controller detected dead nodes:", dead)
                        # TODO: trigger re-replication or other recovery here
                except Exception as e:
                    # ensure monitor doesn't die silently
                    print("Controller monitor error:", e)
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            # graceful cancellation
            pass

    monitor_task = asyncio.create_task(monitor())

    try:
        yield
    finally:
        stop_event.set()
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="KV Controller", lifespan=lifespan)

@app.post("/register")
async def register(req: RegisterReq):
    """
    Accepts either:
      - { node_id, http_addr, grpc_addr }
    or (legacy)
      - { node_id, addr }  where addr is an http url like "http://worker2:8001"
    """
    # prefer new fields, fallback to legacy addr
    http_addr = req.http_addr or req.addr
    grpc_addr = req.grpc_addr

    # if legacy addr provided and grpc_addr missing, derive host and default grpc port 50051
    if http_addr and not grpc_addr:
        # parse host from http_addr
        try:
            from urllib.parse import urlparse
            p = urlparse(http_addr)
            host = p.hostname or http_addr
            grpc_addr = f"{host}:50051"
        except Exception:
            grpc_addr = None

    if not http_addr and not grpc_addr:
        # invalid payload
        raise HTTPException(status_code=400, detail="provide http_addr and/or grpc_addr (or legacy addr)")

    ctrl.register_worker(req.node_id, http_addr=http_addr, grpc_addr=grpc_addr)
    return {"ok": True, "workers": ctrl.workers}

@app.post("/heartbeat")
async def heartbeat(req: HeartbeatReq):
    ctrl.heartbeat(req.node_id)
    return {"ok": True}

@app.get("/mapping")
async def mapping(key: str):
    try:
        m = ctrl.mapping_for_key(key)
        return m
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/nodes")
async def nodes():
    return {"workers": ctrl.workers, "alive": ctrl.get_alive_workers()}
