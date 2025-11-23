# controller/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import httpx
from controller import Controller

# instantiate controller (shared)
ctrl = Controller()

class RegisterReq(BaseModel):
    node_id: str
    addr: str

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
        # signal monitor to stop and cancel the task
        stop_event.set()
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="KV Controller", lifespan=lifespan)

@app.post("/register")
async def register(req: RegisterReq):
    ctrl.register_worker(req.node_id, req.addr)
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
