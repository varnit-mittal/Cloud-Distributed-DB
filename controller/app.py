# controller/app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from controller import Controller

ctrl = Controller()

class RegisterReq(BaseModel):
    node_id: str
    http_addr: str = None
    grpc_addr: str = None
    addr: str = None  # legacy

class HeartbeatReq(BaseModel):
    node_id: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    async def monitor():
        while not stop_event.is_set():
            try:
                alive = ctrl.get_alive_workers()
                dead = set(ctrl.workers.keys()) - set(alive.keys())
                if dead:
                    print("Controller detected dead nodes:", dead)
                    for d in dead:
                        ctrl.handle_worker_failure(d)
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print("monitor error:", e)
                await asyncio.sleep(5)
    t = asyncio.create_task(monitor())
    try:
        yield
    finally:
        stop_event.set()
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)

@app.post("/register")
async def register(req: RegisterReq):
    http_addr = req.http_addr or req.addr
    ctrl.register_worker(req.node_id, http_addr=http_addr, grpc_addr=req.grpc_addr)
    return {"ok": True, "workers": ctrl.workers}

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
    return {"workers": ctrl.workers, "alive": ctrl.get_alive_workers()}

@app.post("/rebalance")
async def rebalance(req: dict):
    node = req.get("node")
    action = req.get("action", "recover")
    try:
        ctrl.handle_worker_failure(node)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

