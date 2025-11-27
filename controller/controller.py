# controller/controller.py
import hashlib, time, json
from typing import Dict, Any
from etcd_client import EtcdClient
import redis
import uuid

class Controller:
    def __init__(self, etcd_host="127.0.0.1", etcd_port=2379, redis_url="redis://redis:6379/0"):
        self.etcd = EtcdClient(host=etcd_host, port=etcd_port)
        raw = self.etcd.load_workers() or {}
        self.workers = self._normalize_loaded_workers(raw)
        self.last_heartbeat = {}
        # Redis sync for re-replication job queue
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)

    def _normalize_loaded_workers(self, raw):
        out={}
        for nid, v in raw.items():
            if isinstance(v, dict):
                out[nid] = {"http": v.get("http"), "grpc": v.get("grpc")}
            elif isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    out[nid] = {"http": parsed.get("http"), "grpc": parsed.get("grpc")}
                except Exception:
                    out[nid] = {"http": v, "grpc": None}
        return out

    def refresh_workers(self):
        raw = self.etcd.load_workers() or {}
        self.workers = self._normalize_loaded_workers(raw)

    def register_worker(self, node_id: str, http_addr: str = None, grpc_addr: str = None):
        existing = self.workers.get(node_id, {})
        http = http_addr or existing.get("http")
        grpc = grpc_addr or existing.get("grpc")
        self.workers[node_id] = {"http": http, "grpc": grpc, "status": "alive"}
        try:
            self.etcd.save_workers(self.workers)
        except Exception as e:
            print("etcd save failed:", e)
        self.last_heartbeat[node_id] = time.time()

    def heartbeat(self, node_id: str):
        self.last_heartbeat[node_id] = time.time()
        # ensure status
        if node_id in self.workers:
            self.workers[node_id]["status"] = "alive"

    def get_alive_workers(self, threshold=10):
        now = time.time()
        alive={}
        for n, meta in self.workers.items():
            ts = self.last_heartbeat.get(n, 0)
            if now - ts < threshold:
                alive[n] = meta
        return alive

    def partition_for_key(self, key: str) -> int:
        if not self.workers:
            raise RuntimeError("No workers registered")
        n = len(self.workers)
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return h % n

    def mapping_for_key(self, key: str):
        if not self.workers:
            raise RuntimeError("No workers registered")
        node_items = sorted(self.workers.items(), key=lambda x: x[0])
        node_ids = [nid for nid,_ in node_items]
        N = len(node_ids)
        if N == 0:
            raise RuntimeError("no workers")
        idx = int(hashlib.sha256(key.encode()).hexdigest(), 16) % N
        selected=[]
        i=idx
        while len(selected) < 3 and len(selected) < N:
            candidate = node_ids[i % N]
            # include only alive nodes
            if candidate in self.get_alive_workers():
                meta = self.workers.get(candidate)
                selected.append({"node_id": candidate, "http": meta.get("http"), "grpc": meta.get("grpc")})
            i += 1
            if i - idx > N*2:
                break
        primary = selected[0] if selected else None
        return {"primary": primary, "nodes": selected, "node_ids": [s["node_id"] for s in selected]}

    # failure handler
    def handle_worker_failure(self, dead_node_id):
        # mark dead
        if dead_node_id in self.workers:
            self.workers[dead_node_id]["status"] = "dead"
        # Recompute and enqueue re-replication jobs for affected buckets
        # For demo: reassign by scanning a simple set of bucket ids [0..15]; in real system we map keys->buckets
        bucket_count = 16
        # choose replacement nodes for affected buckets:
        alive = list(self.get_alive_workers().keys())
        if not alive:
            print("No alive nodes for re-replication")
            return
        for bucket in range(bucket_count):
            # pick source = one alive node that has this bucket (best-effort: choose first alive)
            source = alive[0]
            # choose target = first alive not equal source and not the dead node
            candidates = [n for n in alive if n != source]
            if not candidates:
                continue
            target = candidates[0]
            job = {
                "job_id": str(uuid.uuid4()),
                "type": "bucket_copy",
                "bucket_id": bucket,
                "source_node": source,
                "source_grpc": self.workers[source]["grpc"],
                "target_node": target,
                "target_grpc": self.workers[target]["grpc"]
            }
            # push to redis stream
            try:
                self.redis.xadd("repl_jobs", job)
            except Exception as e:
                print("Failed to enqueue re-replication job:", e)
