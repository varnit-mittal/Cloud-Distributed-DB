import hashlib
import time
import json
import uuid
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, List

from etcd_client import EtcdClient
import redis


class Controller:

    def __init__(
        self,
        etcd_host: str = "127.0.0.1",
        etcd_port: int = 2379,
        redis_url: str = "redis://redis:6379/0",
        bucket_count: int = 16,
    ):
        self.etcd = EtcdClient(host=etcd_host, port=etcd_port)
        raw = self.etcd.load_workers() or {}
        self.workers = self.NormalizeWorkers(raw)  
        self.last_heartbeat: Dict[str, float] = {}
        self.bucket_count = bucket_count

        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)

    def NormalizeWorkers(self, raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        out = {}
        for nid, v in raw.items():
            if isinstance(v, dict):
                out[nid] = {"http": v.get("http"), "grpc": v.get("grpc"), "status": "alive"}
            elif isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    out[nid] = {"http": parsed.get("http"), "grpc": parsed.get("grpc"), "status": "alive"}
                except Exception:
                    out[nid] = {"http": v, "grpc": None, "status": "alive"}
        return out

    def refresh_workers(self):
        raw = self.etcd.load_workers() or {}
        self.workers = self.NormalizeWorkers(raw)

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
        if node_id in self.workers:
            self.workers[node_id]["status"] = "alive"

    def getAliveWorkers(self, threshold: float = 10.0) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        alive = {}
        for nid, meta in self.workers.items():
            ts = self.last_heartbeat.get(nid, 0)
            if now - ts < threshold and meta.get("status") != "dead":
                alive[nid] = meta
        return alive

    def partition_for_key(self, key: str) -> int:
        if not self.workers:
            raise RuntimeError("No workers registered")
        n = len(self.workers)
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return h % n
    
    def NodeSelector(self, bucket_id: int, alive_nodes: List[str]) -> List[str]:
    
        scores = []
        for node_id in alive_nodes:
            raw_key = f"{bucket_id}_{node_id}"
            score = hashlib.sha256(raw_key.encode()).hexdigest()
            scores.append((score, node_id))
        
        scores.sort(key=lambda x: x[0], reverse=True)
        
        return [s[1] for s in scores]

    def mapping_for_key(self, key: str) -> Dict[str, Any]:
        alive_nodes = list(self.getAliveWorkers().keys())
        if not alive_nodes:
            raise HTTPException(status_code=503, detail="No alive workers")

        bucket_id = int(hashlib.sha256(key.encode()).hexdigest(), 16) % self.bucket_count
        ranked_nodes = self.NodeSelector(bucket_id, alive_nodes)
        selected_ids = ranked_nodes[:3]
        
        selected_meta = []
        for nid in selected_ids:
            meta = self.workers.get(nid, {})
            selected_meta.append({
                "node_id": nid,
                "http": meta.get("http"),
                "grpc": meta.get("grpc")
            })

        return {
            "primary": selected_meta[0] if selected_meta else None,
            "nodes": selected_meta,
            "node_ids": selected_ids,
            "debug_bucket": bucket_id
        }

    # -----------------------------
    # Failure handling + re-replication
    # -----------------------------
    def handle_worker_failure(self, dead_node_id: str):
        print(f"Handling failure for node {dead_node_id}")
        
        # Mark dead
        if dead_node_id in self.workers:
            self.workers[dead_node_id]["status"] = "dead"
            try: self.etcd.save_workers(self.workers)
            except: pass

        alive_nodes = list(self.getAliveWorkers().keys())
        if not alive_nodes: return

        # "Previous" Universe: Alive nodes + the node that just died
        previous_universe = alive_nodes + [dead_node_id]

        for bucket in range(self.bucket_count):
            # 1. Who WAS handling this bucket? (Top 3 from previous universe)
            # This logic mimics what mapping_for_key returned BEFORE the death.
            prev_ranked = self.NodeSelector(bucket, previous_universe)
            old_replicas = prev_ranked[:3]

            # 2. Who SHOULD handle it now? (Top 3 from current/alive universe)
            new_ranked = self.NodeSelector(bucket, alive_nodes)
            new_replicas = new_ranked[:3]

            # 3. CHECK: Was the dead node actually involved in this bucket?
            if dead_node_id not in old_replicas:
                # CRITICAL FIX:
                # If the dead node was NOT in the top 3 for this bucket,
                # then removing it changes nothing for the top 3 of the remaining nodes.
                # We skip this bucket entirely.
                continue

            # 4. If we are here, the dead node WAS a replica/primary. 
            # We need to copy data to the new node that entered the Top 3.
            
            # Find the new node: It's in new_replicas but wasn't in old_replicas
            targets = [n for n in new_replicas if n not in old_replicas]
            
            # Find a source: Any surviving node from the old set
            potential_sources = [n for n in old_replicas if n != dead_node_id]
            if not potential_sources: continue
            source_node = potential_sources[0]

            for target_node in targets:
                if target_node == source_node: continue
                
                # Enqueue Job
                job = {
                    "job_id": str(uuid.uuid4()),
                    "type": "bucket_copy",
                    "bucket_id": str(bucket),
                    "source_node": source_node,
                    "source_grpc": (self.workers.get(source_node) or {}).get("grpc"),
                    "target_node": target_node,
                    "target_grpc": (self.workers.get(target_node) or {}).get("grpc"),
                    "created_at": str(time.time()),
                }
                try:
                    self.redis.xadd("repl_jobs", job)
                    print(f"Repair Bucket {bucket}: {source_node} -> {target_node}")
                except Exception as e:
                    print("Redis enqueue failed:", e)
