# controller/controller.py
import hashlib
import time
import json
import uuid
from typing import Dict, Any, List

# these are the libraries you're already using elsewhere
from etcd_client import EtcdClient
import redis


class Controller:
    """
    Controller that keeps track of worker metadata (http/grpc addrs + status),
    computes mapping for keys (hash mod N) and, on worker failure, promotes the
    next active replica as primary and enqueues async jobs to create the 3rd replica.
    """

    def __init__(
        self,
        etcd_host: str = "127.0.0.1",
        etcd_port: int = 2379,
        redis_url: str = "redis://redis:6379/0",
        bucket_count: int = 16,
    ):
        self.etcd = EtcdClient(host=etcd_host, port=etcd_port)
        raw = self.etcd.load_workers() or {}
        self.workers = self._normalize_loaded_workers(raw)  # node_id -> {"http":..,"grpc":..,"status":..}
        self.last_heartbeat: Dict[str, float] = {}
        self.bucket_count = bucket_count

        # Redis stream queue for re-replication jobs
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)

    def _normalize_loaded_workers(self, raw: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
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
        self.workers = self._normalize_loaded_workers(raw)

    def register_worker(self, node_id: str, http_addr: str = None, grpc_addr: str = None):
        existing = self.workers.get(node_id, {})
        http = http_addr or existing.get("http")
        grpc = grpc_addr or existing.get("grpc")
        self.workers[node_id] = {"http": http, "grpc": grpc, "status": "alive"}
        try:
            # persist to etcd as JSON (so other controllers see same format)
            self.etcd.save_workers(self.workers)
        except Exception as e:
            print("etcd save failed:", e)
        self.last_heartbeat[node_id] = time.time()

    def heartbeat(self, node_id: str):
        self.last_heartbeat[node_id] = time.time()
        if node_id in self.workers:
            self.workers[node_id]["status"] = "alive"

    def get_alive_workers(self, threshold: float = 10.0) -> Dict[str, Dict[str, Any]]:
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

    def mapping_for_key(self, key: str) -> Dict[str, Any]:
        """
        Return primary and replica infos for the key while skipping dead nodes.
        Deterministic ordering: sort node ids lexicographically.
        """
        if not self.workers:
            raise RuntimeError("No workers registered")
        node_items = sorted(self.workers.items(), key=lambda x: x[0])  # (node_id, meta)
        node_ids = [nid for nid, _ in node_items]
        # Use only alive nodes for assignment (so mapping matches current cluster)
        alive_ids = [nid for nid in node_ids if nid in self.get_alive_workers()]
        if not alive_ids:
            raise RuntimeError("no alive workers")
        idx = int(hashlib.sha256(key.encode()).hexdigest(), 16) % len(alive_ids)
        selected = []
        i = idx
        # pick up to 3 alive nodes (wrap-around)
        while len(selected) < 3 and len(selected) < len(alive_ids):
            cand = alive_ids[i % len(alive_ids)]
            meta = self.workers.get(cand)
            selected.append({"node_id": cand, "http": meta.get("http"), "grpc": meta.get("grpc")})
            i += 1
        primary = selected[0] if selected else None
        return {"primary": primary, "nodes": selected, "node_ids": [s["node_id"] for s in selected]}

    # -----------------------------
    # Failure handling + re-replication
    # -----------------------------
    def handle_worker_failure(self, dead_node_id: str):
        """
        Mark dead_node as dead, compute new desired replica sets per bucket,
        and enqueue re-replication jobs to create a 3rd replica on an unused active worker.
        The job payload is pushed to Redis Stream 'repl_jobs'. Worker(s) consuming this stream
        will perform copy from source_grpc -> target_grpc.
        """
        print(f"Handling failure for node {dead_node_id}")
        if dead_node_id not in self.workers:
            print("Unknown node:", dead_node_id)
            return

        # Mark the node as dead locally and persist
        self.workers[dead_node_id]["status"] = "dead"
        try:
            self.etcd.save_workers(self.workers)
        except Exception as e:
            print("etcd save failed:", e)

        alive_nodes = list(self.get_alive_workers().keys())
        if not alive_nodes:
            print("No alive nodes available; cannot re-replicate now.")
            return

        # We'll compute per-bucket desired replicas using alive_nodes (deterministic order)
        alive_nodes_sorted = sorted(alive_nodes)

        bucket_count = self.bucket_count

        for bucket in range(bucket_count):
            # compute deterministic index using bucket id so mapping remains stable
            idx = int(hashlib.sha256(str(bucket).encode()).hexdigest(), 16) % len(alive_nodes_sorted)
            selected = []
            i = idx
            while len(selected) < 3 and len(selected) < len(alive_nodes_sorted):
                cand = alive_nodes_sorted[i % len(alive_nodes_sorted)]
                if cand not in selected:
                    selected.append(cand)
                i += 1

            # If the old dead node would have been in this bucket's desired set before failure,
            # we should recreate the third replica (because copy count dropped).
            # To decide that, check the previous desired set including dead node (fallback)
            # For simplicity here we assume dead node was indeed responsible for some buckets.
            # We'll enqueue job if dead_node_id is NOT in the selected set (i.e., reduced set) OR
            # if previously dead node was part of desired set. To be conservative, enqueue for all buckets.
            # But to avoid unnecessary work, we require there to be at least one candidate target not in selected.

            # choose source: pick first member of selected (the new primary) or second if you want
            # prefer using an existing replica as source (selected[0] or selected[1] if exists)
            if not selected:
                continue

            new_primary = selected[0]
            current_replicas = set(selected)

            # pick a target: an alive node not already in current_replicas and not the dead node
            candidates_for_target = [n for n in alive_nodes_sorted if n not in current_replicas and n != dead_node_id]
            if not candidates_for_target:
                # no spare node to place third replica (cluster too small)
                continue
            target_node = candidates_for_target[0]

            # pick a source node to copy from: prefer a node that is in current_replicas (like selected[0] or selected[1])
            # Use selected[0] as source (new primary). If new primary equals target then skip.
            source_node = None
            for cand in selected:
                if cand != target_node:
                    source_node = cand
                    break
            if source_node is None:
                continue

            # prepare job payload (string fields only)
            job = {
                "job_id": str(uuid.uuid4()),
                "type": "bucket_copy",
                "bucket_id": str(bucket),
                "source_node": source_node,
                "source_grpc": (self.workers.get(source_node) or {}).get("grpc"),
                "target_node": target_node,
                "target_grpc": (self.workers.get(target_node) or {}).get("grpc"),
                "promote_primary_to": new_primary,
                "created_at": str(time.time()),
            }

            # push job to redis stream (consumer will handle streaming copy)
            try:
                # Redis xadd requires string values; we push flattened dict
                self.redis.xadd("repl_jobs", job)
                print(f"Enqueued re-replication job for bucket {bucket}: {job['job_id']} source={source_node} -> target={target_node}")
            except Exception as e:
                print("Failed to enqueue re-replication job:", e)

        # done: controller will have enqueued jobs to recreate replicas.
        # mapping_for_key will now use alive nodes (dead node filtered out) so clients
        # will be redirected to the new primary. Re-replication will happen asynchronously.
