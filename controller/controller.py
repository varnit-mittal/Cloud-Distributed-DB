# controller.py
import hashlib
import time
import json
from typing import Dict, Tuple, List, Any
from etcd_client import EtcdClient

# Basic consistent mapping using hash mod N
class Controller:
    def __init__(self, etcd_host="127.0.0.1", etcd_port=2379):
        self.etcd = EtcdClient(host=etcd_host, port=etcd_port)
        # try to load workers from etcd; normalize to node_id -> {http, grpc}
        raw = self.etcd.load_workers() or {}
        self.workers = self._normalize_loaded_workers(raw)   # node_id -> dict(http, grpc)
        self.last_heartbeat = {}  # node_id -> ts

    def _normalize_loaded_workers(self, raw: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """
        Normalize whatever etcd returned into node_id -> {'http':..., 'grpc':...}
        raw may be:
          - {node_id: "http://workerN:8001"}  (legacy)
          - {node_id: '{"http": "...", "grpc":"..."}'} (json-string)
          - {node_id: {"http": "...", "grpc": "..."}} (dict)
        """
        out = {}
        for nid, v in raw.items():
            # if v is dict-like with http/grpc
            if isinstance(v, dict):
                http = v.get("http")
                grpc = v.get("grpc")
                out[nid] = {"http": http, "grpc": grpc}
                continue
            # if v is a string try to parse JSON
            if isinstance(v, str):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, dict):
                        out[nid] = {"http": parsed.get("http"), "grpc": parsed.get("grpc")}
                        continue
                except Exception:
                    pass
                # fallback: treat v as legacy http addr string
                http = v
                # derive host for grpc
                try:
                    from urllib.parse import urlparse
                    p = urlparse(v)
                    host = p.hostname or v
                except Exception:
                    host = v
                grpc = f"{host}:50051"
                out[nid] = {"http": http, "grpc": grpc}
                continue
            # unknown type - skip
        return out

    def refresh_workers(self):
        raw = self.etcd.load_workers() or {}
        self.workers = self._normalize_loaded_workers(raw)

    def register_worker(self, node_id: str, http_addr: str = None, grpc_addr: str = None):
        """
        Register or update a worker.
        Stores self.workers[node_id] = {'http':..., 'grpc':...}
        Persists to etcd via save_workers (best-effort).
        """
        if http_addr is None and grpc_addr is None:
            raise ValueError("http_addr and/or grpc_addr required")

        existing = self.workers.get(node_id, {})
        http = http_addr or existing.get("http")
        grpc = grpc_addr or existing.get("grpc")
        self.workers[node_id] = {"http": http, "grpc": grpc}
        # persist to etcd (attempt to save the dict; EtcdClient.save_workers should accept serializable)
        try:
            # try to save the dict directly
            self.etcd.save_workers(self.workers)
        except Exception:
            # fallback: save JSON-serialized values (node_id -> json-string)
            try:
                serial = {nid: json.dumps(v) for nid, v in self.workers.items()}
                self.etcd.save_workers(serial)
            except Exception as e:
                # log but don't fail registration
                print("Warning: failed to persist workers to etcd:", e)
        self.last_heartbeat[node_id] = time.time()

    def heartbeat(self, node_id: str):
        self.last_heartbeat[node_id] = time.time()

    def get_alive_workers(self, threshold=10) -> Dict[str, Dict[str, str]]:
        now = time.time()
        alive = {}
        for n, meta in self.workers.items():
            ts = self.last_heartbeat.get(n, 0)
            if now - ts < threshold:
                alive[n] = meta
        return alive

    def partition_for_key(self, key: str) -> int:
        # For now partition = hash(key) mod N
        # N = number of workers
        if not self.workers:
            raise RuntimeError("No workers registered")
        n = len(self.workers)
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return h % n

    def mapping_for_key(self, key: str) -> Dict[str, object]:
        """
        returns a mapping object with:
          - primary: {node_id, http, grpc}
          - nodes: [ {node_id, http, grpc}, ... ]  (replication_factor entries)
          - node_ids: [node_id1, node_id2, ...]
        """
        if not self.workers:
            raise RuntimeError("No workers registered")
        # deterministic order of node ids
        node_items = sorted(self.workers.items(), key=lambda x: x[0])  # [(node_id, meta_dict), ...]
        node_ids = [nid for nid, _ in node_items]
        idx = self.partition_for_key(key)
        n = len(node_items)
        selected = []
        i = idx
        while len(selected) < 3:
            nid = node_ids[i % n]
            if nid not in [s["node_id"] for s in selected]:
                meta = self.workers.get(nid, {})
                selected.append({"node_id": nid, "http": meta.get("http"), "grpc": meta.get("grpc")})
            i += 1
            # safety: break if we looped many times (shouldn't happen)
            if i - idx > n * 2:
                break

        primary = selected[0] if selected else None
        nodes = selected
        node_ids_out = [s["node_id"] for s in selected]
        return {"primary": primary, "nodes": nodes, "node_ids": node_ids_out}
