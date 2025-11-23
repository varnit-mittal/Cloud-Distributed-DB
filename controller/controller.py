import hashlib
import asyncio
import time
from typing import Dict, Tuple, List
from etcd_client import EtcdClient

# Basic consistent mapping using hash mod N
class Controller:
    def __init__(self, etcd_host="127.0.0.1", etcd_port=2379):
        self.etcd = EtcdClient(host=etcd_host, port=etcd_port)
        self.workers = self.etcd.load_workers()  # node_id -> address
        self.last_heartbeat = {}  # node_id -> ts

    def refresh_workers(self):
        self.workers = self.etcd.load_workers()

    def register_worker(self, node_id: str, addr: str):
        self.workers[node_id] = addr
        self.etcd.save_workers(self.workers)
        self.last_heartbeat[node_id] = time.time()

    def heartbeat(self, node_id: str):
        self.last_heartbeat[node_id] = time.time()

    def get_alive_workers(self, threshold=10) -> Dict[str, str]:
        now = time.time()
        alive = {}
        for n, addr in self.workers.items():
            ts = self.last_heartbeat.get(n, 0)
            if now - ts < threshold:
                alive[n] = addr
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
        # returns primary address and replica addresses (next 2)
        if not self.workers:
            raise RuntimeError("No workers registered")
        node_list = list(self.workers.items())  # [(node_id, addr), ...]
        node_list_sorted = sorted(node_list, key=lambda x: x[0])  # deterministic order by node_id
        idx = self.partition_for_key(key)
        n = len(node_list_sorted)
        primary = node_list_sorted[idx % n]
        replica1 = node_list_sorted[(idx + 1) % n]
        replica2 = node_list_sorted[(idx + 2) % n]
        return {
            "primary": primary[1],
            "nodes": [primary[1], replica1[1], replica2[1]],
            "node_ids": [primary[0], replica1[0], replica2[0]]
        }
