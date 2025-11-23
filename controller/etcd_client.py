# simple etcd wrapper
import etcd3
import json
from typing import Dict, List

import os
ETCD_HOST = os.environ.get("ETCD_HOST", "etcd")
ETCD_PORT = int(os.environ.get("ETCD_PORT", "2379"))

class EtcdClient:
    def __init__(self, host=ETCD_HOST, port=ETCD_PORT):
        self.client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)

    def save_workers(self, workers: Dict[str, str]):
        # workers: {node_id: address}
        self.client.put("../workers", json.dumps(workers))

    def load_workers(self) -> Dict[str, str]:
        v, _ = self.client.get("/workers")
        if not v:
            return {}
        return json.loads(v.decode())

    def put(self, key: str, value: str):
        self.client.put(key, value)

    def get(self, key: str):
        v, _ = self.client.get(key)
        if v:
            return v.decode()
        return None
