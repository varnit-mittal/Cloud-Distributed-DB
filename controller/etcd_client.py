# simple etcd wrapper
import etcd3
import json
from typing import Dict, List

class EtcdClient:
    def __init__(self, host="127.0.0.1", port=2379):
        self.client = etcd3.client(host=host, port=port)

    def save_workers(self, workers: Dict[str, str]):
        # workers: {node_id: address}
        self.client.put("../workers", json.dumps(workers))

    def load_workers(self) -> Dict[str, str]:
        v, _ = self.client.get("../workers")
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
