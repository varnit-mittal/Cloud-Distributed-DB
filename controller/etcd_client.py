import etcd3
import json

class EtcdClient:
    def __init__(self, host="127.0.0.1", port=2379):
        self.client = etcd3.client(host=host, port=port)

    def save_workers(self, workers_dict):
        for nid, meta in workers_dict.items():
            self.client.put(f"workers/{nid}", json.dumps(meta))

    def load_workers(self):
        out={}
        prefix = self.client.get_prefix("workers/")
        for value, meta in prefix:
            key = meta.key.decode().split("/",1)[1]
            try:
                out[key] = json.loads(value.decode())
            except Exception:
                out[key] = value.decode()
        return out
