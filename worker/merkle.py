# worker/merkle.py
import hashlib
import asyncio
from typing import Dict, List

def hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

async def compute_merkle_root(storage, bucket_count=16, prefix=""):
    keys = []
    # Use storage.col directly to fetch keys in mongo
    cursor = storage.col.find({}, {"key":1, "value":1, "version":1})
    buckets = [[] for _ in range(bucket_count)]
    async for doc in cursor:
        k = doc["key"]
        idx = int(hashlib.sha256(k.encode()).hexdigest(), 16) % bucket_count
        buckets[idx].append(doc)
    bucket_hashes=[]
    for b in buckets:
        if not b:
            bucket_hashes.append(hash_bytes(b""))
            continue
        acc = b"".join([(item["key"] + "=" + str(item.get("value","")) + ":" + str(item.get("version",1))).encode() for item in b])
        bucket_hashes.append(hash_bytes(acc))
    nodes = bucket_hashes
    while len(nodes) > 1:
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1])
        new_nodes=[]
        for i in range(0,len(nodes),2):
            new_nodes.append(hash_bytes((nodes[i]+nodes[i+1]).encode()))
        nodes=new_nodes
    return nodes[0] if nodes else hash_bytes(b"")
