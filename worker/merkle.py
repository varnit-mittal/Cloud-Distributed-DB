# simple merkle by hashing key buckets
import hashlib
import asyncio
import math
from typing import Dict, List

def hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

async def compute_merkle_root(redis_client, bucket_count=16, prefix=""):
    """
    Simple approach:
    - divide keyspace into bucket_count buckets by hash(key) % bucket_count
    - for each bucket compute hash of concatenated key+value+version
    - compute merkle root by pairwise hashing
    """
    keys = await redis_client.keys(prefix + "*")
    buckets = [[] for _ in range(bucket_count)]
    for k in keys:
        idx = int(hashlib.sha256(k.encode()).hexdigest(), 16) % bucket_count
        v = await redis_client.get(k)
        if v:
            buckets[idx].append((k, v))
    # bucket hashes
    bucket_hashes = []
    for b in buckets:
        if not b:
            bucket_hashes.append(hash_bytes(b""))
            continue
        acc = b"".join([ (kv[0]+"="+(kv[1].get("value","") if isinstance(kv[1], dict) else kv[1])).encode() for kv in b ])
        bucket_hashes.append(hash_bytes(acc))
    # build tree
    nodes = bucket_hashes
    while len(nodes) > 1:
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1])
        new_nodes = []
        for i in range(0, len(nodes), 2):
            new_nodes.append(hash_bytes((nodes[i] + nodes[i+1]).encode()))
        nodes = new_nodes
    return nodes[0] if nodes else hash_bytes(b"")
