# merkle.py
import hashlib
from typing import List, Dict, Any

def hash_bytes(b: bytes) -> str:
    """Return SHA-256 hex digest."""
    return hashlib.sha256(b).hexdigest()

async def compute_merkle_root(storage, bucket_count=16):
    """
    Computes a Merkle root over all key/value pairs stored in MongoDB:
    - Uses storage.keys()   -> list of keys
    - Uses storage.get(key) -> {"value": ..., "version": ...}
    
    Process:
      1. Hash keys into N buckets using SHA256(key) % bucket_count
      2. For each bucket:
            hash all (key=value:version) pairs
      3. Build a binary Merkle tree from bucket hashes
      4. Return root hash
    """

    # Step 1: get all keys from Mongo
    keys = await storage.keys()           # list[str]

    # Prepare bucket lists
    buckets: List[List[tuple]] = [[] for _ in range(bucket_count)]

    # Step 2: fill buckets
    for key in keys:
        idx = int(hashlib.sha256(key.encode()).hexdigest(), 16) % bucket_count
        v = await storage.get(key)

        if v is None:
            continue

        value = v.get("value", "")
        version = int(v.get("version", 1))

        buckets[idx].append((key, value, version))

    # Step 3: hash each bucket
    bucket_hashes: List[str] = []

    for bucket in buckets:
        if not bucket:
            # empty bucket → hash of empty bytes
            bucket_hashes.append(hash_bytes(b""))
            continue

        # Sort entries to ensure deterministic output
        bucket_sorted = sorted(bucket, key=lambda x: x[0])

        # Concatenate key + value + version
        acc = b"".join([
            f"{k}={val}:{ver}".encode()
            for (k, val, ver) in bucket_sorted
        ])

        bucket_hashes.append(hash_bytes(acc))

    # Step 4: build Merkle tree (hash pairs)
    nodes = bucket_hashes[:]

    while len(nodes) > 1:
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1])  # duplicate last hash if odd number of nodes

        new_nodes = []
        for i in range(0, len(nodes), 2):
            combined = (nodes[i] + nodes[i+1]).encode()
            new_nodes.append(hash_bytes(combined))

        nodes = new_nodes

    # Step 5: return root
    return nodes[0] if nodes else hash_bytes(b"")
