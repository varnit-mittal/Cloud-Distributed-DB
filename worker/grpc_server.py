import asyncio
import grpc
import kv_pb2
import kv_pb2_grpc
from storage import RedisStorage
from merkle import compute_merkle_root as compute_root
import json

class ReplicatorServicer(kv_pb2_grpc.ReplicatorServicer):
    def __init__(self, storage: RedisStorage):
        self.storage = storage

    async def Replicate(self, request, context):
        kv = request.kv
        # idempotent write: accept version if greater or equal (simple)
        existing = await self.storage.get(kv.key)
        incoming_version = kv.version or 1
        if existing and existing.get("version", 0) > incoming_version:
            # older write, ignore
            return kv_pb2.ReplicateResponse(ok=True, message="ignored older version")
        await self.storage.put(kv.key, kv.value, incoming_version)
        return kv_pb2.ReplicateResponse(ok=True, message="replicated")

    async def GetMerkleRoot(self, request, context):
        root = await compute_root(request.partition)
        return kv_pb2.MerkleRootResponse(root=root)

    async def TransferRange(self, request, context):
        # get keys in bucket range and stream them (for simplicity we return all)
        # NOTE: this RPC returns all items in the range in a single message (not streaming) to keep MVP simple.
        # In production you'd implement streaming RPC.
        start = request.start_bucket
        end = request.end_bucket
        # naive fetch: return all keys (filtering by buckets omitted for brevity)
        keys = await self.storage.keys()
        kvs = []
        for k in keys:
            v = await self.storage.get(k)
            if v:
                kvs.append(kv_pb2.KeyValue(key=k, value=str(v.get("value")), version=int(v.get("version",1))))
        return kv_pb2.RangeResponse(kvs=kvs)


async def serve_grpc(host="0.0.0.0", port=50051, storage=None):
    server = grpc.aio.server()
    servicer = ReplicatorServicer(storage)
    kv_pb2_grpc.add_ReplicatorServicer_to_server(servicer, server)
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    await server.start()
    print(f"gRPC server started on {listen_addr}")
    await server.wait_for_termination()
