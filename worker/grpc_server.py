# worker/grpc_server.py
import asyncio
import grpc
import kv_pb2
import kv_pb2_grpc
from storage import MongoStorage
from merkle import compute_merkle_root  # ensure merkle supports storage interface / or adjust
import json

class ReplicatorServicer(kv_pb2_grpc.ReplicatorServicer):
    def __init__(self, storage: MongoStorage):
        self.storage = storage

    async def Replicate(self, request, context):
        kv = request.kv
        incoming_version = kv.version or 1
        # write to MongoStorage
        applied = await self.storage.put(kv.key, kv.value, incoming_version)
        if applied:
            return kv_pb2.ReplicateResponse(ok=True, message="replicated")
        else:
            return kv_pb2.ReplicateResponse(ok=True, message="ignored older version")

    async def GetMerkleRoot(self, request, context):
        # compute_merkle_root should accept a storage-like object with keys()/get()
        root = await compute_merkle_root(self.storage, bucket_count=16)
        return kv_pb2.MerkleRootResponse(root=root)

    async def TransferRange(self, request, context):
        # Simple MVP: return all items (or implement filtering by buckets)
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
