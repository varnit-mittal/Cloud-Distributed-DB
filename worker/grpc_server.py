# worker/grpc_server.py
import asyncio
import grpc
import kv_pb2, kv_pb2_grpc

class ReplicatorServicer(kv_pb2_grpc.ReplicatorServicer):
    def __init__(self, storage, node_id):
        self.storage = storage
        self.node_id = node_id

    async def Replicate(self, request, context):
        kv = request.kv
        try:
            await self.storage.put(kv.key, kv.value, kv.version)
            return kv_pb2.ReplicateResponse(ok=True, message="OK")
        except Exception as e:
            return kv_pb2.ReplicateResponse(ok=False, message=str(e))

    async def StreamBucket(self, request, context):
        bucket_id = request.bucket_id
        items = await self.storage.list_keys_in_bucket(bucket_id)
        for it in items:
            kv = kv_pb2.KeyValue(key=it["key"], value=it["value"], version=int(it.get("version",1)))
            yield kv_pb2.StreamKV(kv=kv)

async def serve_grpc(host="0.0.0.0", port=50051, storage=None, node_id="worker"):
    server = grpc.aio.server()
    kv_pb2_grpc.add_ReplicatorServicer_to_server(ReplicatorServicer(storage, node_id), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    print(f"gRPC server started on {host}:{port}")
    await server.wait_for_termination()
