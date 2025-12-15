import grpc
import kv_pb2, kv_pb2_grpc
import asyncio


class ReplicatorServicer(kv_pb2_grpc.ReplicatorServicer):
    def __init__(self, storage, node_id):
        self.storage = storage
        self.node_id = node_id

    async def Replicate(self, request, context):
        try:
            kv = request.kv
            await self.storage.put(kv.key, kv.value, kv.version)
            return kv_pb2.ReplicateResponse(ok=True, message="OK")
        except Exception as e:
            return kv_pb2.ReplicateResponse(ok=False, message=str(e))

    async def StreamBucket(self, request, context):
        bucket = request.bucket_id
        items = await self.storage.list_keys_in_bucket(bucket)

        for d in items:
            kv = kv_pb2.KeyValue(
                key=d["key"],
                value=d["value"],
                version=int(d.get("version", 1))
            )
            yield kv_pb2.StreamKV(kv=kv)


async def serve_grpc(host, port, storage, node_id):
    server = grpc.aio.server()
    kv_pb2_grpc.add_ReplicatorServicer_to_server(
        ReplicatorServicer(storage, node_id),
        server
    )
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    import logging
    logger = logging.getLogger('worker.grpc')
    logger.info(f"gRPC server started {host}:{port}")
    await server.wait_for_termination()
