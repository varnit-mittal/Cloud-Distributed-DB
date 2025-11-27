# controller/repl_worker.py
import redis, time, grpc, kv_pb2, kv_pb2_grpc, asyncio
import json

REDIS_URL = "redis://redis:6379/0"

def run_consumer(redis_url=REDIS_URL):
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    last_id = "0-0"
    while True:
        try:
            msgs = r.xread({"repl_jobs": last_id}, block=5000, count=1)
            if not msgs:
                time.sleep(0.1)
                continue
            for stream, items in msgs:
                for item_id, fields in items:
                    last_id = item_id
                    job = fields
                    # fields are strings: job_id, bucket_id, source_grpc, target_grpc ...
                    job_dict = job
                    # perform streaming copy
                    src = job_dict.get("source_grpc")
                    tgt = job_dict.get("target_grpc")
                    bucket = int(job_dict.get("bucket_id",0))
                    print("Processing job", job_dict)
                    # gRPC stream from src
                    async def do_copy():
                        try:
                            async with grpc.aio.insecure_channel(src) as chan:
                                stub = kv_pb2_grpc.ReplicatorStub(chan)
                                req = kv_pb2.StreamBucketRequest(bucket_id=bucket, start=0, limit=10000)
                                async for kv in stub.StreamBucket(req):
                                    # push to target via gRPC replicate
                                    async with grpc.aio.insecure_channel(tgt) as tchan:
                                        tstub = kv_pb2_grpc.ReplicatorStub(tchan)
                                        rreq = kv_pb2.ReplicateRequest(kv=kv.kv, from_node="controller-repl")
                                        await tstub.Replicate(rreq, timeout=10)
                        except Exception as e:
                            print("job failed:", e)
                    asyncio.run(do_copy())
        except Exception as e:
            print("consumer error", e)
            time.sleep(1)

if __name__ == "__main__":
    run_consumer()
