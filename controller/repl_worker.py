"""
Re-replication worker for the controller.

Consumes Redis stream 'repl_jobs':
    job_id
    bucket_id
    source_node
    source_grpc
    target_node
    target_grpc
    promote_primary_to
    created_at

It streams buckets from source via gRPC StreamBucket(),
and writes KV pairs to target via Replicate().
"""

import asyncio
import grpc
import redis
import time
import kv_pb2
import kv_pb2_grpc

REDIS_URL = "redis://redis:6379/0"


class ReplWorker:
    def __init__(self, redis_url=REDIS_URL):
        self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.last_id = "0-0"

    async def copy_bucket(self, job):
        """Perform bucket streaming from source_grpc -> target_grpc."""
        bucket = int(job["bucket_id"])
        source = job["source_grpc"]
        target = job["target_grpc"]
        job_id = job["job_id"]

        print(f"[JOB {job_id}] Starting copy: source={source} -> target={target} bucket={bucket}")

        try:
            async with grpc.aio.insecure_channel(source) as s_chan:
                s_stub = kv_pb2_grpc.ReplicatorStub(s_chan)

                req = kv_pb2.StreamBucketRequest(
                    bucket_id=bucket,
                    start=0,
                    limit=1000000
                )

                async for kv_msg in s_stub.StreamBucket(req):
                    kv = kv_msg.kv

                    # replicate to target
                    try:
                        async with grpc.aio.insecure_channel(target) as t_chan:
                            t_stub = kv_pb2_grpc.ReplicatorStub(t_chan)
                            r_req = kv_pb2.ReplicateRequest(kv=kv, from_node="controller-repl")
                            await t_stub.Replicate(r_req, timeout=10)

                    except Exception as e:
                        print(f"[JOB {job_id}] FAILED replicate to {target}: {e}")
                        # retry logic can be added here
                        return False

            print(f"[JOB {job_id}] Completed bucket copy successfully.")
            return True

        except Exception as e:
            print(f"[JOB {job_id}] FAILED stream from {source}: {e}")
            return False

    def write_done(self, job, status):
        """Push completion event to repl_done stream."""
        entry = {
            "job_id": job["job_id"],
            "bucket_id": job["bucket_id"],
            "source_node": job["source_node"],
            "target_node": job["target_node"],
            "status": "success" if status else "failed",
            "timestamp": str(time.time()),
        }
        try:
            self.redis.xadd("repl_done", entry)
        except Exception as e:
            print("Failed to write repl_done:", e)

    async def process_job(self, job):
        ok = await self.copy_bucket(job)
        self.write_done(job, ok)

    async def run_forever(self):
        print("=== ReplWorker started ===")
        while True:
            try:
                msgs = self.redis.xread({"repl_jobs": self.last_id}, block=5000, count=1)

                if not msgs:
                    await asyncio.sleep(0.1)
                    continue

                for stream_name, items in msgs:
                    for msg_id, fields in items:
                        self.last_id = msg_id
                        job = fields  # string fields
                        print(f"[JOB {job.get('job_id')}] Received job")

                        await self.process_job(job)

            except Exception as e:
                print("ERROR in repl worker:", e)
                await asyncio.sleep(1)


async def main():
    worker = ReplWorker()
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
