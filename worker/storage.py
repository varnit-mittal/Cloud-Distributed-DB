# worker/storage.py
import motor.motor_asyncio
from typing import Optional

class MongoStorage:
    def __init__(self, mongo_uri="mongodb://mongo:27017", db_name="kvstore"):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db.kv

    async def put(self, key: str, value: str, version: int = 1):
        # Upsert with version check: only update if incoming version >= current
        existing = await self.col.find_one({"key": key})
        if existing:
            # preserve only if version >= existing.version
            if version >= existing.get("version", 0):
                await self.col.update_one({"key": key}, {"$set": {"value": value, "version": version}})
        else:
            await self.col.insert_one({"key": key, "value": value, "version": version})

    async def get(self, key: str) -> Optional[dict]:
        doc = await self.col.find_one({"key": key}, {"_id": 0})
        return doc

    async def list_keys_in_bucket(self, bucket_id: int, bucket_count: int = 16):
        # Very simple bucketing: hash(key) % bucket_count == bucket_id
        import hashlib
        cursor = self.col.find({}, {"key":1, "value":1, "version":1, "_id":0})
        out = []
        async for d in cursor:
            h = int(hashlib.sha256(d["key"].encode()).hexdigest(), 16) % bucket_count
            if h == bucket_id:
                out.append(d)
        return out

    async def close(self):
        self.client.close()
