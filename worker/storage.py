import motor.motor_asyncio
import hashlib

class MongoStorage:
    def __init__(self, mongo_uri, db_name="kvstore"):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db.kv

    async def put(self, key, value, version):
        existing = await self.col.find_one({"key": key})
        if existing:
            if version >= existing.get("version", 0):
                await self.col.update_one(
                    {"key": key},
                    {"$set": {"value": value, "version": version}}
                )
        else:
            await self.col.insert_one({"key": key, "value": value, "version": version})

    async def get(self, key):
        doc = await self.col.find_one({"key": key}, {"_id": 0})
        return doc

    async def list_keys_in_bucket(self, bucket_id, bucket_count=16):
        cursor = self.col.find({}, {"_id": 0, "key": 1, "value": 1, "version": 1})
        out = []
        async for d in cursor:
            h = int(hashlib.sha256(d["key"].encode()).hexdigest(), 16)
            if h % bucket_count == bucket_id:
                out.append(d)
        return out
