# worker/storage.py
import motor.motor_asyncio
from typing import Optional, Dict, Any, List
from pymongo import ReturnDocument

class MongoStorage:
    def __init__(self, mongo_uri="mongodb://mongo:27017", db_name="kvstore", coll_name="kv"):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.coll = self.db[coll_name]
        # ensure index on key
        # Note: create_index is async but safe to call multiple times
        try:
            # schedule index creation, non-blocking
            self.coll.create_index("key", unique=True)
        except Exception:
            pass

    async def put(self, key: str, value: str, version: int = 1) -> bool:
        """
        Write the key only if incoming version >= existing version.
        Returns True if write applied, False if ignored due to older version.
        """
        # Try atomic conditional update: if existing.version <= version OR no existing doc
        filter_doc = {
            "key": key,
            "$or": [
                {"version": {"$lte": version}},
                {"version": {"$exists": False}}
            ]
        }
        update_doc = {"$set": {"value": value, "version": version}}
        res = await self.coll.find_one_and_update(
            filter_doc,
            update_doc,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        # If res is not None then operation succeeded (either update or upsert).
        return res is not None

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        doc = await self.coll.find_one({"key": key})
        if not doc:
            return None
        return {"value": doc.get("value"), "version": int(doc.get("version", 1))}

    async def keys(self, pattern: str = "") -> List[str]:
        # pattern ignored for now (simple MVP)
        cursor = self.coll.find({}, {"_id": 0, "key": 1})
        keys = []
        async for doc in cursor:
            keys.append(doc["key"])
        return keys

    async def get_many(self, keys: list) -> Dict[str, Optional[Dict[str, Any]]]:
        docs = self.coll.find({"key": {"$in": keys}})
        out = {}
        async for d in docs:
            out[d["key"]] = {"value": d.get("value"), "version": int(d.get("version", 1))}
        # include missing keys as None
        for k in keys:
            out.setdefault(k, None)
        return out
