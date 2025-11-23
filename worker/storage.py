import redis.asyncio as aioredis
import json
from typing import Optional

class RedisStorage:
    def __init__(self, url="redis://localhost:6379/0"):
        self.url = url
        self.client = aioredis.from_url(self.url, decode_responses=True)

    async def put(self, key: str, value: str, version: int = None):
        # store value and version in a JSON blob
        if version is None:
            version = 1
        data = {"value": value, "version": version}
        await self.client.set(key, json.dumps(data))
        return True

    async def get(self, key: str):
        v = await self.client.get(key)
        if not v:
            return None
        return json.loads(v)

    async def keys(self, pattern="*"):
        return await self.client.keys(pattern)

    async def get_many(self, keys):
        vals = await self.client.mget(keys)
        import json
        out = {}
        for k, v in zip(keys, vals):
            out[k] = json.loads(v) if v else None
        return out
