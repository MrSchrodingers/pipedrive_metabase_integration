import os
import json
import redis
from datetime import timedelta
from infrastructure.config.settings import settings

class RedisCache:
    def __init__(self):
        self.client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

    def get(self, key):
        value = self.client.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return None

    def set(self, key, value, ex_seconds=3600):
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        self.client.set(name=key, value=value, ex=timedelta(seconds=ex_seconds))

    def delete(self, key):
        self.client.delete(key)
