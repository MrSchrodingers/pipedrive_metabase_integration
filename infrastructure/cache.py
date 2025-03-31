import json
import redis
from datetime import timedelta

class RedisCache:
    def __init__(self, connection_string: str):
        self.client = redis.Redis.from_url(
            connection_string,
            decode_responses=True
        )

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
        self.client.setex(
            name=key,
            time=timedelta(seconds=ex_seconds),
            value=value
        )

    def delete(self, key):
        self.client.delete(key)