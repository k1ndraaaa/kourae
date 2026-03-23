from adapters.EnvLoader.MainClass import *
from adapters.Redis.Errors import *

import redis
class RedisClient:
    def __init__(
        self,
        host: str,
        port: int,
        db: int
    ):
        try:
            self.client = redis.Redis(
                host=host,
                port=port,
                db=db,
                decode_responses=True
            )
        except Exception as e:
            raise AdapterError(e) from e
        
    def healthcheck(self) -> bool:
        try:
            return self.client.ping()
        except Exception as e:
            raise AdapterError("Redis no responde") from e

    def set(self, key: str, value: str, ex: int | None = None):
        try:
            self.client.set(name=key, value=value, ex=ex)
        except Exception as e:
            raise RedisClientError(e) from e

    def get(self, key: str):
        try:
            return self.client.get(name=key)
        except Exception as e:
            raise RedisClientError(e) from e

    def delete(self, key: str):
        try:
            self.client.delete(key)
        except Exception as e:
            raise RedisClientError(e) from e