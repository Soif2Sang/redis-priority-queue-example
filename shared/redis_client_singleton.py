import redis
import time
import os

# Configuration from environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

class RedisClientSingleton:
    """Thread‑safe (re)connect‑on‑demand Redis client singleton."""

    _client = None

    @classmethod
    def get_client(cls) -> redis.Redis:
        """Return a live Redis client, reconnecting as needed."""
        while cls._client is None:
            try:
                cls._client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    decode_responses=True,
                )
                cls._client.ping()
                print("Monitor: Successfully connected to Redis.")
            except redis.exceptions.ConnectionError as e:
                print(f"Monitor: Redis connection failed: {e}. Retrying in 5 s…")
                time.sleep(5)
                cls._client = None
        return cls._client

    @classmethod
    def reset(cls):
        """Drop the current client so that the next call reconnects."""
        cls._client = None