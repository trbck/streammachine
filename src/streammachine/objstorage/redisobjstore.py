"""
StreamMachine Redis Object Storage Module

This module provides RedisObjectStorage for storing Python objects in Redis
with automatic pickle serialization.

Why pickle?
    Redis stores bytes, and pickle provides:
    - Serialization of arbitrary Python objects
    - Handles complex types (nested dicts, custom classes)
    - Preserves object identity and references

    Security note: Only unpickle data from trusted sources. Pickle can
    execute arbitrary code during deserialization.

Distributed Locking:
    The store_with_pickle method uses Redis's distributed lock to prevent
    concurrent writes to the same key. This is important when multiple
    processes/instances might try to update the same key simultaneously.

    Lock behavior:
    - Default wait time: None (waits indefinitely)
    - Lock is automatically released when context exits
    - Lock key is "lock:{data_key}"

When to use:
    - Sharing state between independent services
    - Persisting complex Python objects
    - When you need distributed locking for writes

When NOT to use:
    - Simple string/number values (use Redis directly)
    - High-frequency writes (lock contention will slow you down)
    - When data must be readable by non-Python services (use JSON)

Example:
    async with RedisObjectStorage() as storage:
        # Store a complex object
        await storage.store_with_pickle('model', {'weights': [1, 2, 3]})

        # Retrieve it
        model = await storage.retrieve_with_pickle('model')

        # List keys matching pattern
        keys = await storage.list_keys('model:*')

        # Clean up
        await storage.delete_keys('model:*')
"""
import redis.asyncio as redis
import pickle
import time
import asyncio
import logging
from typing import Any, List, Optional


class RedisObjectStorage:
    """
    Async Redis object storage with pickle serialization.

    Provides methods to store and retrieve Python objects in Redis
    with automatic pickle serialization and optional locking.

    Example:
        storage = RedisObjectStorage()
        await storage.store_with_pickle('my_key', {'data': 'value'})
        obj = await storage.retrieve_with_pickle('my_key')
        await storage.close()
    """

    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379,
        redis_db: int = 0,
        log_level: int = logging.WARNING
    ):
        """
        Initialize the Redis client and logger.

        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            log_level: Logging level
        """
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=log_level)

    async def store_with_pickle(self, key: str, obj: Any) -> float:
        """
        Store a Python object in Redis using pickle serialization.

        Uses a distributed lock to prevent concurrent writes to the same key.

        Args:
            key: The key to store the object under
            obj: The Python object to store

        Returns:
            Time taken in milliseconds
        """
        start_time = time.time()
        async with self.redis_client.lock(f"lock:{key}"):
            pickled_obj = pickle.dumps(obj)
            await self.redis_client.set(key, pickled_obj)
        end_time = time.time()
        elapsed_ms = (end_time - start_time) * 1000
        self.logger.info(f"Pickle serialization time for {key}: {elapsed_ms:.2f} ms")
        return elapsed_ms

    async def retrieve_with_pickle(self, key: str) -> Optional[Any]:
        """
        Retrieve a Python object from Redis using pickle deserialization.

        Args:
            key: The key to retrieve

        Returns:
            The deserialized Python object, or None if key doesn't exist
        """
        start_time = time.time()
        pickled_obj = await self.redis_client.get(key)
        obj = pickle.loads(pickled_obj) if pickled_obj else None
        end_time = time.time()
        elapsed_ms = (end_time - start_time) * 1000
        self.logger.info(f"Pickle read time for {key}: {elapsed_ms:.2f} ms")
        return obj

    async def list_keys(self, pattern: str = "*") -> List[str]:
        """
        List all keys in Redis that match the given pattern.

        Args:
            pattern: Key pattern to match (supports Redis wildcards)

        Returns:
            List of matching keys as strings
        """
        keys = await self.redis_client.keys(pattern)
        result = [key.decode('utf-8') for key in keys]
        self.logger.info(f"Keys matching pattern '{pattern}': {result}")
        return result

    async def delete_keys(self, pattern: str) -> int:
        """
        Delete all keys in Redis that match the given pattern.

        Args:
            pattern: Key pattern to match (supports Redis wildcards)

        Returns:
            Number of keys deleted
        """
        keys = await self.redis_client.keys(pattern)
        if keys:
            deleted = await self.redis_client.delete(*keys)
            self.logger.info(
                f"Deleted {deleted} keys matching pattern '{pattern}'"
            )
            return deleted
        else:
            self.logger.info(f"No keys matching pattern '{pattern}' found to delete.")
            return 0

    async def close(self) -> None:
        """Close the Redis client connection."""
        await self.redis_client.aclose()
        self.logger.debug("Redis connection closed")

    async def __aenter__(self) -> "RedisObjectStorage":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Async context manager exit."""
        await self.close()
        return False