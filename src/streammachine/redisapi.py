"""
StreamMachine Redis API Module

This module provides the RedisConnection class for async Redis operations
using coredis, an async-first Redis client.

Why coredis instead of redis-py?
    coredis is designed from the ground up for async operations:
    - Native async/await support (no sync-to-async wrappers)
    - Type hints throughout the codebase
    - Connection pooling with proper async context managers
    - Stream patterns (GroupConsumer) built-in

    redis-py's async support was added later and can have edge cases
    with connection management. coredis avoids these issues.

Connection Pooling:
    Each RedisConnection instance manages a connection pool. The pool
    is created lazily on first access and must be entered as an async
    context before use (coredis 6.x requirement).

    Key methods:
    - _ensure_pool(): Enters the pool context (creates task group)
    - close(): Exits the pool context and closes connections

Consumer Groups:
    Consumer groups enable horizontal scaling. Multiple consumers in
    the same group share the message load:
    - Each message is delivered to exactly one consumer
    - Consumers track their position in the stream
    - Pending Entries List (PEL) tracks unacknowledged messages

    The GroupConsumer class from coredis provides:
    - Automatic group creation (if not exists)
    - XREADGROUP with configurable timeout
    - Optional auto-acknowledge

Example:
    async with RedisConnection() as rc:
        consumer = await rc.consumer("my_stream", "consumer_1", "my_group")
        async for stream, entry in consumer:
            print(f"Got message from {stream}: {entry}")
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, List, Optional

import coredis
from coredis import PureToken, Redis
from coredis.stream import GroupConsumer

from .models import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_MAX_CONNECTIONS,
    REDIS_CONNECTION_STRING,
)

logger = logging.getLogger(__name__)


class RedisConnection:
    """
    Async Redis connection manager using coredis with connection pooling.

    Provides consumer group support for Redis Streams and async context
    manager protocol for proper resource cleanup.

    Connection Management:
        The client uses lazy initialization - the Redis client and pool
        are created on first access. This avoids issues with coredis 6.x
        requiring an async context for the connection pool.

        Always use the async context manager for proper cleanup::

            async with RedisConnection() as rc:
                await rc.client.set("key", "value")

        Or manually manage::

            rc = RedisConnection()
            await rc._ensure_pool()  # Must call before any operation
            try:
                await rc.client.set("key", "value")
            finally:
                await rc.close()

    Connection Pooling:
        The max_connections parameter controls pool size. Each concurrent
        operation needs its own connection. For high-throughput apps,
        increase this (e.g., 50-100 for hundreds of concurrent agents).

    Environment Variables:
        REDIS_URL: Full connection URL (redis://host:port/db)
        REDIS_HOST: Host if not using URL
        REDIS_PORT: Port if not using URL
        REDIS_DB: Database number if not using URL
        REDIS_MAX_CONNECTIONS: Pool size

    Example:
        With URL::

            rc = RedisConnection(url="redis://localhost:6379/0")
            async with rc:
                await rc.client.set("key", "value")

        With individual params::

            rc = RedisConnection(host="localhost", port=6379, db=0)
            async with rc:
                consumer = await rc.consumer("stream", "consumer1", "group")
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
        max_connections: Optional[int] = None,
        url: Optional[str] = None,
    ):
        """
        Initialize Redis connection.

        Client creation is deferred until first use to avoid issues with
        coredis 6.x ConnectionPool requiring an async context (anyio task group).

        Args:
            host: Redis host (default from REDIS_HOST env var)
            port: Redis port (default from REDIS_PORT env var)
            db: Redis database number (default from REDIS_DB env var)
            max_connections: Max connection pool size (default from REDIS_MAX_CONNECTIONS env var)
            url: Redis connection URL (overrides individual params if provided)
        """
        self._url = url or REDIS_CONNECTION_STRING
        self._host = host or REDIS_HOST
        self._port = port or REDIS_PORT
        self._db = db if db is not None else REDIS_DB
        self._max_connections = max_connections or REDIS_MAX_CONNECTIONS
        self._use_url = url is not None
        self._client: Optional[Redis[bytes]] = None

    @property
    def client(self) -> Redis[bytes]:
        """Lazily create the Redis client on first access."""
        if self._client is None:
            if self._use_url:
                self._client = coredis.Redis.from_url(
                    self._url,
                    max_connections=self._max_connections
                )
            else:
                self._client = coredis.Redis(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    max_connections=self._max_connections
                )
        return self._client

    async def __aenter__(self) -> "RedisConnection":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """Async context manager exit - close connection."""
        await self.close()
        return False

    async def close(self) -> None:
        """Close the Redis connection pool."""
        if self._client is None:
            return
        try:
            await self.client.quit()
            logger.debug("Redis connection closed")
        except Exception as e:
            # Connection may not have been established (lazy connection).
            # This is fine during shutdown.
            logger.debug(f"Redis connection close skipped: {e}")

    async def consumer(
        self,
        channel: List[str],
        consumer: str,
        group: str,
        start_from_backlog: bool = False,
        auto_acknowledge: bool = True,
        timeout: int = 5000,
    ) -> GroupConsumer:
        """
        Create a Redis stream group consumer for the given channels.

        Args:
            channel: List of stream names to consume from
            consumer: Unique consumer identifier
            group: Consumer group name
            start_from_backlog: Whether to start from pending messages
            auto_acknowledge: Whether to auto-ack messages after processing
            timeout: Block timeout in milliseconds when waiting for new
                messages. Without this, xreadgroup returns immediately if
                no messages are available and the consumer loop exits.

        Returns:
            GroupConsumer instance for iterating over messages
        """
        if isinstance(channel, str):
            channel = [channel]
        return GroupConsumer(
            self.client,
            streams=channel,
            group=group,
            consumer=consumer,
            auto_acknowledge=auto_acknowledge,
            start_from_backlog=start_from_backlog,
            timeout=timeout,
        )

    async def pipeline_xadd(
        self, topic: str, records: List[dict], maxlen: Optional[int] = None
    ) -> List:
        """
        Batch add multiple records to a Redis stream using pipeline for speed.

        In coredis, pipeline commands are queued without await. The pipeline
        executes automatically when the async-with block exits, and results
        are available via pipe.results.

        Args:
            topic: Stream name
            records: List of record dictionaries to add
            maxlen: Optional approximate MAXLEN trim applied with each XADD

        Returns:
            List of message IDs from the XADD commands
        """
        trim_kwargs = (
            dict(
                trim_strategy=PureToken.MAXLEN,
                trim_operator=PureToken.APPROXIMATELY,
                threshold=maxlen,
            )
            if maxlen
            else {}
        )
        pipe = await self.client.pipeline(transaction=False)
        for record in records:
            await pipe.xadd(topic, record, **trim_kwargs)
        results = await pipe.execute()
        return list(results)

    async def health_check(self) -> bool:
        """
        Check if Redis connection is healthy.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            await self.client.ping()
            return True
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False


async def tests():
    """Test Redis connection functionality."""
    rc = RedisConnection()

    await rc.client.flushdb()

    async def producer():
        while True:
            [await rc.client.xadd("channel", {"id": i}) for i in range(11)]
            await asyncio.sleep(1)

    async def consumer():
        cons = await rc.consumer("channel", "consumer1", "group")

        while True:
            async for stream, entry in cons:
                print(stream)
                print(entry)

    res1, res2 = await asyncio.gather(
        producer(),
        consumer()
    )

    print("##################################################################################")
    print("test done")
    print("##################################################################################")

    await rc.close()


# asyncio.run(tests())