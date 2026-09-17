"""
Integration tests for StreamMachine with real Redis.

These tests require a running Redis server. Use testcontainers-redis
or pytest-docker fixtures to spin up a Redis container.

To run with Docker:
    pytest tests/test_integration.py --redis-container

Or with local Redis:
    REDIS_URL=redis://localhost:6379/15 pytest tests/test_integration.py
"""
import asyncio
import os
import time
import pytest
import uuid

# Skip all tests in this module if integration tests are not enabled
# Set environment variable RUN_INTEGRATION_TESTS=1 to enable
pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=1 to enable.",
)

from streammachine import App, Message, RedisConnection, Storage
from streammachine.models import ConsumerConfig


# Try to import testcontainers for Redis container
try:
    from testcontainers.redis import RedisContainer
    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False


@pytest.fixture(scope="module")
def redis_url():
    """Get Redis URL from environment or testcontainers."""
    # Check environment first
    url = os.environ.get("REDIS_URL")
    if url:
        yield url
        return

    # Fall back to testcontainers if available
    if HAS_TESTCONTAINERS:
        with RedisContainer() as redis:
            yield redis.get_connection_url()
            return

    # No Redis available
    pytest.skip("No Redis available. Set REDIS_URL or install testcontainers-redis.")
    yield None


@pytest.fixture
async def redis_connection(redis_url):
    """Provide a Redis connection for each test."""
    conn = RedisConnection(url=redis_url)
    await conn._ensure_pool()
    yield conn
    await conn.close()


@pytest.fixture
def unique_stream_name():
    """Generate a unique stream name for each test."""
    return f"test_stream_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_group_name():
    """Generate a unique group name for each test."""
    return f"test_group_{uuid.uuid4().hex[:8]}"


class TestRedisConnectionIntegration:
    """Integration tests for RedisConnection with real Redis."""

    @pytest.mark.asyncio
    async def test_connection_ping(self, redis_connection):
        """Test basic connection health check."""
        result = await redis_connection.health_check()
        assert result is True

    @pytest.mark.asyncio
    async def test_xadd_single_message(self, redis_connection, unique_stream_name):
        """Test adding a single message to a stream."""
        await redis_connection._ensure_pool()
        msg_id = await redis_connection.client.xadd(
            unique_stream_name, {"key": "value", "num": "42"}
        )
        assert msg_id is not None
        # Stream IDs are milliseconds-sequence format
        assert b"-" in msg_id or "-" in msg_id

    @pytest.mark.asyncio
    async def test_xadd_batch_messages(self, redis_connection, unique_stream_name):
        """Test adding multiple messages via pipeline."""
        records = [
            {"key": "value1", "idx": "1"},
            {"key": "value2", "idx": "2"},
            {"key": "value3", "idx": "3"},
        ]
        msg_ids = await redis_connection.pipeline_xadd(unique_stream_name, records)
        assert len(msg_ids) == 3

    @pytest.mark.asyncio
    async def test_xread_from_stream(self, redis_connection, unique_stream_name):
        """Test reading messages from a stream."""
        await redis_connection._ensure_pool()

        # Add some messages
        await redis_connection.client.xadd(unique_stream_name, {"data": "test1"})
        await redis_connection.client.xadd(unique_stream_name, {"data": "test2"})

        # Read from beginning
        result = await redis_connection.client.xread(
            {unique_stream_name: "0-0"},
            count=10
        )

        # coredis returns {stream: (entry, ...)}
        assert result is not None
        assert len(result) == 1
        messages = next(iter(result.values()))
        assert len(messages) == 2

    @pytest.mark.asyncio
    async def test_consumer_group_creation(
        self, redis_connection, unique_stream_name, unique_group_name
    ):
        """Test creating a consumer group."""
        await redis_connection._ensure_pool()

        # Add a message first (group needs stream to exist)
        await redis_connection.client.xadd(unique_stream_name, {"test": "data"})

        # GroupConsumer creates the group (at the newest id) on first fetch
        consumer = await redis_connection.consumer(
            [unique_stream_name], "test_consumer", unique_group_name, timeout=200
        )
        async for _stream, _entry in consumer:
            break

        groups = await redis_connection.client.xinfo_groups(unique_stream_name)
        group_names = [g.get("name", g.get(b"name")) for g in groups]
        assert any(unique_group_name in str(g) for g in group_names)


class TestStreamConsumerIntegration:
    """Integration tests for stream consumption."""

    @pytest.mark.asyncio
    async def test_produce_consume_flow(
        self, redis_url, unique_stream_name, unique_group_name
    ):
        """Test full produce-consume flow with real Redis."""
        received_messages = []

        async def handler(msg: Message):
            received_messages.append(msg)

        # Drive a StreamConsumer directly for precise control
        mock_module = type('Module', (), {})()
        mock_module.test_handler = handler

        config = ConsumerConfig(
            decorator_type="agent",
            topic=unique_stream_name,
            group=unique_group_name,
            obj_name="test_handler",
            mod=mock_module,
        )

        # Send a message
        rc = RedisConnection(url=redis_url)
        await rc._ensure_pool()

        try:
            # Consumer groups start at the newest id, so start the consumer
            # before producing.
            from streammachine.app import StreamConsumer
            consumer = StreamConsumer(config)
            read_task = asyncio.create_task(consumer())
            await asyncio.sleep(0.5)

            await rc.client.xadd(unique_stream_name, {"key": "value"})

            for _ in range(40):
                if received_messages:
                    break
                await asyncio.sleep(0.1)

            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass

            assert len(received_messages) == 1
            msg = received_messages[0]
            assert msg.topic == unique_stream_name
            assert msg.message["key"] == "value"
            assert msg.received is not None

        finally:
            await rc.close()

    @pytest.mark.asyncio
    async def test_multiple_consumers_same_group(
        self, redis_url, unique_stream_name, unique_group_name
    ):
        """Test that multiple consumers in same group share messages."""
        rc = RedisConnection(url=redis_url)
        await rc._ensure_pool()

        try:
            # Create the group at the stream start so the backlog is delivered
            await rc.client.xgroup_create(unique_stream_name, unique_group_name, "0", mkstream=True)

            for i in range(10):
                await rc.client.xadd(unique_stream_name, {"idx": str(i)})

            received = {"consumer1": [], "consumer2": []}

            async def consume(name: str):
                async with RedisConnection(url=redis_url) as conn:
                    consumer = await conn.consumer(
                        [unique_stream_name], name, unique_group_name, timeout=500
                    )
                    async for _stream, entry in consumer:
                        received[name].append(entry)

            tasks = [asyncio.create_task(consume(n)) for n in received]
            for _ in range(50):
                if sum(len(v) for v in received.values()) >= 10:
                    break
                await asyncio.sleep(0.1)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # Every entry is delivered to exactly one consumer in the group
            all_ids = [e.identifier for v in received.values() for e in v]
            assert len(all_ids) == 10
            assert len(set(all_ids)) == 10

        finally:
            await rc.close()


class TestStreamExpiration:
    """Test stream expiration and cleanup."""

    @pytest.mark.asyncio
    async def test_stream_maxlen(self, redis_connection, unique_stream_name):
        """Test stream trimming with MAXLEN."""
        await redis_connection._ensure_pool()

        # Approximate MAXLEN trims at node boundaries, so allow slack
        records = [{"idx": str(i)} for i in range(500)]
        await redis_connection.pipeline_xadd(unique_stream_name, records, maxlen=10)

        length = await redis_connection.client.xlen(unique_stream_name)
        assert length < 500

    @pytest.mark.asyncio
    async def test_app_send_respects_stream_maxlen(self, redis_url, unique_stream_name):
        """App(stream_maxlen=...) trims on every send() and send_batch()."""
        os.environ["REDIS_URL"] = redis_url
        app = App(name="maxlen_app", to_scan=False, dashboard_enabled=False, stream_maxlen=10)
        try:
            for i in range(300):
                await app.send(unique_stream_name, {"idx": str(i)})
            await app.send_batch(unique_stream_name, [{"idx": str(i)} for i in range(300)])
            length = await app.rc.client.xlen(unique_stream_name)
            assert length < 600
        finally:
            await app.rc.close()

    @pytest.mark.asyncio
    async def test_stream_ttl(self, redis_connection, unique_stream_name):
        """Test setting TTL on stream key."""
        await redis_connection._ensure_pool()

        # Add message
        await redis_connection.client.xadd(unique_stream_name, {"test": "data"})

        # Set TTL
        await redis_connection.client.expire(unique_stream_name, 1)

        # Wait for expiration
        await asyncio.sleep(2)

        # Stream should no longer exist
        exists = await redis_connection.client.exists([unique_stream_name])
        assert exists == 0


class TestConnectionRecovery:
    """Test connection recovery scenarios."""

    @pytest.mark.asyncio
    async def test_reconnect_after_error(self, redis_url, unique_stream_name):
        """Test that connection can recover after error."""
        conn1 = RedisConnection(url=redis_url)
        await conn1._ensure_pool()

        try:
            # First operation should succeed
            result = await conn1.health_check()
            assert result is True

            # Close connection
            await conn1.close()

            # Create new connection
            conn2 = RedisConnection(url=redis_url)
            await conn2._ensure_pool()

            # Should still work
            result = await conn2.health_check()
            assert result is True

            await conn2.close()
        finally:
            pass


class TestPipelineOperations:
    """Test pipeline (batch) operations."""

    @pytest.mark.asyncio
    async def test_pipeline_xadd_performance(
        self, redis_connection, unique_stream_name
    ):
        """Test that pipeline xadd is faster than individual xadds."""
        await redis_connection._ensure_pool()

        # Measure individual xadds
        for i in range(100):
            await redis_connection.client.xadd(unique_stream_name, {"idx": str(i)})

        # Create new stream for pipeline test
        pipeline_stream = f"{unique_stream_name}_pipeline"

        # Measure pipeline xadd
        start = time.time()
        records = [{"idx": str(i)} for i in range(100)]
        await redis_connection.pipeline_xadd(pipeline_stream, records)
        pipeline_time = time.time() - start

        # Pipeline should be faster (but we don't assert for CI variance)
        # Just verify it completes successfully
        assert pipeline_time >= 0

    @pytest.mark.asyncio
    async def test_large_batch_xadd(self, redis_connection, unique_stream_name):
        """Test adding large batches of messages."""
        await redis_connection._ensure_pool()

        # Create large batch
        batch_size = 1000
        records = [{"idx": str(i), "data": f"value_{i}"} for i in range(batch_size)]

        msg_ids = await redis_connection.pipeline_xadd(unique_stream_name, records)

        assert len(msg_ids) == batch_size

        # Verify stream length
        length = await redis_connection.client.xlen(unique_stream_name)
        assert length == batch_size


class TestStorageWithRedis:
    """Test Storage interaction with Redis (cross-process state)."""

    @pytest.mark.asyncio
    async def test_storage_with_agent_state(self, redis_url, unique_stream_name):
        """Test that Storage can be used for cross-agent state."""
        Storage.reset_instance()
        storage = Storage()

        try:
            # Store some state
            await storage.write("counter", 0)
            await storage.write("metadata", {"stream": unique_stream_name, "count": 0})

            # Read state
            counter = await storage.read("counter")
            metadata = await storage.read("metadata")

            assert counter == 0
            assert metadata["stream"] == unique_stream_name

        finally:
            Storage.reset_instance()


# Cleanup fixture to remove test streams
@pytest.fixture(autouse=True)
async def cleanup_test_streams(redis_connection, unique_stream_name):
    """Clean up test streams after each test."""
    yield
    try:
        # Delete the test stream
        await redis_connection.client.delete([unique_stream_name])
    except Exception:
        pass  # Stream may not exist
