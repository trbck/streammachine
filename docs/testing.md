# Testing Guide

This guide covers testing StreamMachine applications.

## Unit Testing

### Testing Handlers

```python
import pytest
from unittest.mock import AsyncMock, MagicMock
from streammachine import Message

# Handler function to test
async def my_handler(record: Message):
    """Process a message."""
    if record.message.get("type") == "important":
        await app.send("important_stream", record.message)
    else:
        await app.send("normal_stream", record.message)

@pytest.mark.asyncio
async def test_handler_important():
    # Create mock message
    msg = Message(
        topic="input_stream",
        key="123-0",
        data={b"type": b"important", b"data": b"test"},
    )

    # Mock app.send
    with pytest.mock.patch("app.send", new_callable=AsyncMock) as mock_send:
        await my_handler(msg)

        # Verify important_stream was called
        mock_send.assert_called_once()
        args = mock_send.call_args
        assert args[0][0] == "important_stream"

@pytest.mark.asyncio
async def test_handler_normal():
    msg = Message(
        topic="input_stream",
        key="123-0",
        data={b"type": b"normal", b"data": b"test"},
    )

    with pytest.mock.patch("app.send", new_callable=AsyncMock) as mock_send:
        await my_handler(msg)

        args = mock_send.call_args
        assert args[0][0] == "normal_stream"
```

### Testing Storage

```python
import pytest
from streammachine import Storage

@pytest.fixture
def storage():
    """Provide a fresh Storage instance for each test."""
    Storage.reset_instance()
    s = Storage()
    s._ensure_manager()
    yield s
    Storage.reset_instance()

@pytest.mark.asyncio
async def test_storage_write_read(storage):
    await storage.write("key", {"data": "value"})
    result = await storage.read("key")
    assert result == {"data": "value"}

@pytest.mark.asyncio
async def test_storage_default(storage):
    result = await storage.read("nonexistent", default="default")
    assert result == "default"
```

### Testing Time Series Buffer

```python
import pytest
import pandas as pd
from streammachine.models import TimeSeriesBuffer

def test_buffer_append_and_get():
    buffer = TimeSeriesBuffer(max_age_seconds=60)

    df = pd.DataFrame({
        "timestamp_ms": [time.time() * 1000],
        "value": [42],
    })

    buffer.append(df)
    result = buffer.get()

    assert len(result) == 1
    assert result.iloc[0]["value"] == 42
```

## Integration Testing

### Using testcontainers

```python
import pytest
from testcontainers.redis import RedisContainer
from streammachine import RedisConnection

@pytest.fixture(scope="module")
def redis_container():
    """Start Redis container for integration tests."""
    with RedisContainer() as redis:
        yield redis

@pytest.fixture
async def redis_connection(redis_container):
    """Provide Redis connection for each test."""
    conn = RedisConnection(url=redis_container.get_connection_url())
    await conn._ensure_pool()
    yield conn
    await conn.close()

@pytest.mark.asyncio
async def test_redis_connection(redis_connection):
    """Test basic Redis operations."""
    result = await redis_connection.health_check()
    assert result is True

@pytest.mark.asyncio
async def test_stream_operations(redis_connection):
    """Test stream XADD and XREAD."""
    # Add message
    msg_id = await redis_connection.client.xadd(
        "test_stream",
        {"key": "value"}
    )
    assert msg_id is not None

    # Read message
    result = await redis_connection.client.xread(
        {"test_stream": "0-0"},
        count=1
    )
    assert len(result) == 1
```

### Testing Consumer Groups

```python
@pytest.mark.asyncio
async def test_consumer_group(redis_connection):
    """Test consumer group creation and consumption."""
    stream = "test_stream"
    group = "test_group"

    # Add message
    await redis_connection.client.xadd(stream, {"data": "test"})

    # Create consumer
    consumer = await redis_connection.consumer(
        streams=[stream],
        consumer="test_consumer",
        group=group,
        timeout=1000,  # 1 second timeout
    )

    # Consume message
    async for stream_name, entry in consumer:
        assert stream_name == stream.encode()
        assert b"data" in entry.field_values
        break
```

## Mocking

### Mocking Redis

```python
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.mark.asyncio
async def test_with_mocked_redis():
    """Test with mocked Redis client."""
    with patch("streammachine.RedisConnection") as mock_class:
        mock_client = MagicMock()
        mock_client.xadd = AsyncMock(return_value=b"1234567890-0")
        mock_client.xreadgroup = AsyncMock(return_value=[])
        mock_class.return_value.client = mock_client

        # Test your code
        await app.send("stream", {"data": "test"})

        # Verify xadd was called
        mock_client.xadd.assert_called_once()
```

### Mocking Time

```python
from unittest.mock import patch

@pytest.mark.asyncio
async def test_timer_timing():
    """Test that timer respects interval."""
    with patch("time.time") as mock_time:
        mock_time.return_value = 1000.0

        # Test timer logic
        await timer_function()

        # Verify timing behavior
```

## Test Fixtures

### conftest.py

```python
# tests/conftest.py
import asyncio
import pytest
from streammachine import Storage, RedisConnection

@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def storage():
    """Provide fresh Storage instance."""
    Storage.reset_instance()
    s = Storage()
    s._ensure_manager()
    yield s
    Storage.reset_instance()

@pytest.fixture
def sample_message_data():
    """Sample message data for testing."""
    return {
        b"key": b"value",
        b"type": b"test",
    }

@pytest.fixture
async def redis_connection():
    """Provide Redis connection (requires running Redis)."""
    conn = RedisConnection(url="redis://localhost:6379/15")
    await conn._ensure_pool()
    yield conn
    # Cleanup
    await conn.client.flushdb()
    await conn.close()
```

## Test Organization

```
tests/
├── conftest.py              # Shared fixtures
├── test_app.py              # App tests
├── test_redisapi.py         # Redis connection tests
├── test_models.py           # Models and utilities tests
├── test_storage.py          # Storage tests
├── test_integration.py      # Integration tests (require Redis)
├── test_multiprocess.py     # Multiprocess tests
├── test_errors.py           # Error handling tests
└── test_benchmarks.py       # Performance benchmarks
```

## Running Tests

### Unit Tests

```bash
# Run all unit tests
pytest tests/ -v

# Run specific test file
pytest tests/test_models.py -v

# Run specific test
pytest tests/test_models.py::TestMessage::test_message_creation -v
```

### Integration Tests

```bash
# Requires running Redis
RUN_INTEGRATION_TESTS=1 pytest tests/test_integration.py -v

# Or with testcontainers
pytest tests/test_integration.py --redis-container -v
```

### Benchmarks

```bash
# Run benchmarks
pytest tests/test_benchmarks.py --benchmark-only -v
```

### Coverage

```bash
# Run with coverage
pytest tests/ --cov=streammachine --cov-report=html
```

## Best Practices

### Use Descriptive Test Names

```python
# Good
def test_message_latency_calculation_with_valid_timestamps():
    ...

# Bad
def test_message():
    ...
```

### Test Edge Cases

```python
@pytest.mark.asyncio
async def test_storage_nonexistent_key():
    """Reading nonexistent key returns default."""
    result = await storage.read("nonexistent", default=None)
    assert result is None

@pytest.mark.asyncio
async def test_storage_empty_key():
    """Writing empty key should work."""
    await storage.write("", "empty_key_value")
    result = await storage.read("")
    assert result == "empty_key_value"
```

### Test Error Conditions

```python
def test_invalid_config():
    """Invalid config raises ValueError."""
    with pytest.raises(ValueError, match="max_processes must be >= 1"):
        AppConfig(max_processes=0)
```

### Use Test Fixtures

```python
# Define fixture
@pytest.fixture
def sample_stream():
    return [(b"stream", [(b"123-0", {b"key": b"value"})])]

# Use in test
def test_stream_conversion(sample_stream):
    df = streams_to_dataframe(sample_stream)
    assert len(df) == 1
```

### Clean Up Resources

```python
@pytest.fixture
async def redis_connection():
    conn = RedisConnection()
    await conn._ensure_pool()
    yield conn
    # Cleanup
    await conn.client.flushdb()
    await conn.close()
```