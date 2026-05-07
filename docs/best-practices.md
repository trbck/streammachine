# Best Practices

This guide covers best practices for building reliable, performant StreamMachine applications.

## Error Handling

### Always Handle Exceptions

```python
@app.agent("stream", group="handlers")
async def handler(record: Message):
    try:
        await process(record.message)
    except ValidationError as e:
        # Expected errors - send to error stream
        await app.send("errors", {"error": str(e), "original": record.message})
    except Exception as e:
        # Unexpected errors - log and send to DLQ
        logger.exception(f"Unexpected error processing {record.key}")
        await app.send("dead_letter", {"error": str(e), "original": record.message})
```

### Use Dead Letter Queues

```python
@app.agent("errors", group="error_handlers")
async def handle_errors(record: Message):
    # Log error
    logger.error(f"Error: {record.message}")

    # Store for analysis
    await store_error(record.message)

    # Optionally retry after delay
    await asyncio.sleep(60)
    await app.send("stream", record.message.get("original"))
```

### Validate Early

```python
def validate_message(msg: dict) -> None:
    """Validate message structure."""
    required = ["id", "type", "data"]
    for field in required:
        if field not in msg:
            raise ValueError(f"Missing required field: {field}")

@app.agent("stream", group="handlers")
async def handler(record: Message):
    try:
        validate_message(record.message)
        await process(record.message)
    except ValueError as e:
        await app.send("errors", {"error": str(e)})
```

## Message Design

### Keep Messages Flat

Redis Streams values must be flat scalars:

```python
# Good: Flat values
await app.send("stream", {
    "id": "123",
    "type": "event",
    "data": json.dumps({"nested": "data"}),
    "timestamp": str(time.time()),
})

# Bad: Nested values (not supported)
await app.send("stream", {
    "id": "123",
    "data": {"nested": "data"},  # Won't work!
})
```

### Include Timestamps

```python
await app.send("stream", {
    "data": "value",
    "timestamp": str(time.time()),
    "source": "producer_name",
})
```

### Use Consistent Key Names

```python
# Consistent naming
await app.send("stream", {
    "event_id": "123",      # Always "event_id"
    "event_type": "click",  # Always "event_type"
    "user_id": "user_456",  # Always "user_id"
})
```

## State Management

### Atomic Updates

```python
async def atomic_increment(key: str, amount: int = 1) -> int:
    """Atomically increment a counter."""
    # Note: For true atomic operations, use Redis INCR
    # This is a conceptual example
    lock = app.storage._get_lock(key)
    async with lock:
        value = await app.storage.read(key, default=0)
        value += amount
        await app.storage.write(key, value)
        return value
```

### Use Separate Keys for Different Data

```python
# Good: Separate keys
await app.storage.write(f"user:{user_id}:profile", profile)
await app.storage.write(f"user:{user_id}:settings", settings)
await app.storage.write(f"user:{user_id}:last_seen", timestamp)

# Bad: Single key with nested data
await app.storage.write(f"user:{user_id}", {
    "profile": profile,
    "settings": settings,
    "last_seen": timestamp,
})
```

### Cleanup Old Data

```python
@app.timer(3600)  # Every hour
async def cleanup():
    keys = await app.storage.keys()
    for key in keys:
        # Check if data is old
        timestamp = await app.storage.read(f"{key}:timestamp", default=0)
        if time.time() - timestamp > 86400:  # 24 hours
            await app.storage.delete(key)
```

## Performance

### Use Batch Operations

```python
# Slow: Individual sends
for msg in messages:
    await app.send("stream", msg)

# Fast: Batch send
await app.send_batch("stream", messages)
```

### Avoid Blocking Operations

```python
# Bad: Blocking operation in handler
@app.agent("stream", group="handlers")
async def handler(record: Message):
    result = blocking_io_operation()  # Blocks event loop!
    await app.send("output", result)

# Good: Use thread pool for blocking I/O
@app.agent("stream", group="handlers")
async def handler(record: Message):
    result = await asyncio.to_thread(blocking_io_operation)
    await app.send("output", result)
```

### Size Connection Pool Appropriately

```python
# Calculate pool size
total_concurrency = sum(agent.concurrency for agent in agents)
pool_size = total_concurrency + len(timers) + 10  # overhead

app = App(redis_max_connections=pool_size)
```

## Idempotency

### Design Idempotent Handlers

```python
@app.agent("stream", group="handlers")
async def handler(record: Message):
    msg = record.message
    event_id = msg.get("event_id")

    # Check if already processed
    if await app.storage.exists(f"processed:{event_id}"):
        return  # Skip

    # Process
    await process(msg)

    # Mark as processed
    await app.storage.write(f"processed:{event_id}", True)
```

### Use Message IDs

```python
@app.agent("stream", group="handlers")
async def handler(record: Message):
    # Redis stream IDs are unique and ordered
    # Use as idempotency key
    message_id = record.key

    # Check if processed
    if await already_processed(message_id):
        return

    await process(record.message)
    await mark_processed(message_id)
```

## Message Ordering

### Order Within Partition

Messages are ordered within a stream, but consumers may process out of order:

```python
# If order matters, include sequence number
await app.send("stream", {
    "data": "value",
    "sequence": next_sequence(),
})
```

### Consumer Groups and Ordering

Within a consumer group, messages with the same key go to the same consumer:

```python
# Redis Streams doesn't support key-based routing
# If you need ordering, use a single consumer or external ordering
```

## Testing

### Mock Redis for Unit Tests

```python
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture
def mock_redis():
    mock = MagicMock()
    mock.xadd = AsyncMock(return_value=b"1234567890-0")
    mock.xreadgroup = AsyncMock(return_value=[])
    return mock

@pytest.mark.asyncio
async def test_handler(mock_redis):
    # Test handler logic without real Redis
    pass
```

### Integration Tests with Real Redis

```python
import pytest
from testcontainers.redis import RedisContainer

@pytest.fixture
def redis_container():
    with RedisContainer() as redis:
        yield redis

@pytest.mark.asyncio
async def test_end_to_end(redis_container):
    # Test with real Redis
    app = App(redis_url=redis_container.get_connection_url())
    # ...
```

## Logging

### Use Structured Logging

```python
import logging
import json

logger = logging.getLogger(__name__)

@app.agent("stream", group="handlers")
async def handler(record: Message):
    logger.info(json.dumps({
        "event": "message_processed",
        "message_id": record.key,
        "topic": record.topic,
        "latency_ms": (record.received - record.sent) * 1000,
    }))
```

### Log Levels

```python
# Use appropriate log levels
logger.debug("Detailed information for debugging")
logger.info("Normal operation events")
logger.warning("Unexpected but handled situations")
logger.error("Errors that need attention")
logger.critical("System-wide failures")
```

## Security

### Validate Input

```python
def validate_user_id(user_id: str) -> bool:
    # Validate format
    if not user_id.startswith("user_"):
        return False
    if len(user_id) > 50:
        return False
    return True

@app.agent("stream", group="handlers")
async def handler(record: Message):
    user_id = record.message.get("user_id")
    if not validate_user_id(user_id):
        raise ValueError(f"Invalid user_id: {user_id}")
```

### Sanitize Data

```python
import html

def sanitize(data: dict) -> dict:
    """Sanitize string values to prevent injection."""
    return {
        k: html.escape(str(v)) if isinstance(v, str) else v
        for k, v in data.items()
    }
```

### Redis Authentication

```python
# Use Redis ACLs
REDIS_URL=redis://:password@localhost:6379/0

# Or configure ACLs
# > ACL SETUSER streammachine_user on +@stream +@connection +@string >password
```