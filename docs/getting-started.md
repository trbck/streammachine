# Getting Started with StreamMachine

This guide will help you get up and running with StreamMachine quickly.

## Installation

```bash
pip install streammachine
```

For development:

```bash
git clone https://github.com/your-repo/streammachine.git
cd streammachine
pip install -e ".[dev]"
```

## Prerequisites

- Python 3.8+
- Redis 5.0+ (for Streams support)

## Quick Start

### 1. Start Redis

```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or local installation
redis-server
```

### 2. Create Your First App

```python
# app.py
from streammachine import App, Message

app = App(name="hello_world", to_scan=True)

# Timer: Send a message every second
@app.timer(1)
async def producer():
    await app.send("greetings", {"message": "Hello, World!"})

# Agent: Process messages from the stream
@app.agent("greetings", group="hello_group")
async def consumer(record: Message):
    print(f"Received: {record.message}")

if __name__ == "__main__":
    app.start()
```

### 3. Run It

```bash
python app.py
```

You should see:
```
[Producer] Sent greeting message
[Consumer] Received: {'message': 'Hello, World!'}
```

## Core Concepts

### App

The `App` is the main entry point. It manages:
- Event loop (uvloop for performance)
- Redis connections
- Task discovery (finding decorated functions)
- Graceful shutdown

```python
app = App(
    name="my_app",        # Application name
    to_scan=True,         # Scan for decorated tasks
    max_processes=5,      # Process pool size
    max_threads=5,        # Thread pool size
)
```

### Timers (Producers)

Timers run periodically to produce messages:

```python
@app.timer(5)  # Run every 5 seconds
async def my_timer():
    await app.send("stream_name", {"data": "value"})
```

### Agents (Consumers)

Agents consume messages from streams:

```python
@app.agent("stream_name", group="consumer_group")
async def my_agent(record: Message):
    # Process the message
    print(record.message)
```

### Consumer Groups

Groups enable horizontal scaling. Multiple consumers in the same group share the message load:

```python
# Run multiple instances with same group name
# Messages are distributed among them
@app.agent("stream_name", group="workers")
async def worker(record: Message):
    process(record.message)
```

### Concurrency

Run multiple coroutines for the same agent:

```python
@app.agent("stream_name", group="workers", concurrency=3)
async def worker(record: Message):
    # 3 concurrent coroutines processing messages
    process(record.message)
```

## Message Structure

The `Message` object contains:

```python
class Message:
    topic: str           # Stream name
    key: str              # Message ID (stream entry ID)
    sent: float           # Timestamp when sent (if present)
    received: float       # Timestamp when received
    data: dict            # Raw field-values from Redis
    consumer_id: str      # Consumer identifier

    @property
    def message(self) -> dict:
        """Decoded field-values as strings"""
        return {k.decode(): v.decode() for k, v in self.data.items()}
```

## Sending Messages

### Single Message

```python
await app.send("stream_name", {"key": "value"})
```

### Batch Messages

```python
messages = [{"key": f"value_{i}"} for i in range(100)]
await app.send_batch("stream_name", messages)
```

## Shared State

Use `Storage` for cross-agent state:

```python
# Write
await app.storage.write("counter", 0)

# Read
count = await app.storage.read("counter", default=0)

# Increment (read-modify-write)
count = await app.storage.read("counter", default=0)
await app.storage.write("counter", count + 1)

# Check existence
exists = await app.storage.exists("counter")

# List keys
keys = await app.storage.keys()
```

## Error Handling

Wrap handler logic in try/except:

```python
@app.agent("stream_name", group="handlers")
async def handler(record: Message):
    try:
        result = process(record.message)
        await app.send("output", result)
    except ValidationError as e:
        await app.send("errors", {"error": str(e), "original": record.message})
    except Exception as e:
        # Log unexpected errors
        logger.exception(f"Unexpected error: {e}")
```

## Configuration

Environment variables:

```bash
REDIS_URL=redis://localhost:6379/0
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_MAX_CONNECTIONS=10
STREAMMACHINE_DEFAULT_GROUP=eventengine
```

Or programmatically:

```python
from streammachine import RedisConnection

rc = RedisConnection(url="redis://localhost:6379/0", max_connections=50)
```

## Graceful Shutdown

StreamMachine handles SIGINT (Ctrl+C) and SIGTERM automatically:

```python
# Signals trigger:
# 1. Set shutdown event
# 2. Cancel all tasks
# 3. Wait for in-flight messages (10 second timeout)
# 4. Close Redis connections
# 5. Stop event loop
```

## Next Steps

- [Configuration Guide](configuration.md)
- [Scaling Guide](scaling.md)
- [Best Practices](best-practices.md)
- [Architecture Guide](architecture.md)

## Examples

See the `examples/` directory for more:

- `tutorials/` - Step-by-step tutorials
- `patterns/` - Common patterns
- `advanced/` - Advanced usage