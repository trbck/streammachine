# Configuration Guide

This guide covers StreamMachine configuration options.

## Environment Variables

### Redis Configuration

```bash
# Full connection URL (takes precedence)
REDIS_URL=redis://localhost:6379/0

# Individual connection parameters
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_MAX_CONNECTIONS=10
```

### Application Configuration

```bash
# Default consumer group name
STREAMMACHINE_DEFAULT_GROUP=eventengine

# Default batch size
STREAMMACHINE_RECORDS=10000

# Messages per read
STREAMMACHINE_COUNT=10
```

## Application Configuration

### AppConfig

```python
from streammachine.models import AppConfig

config = AppConfig(
    name="my_app",           # Application name
    to_scan=True,            # Scan for decorated tasks
    max_processes=5,         # Process pool workers
    max_threads=5,           # Thread pool workers
    webserver_port=8000,     # Web server port (future)
    webserver_host="localhost",
    debug=False,
    redis_url="redis://localhost:6379/0",
    redis_max_connections=50,
)

app = App(**config.__dict__)
```

### ConsumerConfig

```python
from streammachine.models import ConsumerConfig

config = ConsumerConfig(
    decorator_type="agent",
    topic="my_stream",
    group="my_group",
    concurrency=3,
    processes=None,      # Or N for multiprocess
    max_retries=3,
    retry_delay_ms=100,
)
```

### TimerConfig

```python
from streammachine.models import TimerConfig

config = TimerConfig(
    decorator_type="timer",
    t=5,  # Interval in seconds
)
```

## Redis Connection

### Connection URL

```python
from streammachine import RedisConnection

# Using URL
rc = RedisConnection(url="redis://localhost:6379/0")

# Using individual parameters
rc = RedisConnection(
    host="localhost",
    port=6379,
    db=0,
    max_connections=50,
)
```

### Connection Pooling

The connection pool is created lazily on first access:

```python
# Pool is created on first use
await rc._ensure_pool()

# Or use context manager
async with RedisConnection() as rc:
    await rc.client.set("key", "value")
```

### Pool Sizing

Size the pool for your workload:

```python
# Minimum for single agent with concurrency=1
max_connections=5

# For multiple agents with concurrency
# max_connections >= sum(concurrency) + timers + overhead
max_connections = sum(concurrency for agent in agents) + 10
```

## Consumer Groups

### Group Name

Consumer groups enable horizontal scaling:

```python
# Same group = messages distributed among consumers
@app.agent("stream", group="workers")
async def worker(record: Message):
    pass
```

### Multiple Groups

Different groups each receive all messages:

```python
# Group 1: Analytics
@app.agent("stream", group="analytics")
async def analytics(record: Message):
    pass

# Group 2: Audit
@app.agent("stream", group="audit")
async def audit(record: Message):
    pass
```

### Consumer Options

```python
consumer = await rc.consumer(
    streams=["stream1", "stream2"],  # Can read from multiple streams
    consumer="consumer_1",            # Unique consumer ID
    group="my_group",
    start_from_backlog=False,        # Start from new messages only
    auto_acknowledge=True,           # Auto-ack after processing
    timeout=5000,                    # Block timeout in milliseconds
)
```

## Storage Configuration

Storage uses `multiprocessing.Manager` for cross-process state:

```python
from streammachine import Storage

storage = Storage()

# Enable read locking for strong consistency
storage.lock_reading = True

# Write with key-specific locking
await storage.write("key", {"data": "value"})

# Read (with or without lock based on lock_reading)
value = await storage.read("key", default=None)
```

## Logging

Configure logging at application startup:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Adjust StreamMachine log level
logging.getLogger("streammachine").setLevel(logging.DEBUG)
```

## Production Configuration

### Recommended Settings

```python
# For production use
config = AppConfig(
    name="production_app",
    to_scan=True,
    max_processes=4,           # Match CPU cores for CPU-bound work
    max_threads=4,             # For blocking I/O
    redis_max_connections=50,  # Size for your concurrency
    debug=False,
)
```

### Environment-Specific Config

```python
import os

# Development
if os.environ.get("ENV") == "development":
    config = AppConfig(
        name="dev_app",
        debug=True,
        redis_url="redis://localhost:6379/0",
    )

# Production
else:
    config = AppConfig(
        name="prod_app",
        debug=False,
        redis_url=os.environ["REDIS_URL"],
        redis_max_connections=int(os.environ.get("REDIS_MAX_CONNECTIONS", 50)),
    )
```

## Health Checks

```python
@app.timer(30)
async def health_check():
    health = await app.health_check()
    # Returns:
    # {
    #     "status": "healthy" | "degraded",
    #     "redis": "connected" | "disconnected",
    #     "active_tasks": N,
    #     "registered_agents": N,
    #     "registered_timers": N,
    # }
    print(f"Health: {health}")
```

## Validation

Configuration is validated on creation:

```python
from streammachine.models import AppConfig, ConsumerConfig

# Invalid values raise ValueError
try:
    config = AppConfig(max_processes=0)  # Error: must be >= 1
except ValueError as e:
    print(f"Invalid config: {e}")

try:
    config = ConsumerConfig(
        decorator_type="agent",
        topic="stream",
        concurrency=0,  # Error: must be >= 1
    )
except ValueError as e:
    print(f"Invalid config: {e}")
```