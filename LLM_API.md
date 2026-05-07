# StreamMachine - LLM API Context

> This file provides comprehensive API documentation for LLMs to understand and use StreamMachine in other projects.

## Overview

StreamMachine is a high-performance, async-first Python framework for distributed stream processing using Redis Streams. It provides a decorator-based API for registering stream consumers (agents) and periodic tasks (timers), with built-in support for multiprocessing-safe shared state.

**Core Use Case:** Build event-driven applications that consume from Redis Streams, process messages, and optionally emit new messages to other streams.

## Installation

```bash
pip install streammachine

# Optional extras
pip install streammachine[cython]      # Cython-accelerated decoding
pip install streammachine[fast-json]    # ujson, orjson
pip install streammachine[monitoring]   # structlog, prometheus-client
pip install streammachine[all]          # All optional dependencies
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `REDIS_HOST` | `localhost` | Redis host (fallback) |
| `REDIS_PORT` | `6379` | Redis port (fallback) |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_MAX_CONNECTIONS` | `10` | Max connection pool size |
| `STREAMMACHINE_RECORDS` | `10000` | Default record count |
| `STREAMMACHINE_COUNT` | `10` | Messages per XREAD call |
| `STREAMMACHINE_DEFAULT_GROUP` | `eventengine` | Default consumer group |

---

## Public API

### Core Imports

```python
from streammachine import (
    # Core classes
    App,
    StreamConsumer,
    Message,

    # Configuration
    AppConfig,
    ConsumerConfig,
    TimerConfig,

    # Storage
    Storage,

    # Redis
    RedisConnection,

    # DataFrame utilities
    streams_to_dataframe,
    streams_to_dataframe_fast,
    prune_old_dataframe_rows,
    TimeSeriesBuffer,

    # Fast OHLC (optional, Python fallback always available)
    FastOHLC,
    FastOHLCConsumer,
    create_ohlc_aggregator,
    parse_stream_id_timestamp,
    format_candle_for_redis,
    _HAS_FAST_OHLC_CYTHON,

    # Optional (may be None if extras not installed)
    RedisObjectStorage,
    decode_dict_bytes_to_utf8,
    _has_cython_decode,
)
```

---

## App Class

The main entry point for StreamMachine applications.

### Constructor

```python
App(
    name: str = __name__,           # Application name for logging
    to_scan: bool = True,           # Auto-discover decorated tasks
    max_processes: int = 5,         # ProcessPoolExecutor workers
    max_threads: int = 5,           # ThreadPoolExecutor workers
)
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `start` | `def start(self) -> None` | Start the application event loop. Blocking call. |
| `send` | `async def send(self, topic: str, record: dict) -> Any` | Send a single record to a Redis stream. |
| `send_batch` | `async def send_batch(self, topic: str, records: List[dict]) -> List` | Batch send multiple records to a stream. |
| `shutdown` | `async def shutdown(self) -> None` | Gracefully shutdown all running tasks. |
| `health_check` | `async def health_check(self) -> dict` | Return health status dict. |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `storage` | `Storage` | Shared multiprocessing-safe storage instance. |
| `redis` | `RedisConnection` | Redis connection manager. |

---

## @app.agent Decorator

Register a stream consumer (agent) that processes messages from a Redis Stream.

### Signature

```python
@app.agent(
    stream: str,                    # Stream name to consume from
    group: str = "eventengine",     # Consumer group name
    concurrency: int = 1,           # Number of concurrent consumers
    processes: Optional[int] = None # Optional: use multiprocessing workers
)
async def my_agent(record: Message) -> None:
    ...
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream` | `str` | required | Redis Stream name to consume from |
| `group` | `str` | `"eventengine"` | Consumer group name (multiple groups get same messages) |
| `concurrency` | `int` | `1` | Number of concurrent consumer tasks |
| `processes` | `int \| None` | `None` | If set, use multiprocessing instead of asyncio |

### Usage

```python
from streammachine import App, Message

app = App(name="my_app")

@app.agent("input_stream", group="workers", concurrency=2)
async def process_messages(record: Message):
    # Access decoded message
    data = record.message  # Dict[str, str]

    # Access timing info
    latency = record.timer  # "topic: task X.XX ms"

    # Send to another stream
    await app.send("output_stream", {"processed": "true"})
```

---

## @app.timer Decorator

Register a periodic task that runs at fixed intervals.

### Signature

```python
@app.timer(
    t: int  # Interval in seconds
)
async def my_timer() -> None:
    ...
```

### Usage

```python
@app.timer(5)  # Run every 5 seconds
async def periodic_task():
    await app.send("heartbeat", {"ts": time.time()})
```

---

## Message Dataclass

Received message from a Redis Stream.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | `str \| None` | Stream name the message came from |
| `key` | `str \| None` | Redis message ID |
| `sent` | `float \| None` | Timestamp when sent (if included in message) |
| `received` | `float \| None` | Timestamp when received |
| `consumer_id` | `str \| None` | Consumer ID that processed the message |
| `data` | `Tuple[str, Dict] \| None` | Raw message data |

### Properties

| Property | Return Type | Description |
|----------|-------------|-------------|
| `message` | `Dict[str, str]` | Decoded message dict (bytes keys/values → utf-8 strings) |
| `timer` | `str` | Latency string: `"topic: task X.XX ms"` |

---

## Storage Class

Singleton async storage using `multiprocessing.Manager` for shared state across processes.

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `write` | `async def write(self, key: str, value: Any) -> None` | Write key-value pair with per-key locking |
| `read` | `async def read(self, key: str, default: Any = None) -> Any` | Read value from storage |
| `delete` | `async def delete(self, key: str) -> bool` | Delete key, return True if existed |
| `exists` | `async def exists(self, key: str) -> bool` | Check if key exists |
| `keys` | `async def keys(self) -> list` | Get all keys |
| `clear` | `async def clear(self) -> None` | Clear all keys |
| `reset_instance` | `classmethod def reset_instance(cls) -> None` | Reset singleton (for testing) |

### Usage

```python
# Read/write shared state
count = await app.storage.read("counter", default=0)
await app.storage.write("counter", count + 1)

# Check existence
if await app.storage.exists("config"):
    config = await app.storage.read("config")
```

---

## RedisConnection Class

Async Redis connection manager with connection pooling.

### Constructor

```python
RedisConnection(
    host: Optional[str] = None,     # Redis host (env: REDIS_HOST)
    port: Optional[int] = None,      # Redis port (env: REDIS_PORT)
    db: Optional[int] = None,        # Redis DB (env: REDIS_DB)
    max_connections: Optional[int] = None,  # Pool size (env: REDIS_MAX_CONNECTIONS)
    url: Optional[str] = None,      # Full Redis URL (env: REDIS_URL)
)
```

### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `consumer` | `async def consumer(...)` | Create a Redis Stream group consumer |
| `pipeline_xadd` | `async def pipeline_xadd(self, topic: str, records: List[dict]) -> List` | Batch add records using pipeline |
| `health_check` | `async def health_check(self) -> bool` | Check Redis connection health |
| `close` | `async def close(self) -> None` | Close connection pool |

### Context Manager

```python
async with RedisConnection() as redis:
    await redis.pipeline_xadd("stream", [{"key": "value"}])
```

---

## DataFrame Utilities

### streams_to_dataframe

Convert Redis XREAD/XREADGROUP output to pandas DataFrame.

```python
from streammachine import streams_to_dataframe

# streams is Redis XREAD output format
df = streams_to_dataframe(
    streams,
    stream_name_column: str = "stream",
    id_column: str = "id",
    timestamp_column: str = "timestamp_ms",
    include_sequence: bool = False,
)
```

### streams_to_dataframe_fast

Optimized version for maximum throughput.

```python
df = streams_to_dataframe_fast(
    streams,
    stream_name_column: str = "stream",
    id_column: str = "id",
    timestamp_column: str = "timestamp_ms",
)
```

### prune_old_dataframe_rows

Remove rows older than cutoff_seconds.

```python
from streammachine import prune_old_dataframe_rows

df_fresh = prune_old_dataframe_rows(
    df,
    cutoff_seconds: float,
    timestamp_column: str = "timestamp_ms",
    current_time: Optional[float] = None,
)
```

### TimeSeriesBuffer

In-memory buffer with automatic time-based pruning.

```python
from streammachine import TimeSeriesBuffer

buffer = TimeSeriesBuffer(
    max_age_seconds: float,      # Keep data newer than this
    timestamp_column: str = "timestamp_ms",
    max_rows: Optional[int] = None,  # Optional row limit
)

buffer.append(df)                # Add DataFrame rows
df = buffer.get()                # Get all buffered data
buffer.clear()                   # Clear buffer
count = len(buffer)              # Number of rows
ts = buffer.last_timestamp       # Last timestamp seen
```

---

## Optional Components

### RedisObjectStorage

Async Redis object storage with pickle serialization.

```python
from streammachine import RedisObjectStorage  # Requires optional deps

storage = RedisObjectStorage()

# Store any Python object
await storage.store_with_pickle("my_key", {"complex": ["object", 123]})

# Retrieve it
obj = await storage.retrieve_with_pickle("my_key")

# List/delete by pattern
keys = await storage.list_keys("prefix:*")
count = await storage.delete_keys("prefix:*")
```

### Cython Decoding

Ultra-fast byte decoding for high-throughput scenarios.

```python
from streammachine import decode_dict_bytes_to_utf8, _has_cython_decode

if _has_cython_decode:
    # Use Cython-accelerated decoder
    decoded = decode_dict_bytes_to_utf8(raw_bytes_dict)
```

---

## Fast OHLC Aggregation

High-performance OHLC (Open-High-Low-Close) candle aggregation for real-time market data.

### Features

- **Cython acceleration** when compiled (falls back to pure Python)
- **Zero-copy parsing** from Redis stream output
- **C-level candle storage** for minimal overhead
- **Multiple interval support** (1min, 5min, 15min, etc.)
- **Sub-millisecond latency** with Cython

### Factory Function

```python
from streammachine import create_ohlc_aggregator

# Create aggregator with default intervals (1min, 5min)
agg = create_ohlc_aggregator()

# Or specify custom intervals
agg = create_ohlc_aggregator(intervals=[60000, 300000, 900000])  # 1min, 5min, 15min
```

### FastOHLC Class

```python
from streammachine import FastOHLC

agg = FastOHLC(intervals=[60000, 300000])  # 1min, 5min candles

# Single tick update
agg.update_tick(
    symbol=b"AAPL",       # Symbol as bytes
    price=150.25,          # Trade price
    volume=1000.0,         # Trade volume
    timestamp_ms=1638360000000  # Unix timestamp in milliseconds
)

# Get candles
candles = agg.get_candles(b"AAPL", 60000)
for c in candles:
    print(f"O={c.open} H={c.high} L={c.low} C={c.close} V={c.volume}")

# Get completed candles (ready to emit)
completed = agg.get_completed_candles(b"AAPL", 60000)

# Flush completed candles from memory
agg.flush_interval(b"AAPL", 60000)

# Get candles as dictionaries (for serialization)
dicts = agg.get_candles_as_dicts(b"AAPL", 60000)

# Process Redis stream batch directly
count = agg.process_stream_batch(
    entries,               # XREADGROUP output
    price_field="price",
    volume_field="volume"
)

# Clear all data
agg.clear()

# Check tick count
print(f"Processed {agg.tick_count} ticks")
```

### Candle Properties

Each candle object has these properties:

| Property | Type | Description |
|----------|------|-------------|
| `open` | `float` | Opening price (first tick) |
| `high` | `float` | Highest price in interval |
| `low` | `float` | Lowest price in interval |
| `close` | `float` | Closing price (last tick) |
| `volume` | `float` | Total volume |
| `timestamp_ms` | `int` | Last tick timestamp |
| `candle_start_ms` | `int` | Start of interval |
| `trade_count` | `int` | Number of trades |

### Utility Functions

```python
from streammachine import parse_stream_id_timestamp, format_candle_for_redis

# Parse timestamp from Redis stream ID
ts = parse_stream_id_timestamp("1638360000000-0")  # Returns 1638360000000

# Format candle for Redis XADD
data = format_candle_for_redis(candle, b"AAPL", 60000)
# Returns: {"symbol": "AAPL", "interval_ms": "60000", "open": "100.5", ...}
```

### High-Level Consumer

For real-time streaming with automatic candle emission:

```python
from streammachine import App, FastOHLCConsumer

app = App(name="ohlc_realtime")

consumer = FastOHLCConsumer(
    input_stream="ticks",
    output_stream_prefix="candles",  # Creates candles_1m, candles_5m, etc.
    intervals=[60000, 300000],        # 1min, 5min candles
    group="ohlc_workers",
    price_field="price",
    volume_field="volume",
)

@app.on_startup
async def start_consumer():
    await consumer.start()

@app.on_shutdown
async def stop_consumer():
    await consumer.stop()

if __name__ == "__main__":
    app.start()
```

### Performance

| Implementation | Ticks/sec | Latency | Memory |
|----------------|-----------|---------|--------|
| Python fallback | ~50k/sec | <20µs/tick | Low |
| Cython compiled | ~500k/sec | <5µs/tick | Minimal |

### Example: Real-Time OHLC Aggregation

```python
from streammachine import App, Message, create_ohlc_aggregator

app = App(name="market_data", dashboard_enabled=True)
agg = create_ohlc_aggregator(intervals=[60000, 300000, 900000])

@app.agent("ticks", group="ohlc_workers")
async def process_ticks(record: Message):
    """Process tick data and aggregate into OHLC candles."""
    msg = record.message

    # Parse tick data
    symbol = msg.get("symbol", "UNKNOWN").encode('utf-8')
    price = float(msg.get("price", 0))
    volume = float(msg.get("volume", 0))

    # Extract timestamp from stream ID
    ts_ms = int(record.key.split('-')[0])

    # Update candles
    agg.update_tick(symbol, price, volume, ts_ms)

    # Periodically emit completed candles
    # (In production, use a timer task)
    for interval in [60000, 300000, 900000]:
        completed = agg.get_completed_candles(symbol, interval)
        for candle in completed:
            await app.send(f"candles_{interval//60000}m", {
                "symbol": symbol.decode('utf-8'),
                "interval_ms": str(interval),
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
                "trade_count": str(candle.trade_count),
            })
        if completed:
            agg.flush_interval(symbol, interval)

if __name__ == "__main__":
    app.start()
```

---

## Complete Usage Examples

### Basic Producer-Consumer

```python
from streammachine import App, Message

app = App(name="basic_example")

@app.timer(1)
async def producer():
    """Send a message every second."""
    await app.send("work_queue", {"task_id": "123", "status": "pending"})

@app.agent("work_queue", group="workers")
async def consumer(record: Message):
    """Process messages from work_queue."""
    print(f"Processing: {record.message}")
    await app.send("results", {"task_id": record.message["task_id"], "status": "done"})

if __name__ == "__main__":
    app.start()
```

### Pipeline (Chained Processing)

```python
from streammachine import App, Message

app = App(name="pipeline_example")

@app.timer(1)
async def source():
    await app.send("stage1", {"value": 10})

@app.agent("stage1", group="s1")
async def stage1(record: Message):
    value = int(record.message["value"])
    await app.send("stage2", {"value": value * 2})

@app.agent("stage2", group="s2")
async def stage2(record: Message):
    value = int(record.message["value"])
    print(f"Final result: {value}")

if __name__ == "__main__":
    app.start()
```

### Shared State

```python
from streammachine import App, Message

app = App(name="stateful_example")

@app.timer(1)
async def counter():
    count = await app.storage.read("counter", default=0)
    count += 1
    await app.storage.write("counter", count)
    await app.send("counts", {"count": count})

@app.agent("counts", group="loggers")
async def logger(record: Message):
    stored = await app.storage.read("counter")
    print(f"Current count: {stored}, received: {record.message}")

if __name__ == "__main__":
    app.start()
```

### Multiple Consumer Groups (Fan-Out)

```python
from streammachine import App, Message

app = App(name="fanout_example")

@app.timer(1)
async def producer():
    await app.send("events", {"type": "user_action", "user_id": "123"})

# Both groups receive ALL messages
@app.agent("events", group="analytics")
async def analytics(record: Message):
    print(f"Analytics: {record.message}")

@app.agent("events", group="audit")
async def audit(record: Message):
    print(f"Audit log: {record.message}")

if __name__ == "__main__":
    app.start()
```

### Time Series Analytics

```python
from streammachine import App, Message, TimeSeriesBuffer, streams_to_dataframe

app = App(name="timeseries_example")
buffer = TimeSeriesBuffer(max_age_seconds=300)  # 5 minutes

@app.agent("ticks", group="analytics")
async def analyze(record: Message):
    # Accumulate recent data
    # ... convert to DataFrame and append to buffer

    recent_df = buffer.get()
    if len(recent_df) > 0:
        # Run analytics on recent window
        mean_val = recent_df["value"].mean()
        print(f"Rolling mean: {mean_val}")

if __name__ == "__main__":
    app.start()
```

### Health Monitoring

```python
from streammachine import App, Message

app = App(name="health_example")

@app.timer(30)
async def health_monitor():
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

if __name__ == "__main__":
    app.start()
```

---

## Architecture Notes

### Event Loop

- Uses `uvloop` for high-performance async I/O
- Each agent runs as an async task within the same event loop
- Timer tasks are scheduled with fixed intervals

### Consumer Groups

- Redis Streams consumer groups enable horizontal scaling
- Each consumer group maintains its own offset
- Multiple groups receive copies of all messages (fan-out pattern)
- Within a group, messages are distributed across consumers (load balancing)

### Multiprocessing

- Set `processes=N` in `@app.agent()` to use process workers
- Use `app.storage` for shared state across processes
- Storage uses `multiprocessing.Manager` for IPC

### Graceful Shutdown

- SIGINT/SIGTERM triggers graceful shutdown
- Active consumers complete current messages
- Redis connections are properly closed

---

## Dependencies

- **coredis**: Async Redis client (Redis 6+ compatible)
- **uvloop**: High-performance event loop
- **venusian**: Decorator discovery
- **pandas**: DataFrame utilities
- **numpy**: Numerical operations
- **redis**: Sync Redis client (for some operations)

---

## MCP Server

StreamMachine includes an optional MCP (Model Context Protocol) server that exposes its functionality as tools for LLM-powered applications.

### Installation

```bash
pip install streammachine[mcp]
```

### Running the MCP Server

```bash
# Command line (stdio server)
streammachine-mcp

# Or as a Python module
python -m streammachine.mcp_server

# Or using __main__.py
python -m streammachine
```

### Testing the MCP Server

```bash
# Run the test client (validates all handlers)
python tests/test_mcp_client.py

# Use MCP Inspector with FastMCP version
mcp dev src/streammachine/mcp_fast.py --with-editable .
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "streammachine": {
      "command": "streammachine-mcp"
    }
  }
}
```

Or with explicit Python path:

```json
{
  "mcpServers": {
    "streammachine": {
      "command": "python",
      "args": ["-m", "streammachine.mcp_server"]
    }
  }
}
```

### Available MCP Tools

#### Stream Tools

| Tool | Description |
|------|-------------|
| `stream_send` | Send a message to a Redis stream |
| `stream_send_batch` | Send multiple messages in a batch |
| `stream_read` | Read messages from a stream |
| `stream_info` | Get stream metadata (length, groups, etc.) |
| `stream_list` | List all streams matching a pattern |

#### Storage Tools

| Tool | Description |
|------|-------------|
| `storage_read` | Read a value from shared storage |
| `storage_write` | Write a value to shared storage |
| `storage_delete` | Delete a key from storage |
| `storage_keys` | List all storage keys |
| `storage_clear` | Clear all storage (requires confirmation) |

#### Health Tools

| Tool | Description |
|------|-------------|
| `health_check` | Check Redis connection health |
| `redis_info` | Get detailed Redis server info |
| `redis_ping` | Ping Redis server |

#### Object Storage Tools

| Tool | Description |
|------|-------------|
| `obj_get` | Retrieve a pickled object from Redis |
| `obj_list` | List object storage keys |
| `obj_delete` | Delete objects by pattern |

#### OHLC Aggregation Tools

| Tool | Description |
|------|-------------|
| `ohlc_create` | Create an OHLC aggregator for candle aggregation |
| `ohlc_update` | Update aggregator with a new tick (trade data) |
| `ohlc_get_candles` | Get all candles for a symbol and interval |
| `ohlc_get_completed` | Get completed candles ready to emit |
| `ohlc_flush` | Remove completed candles from memory |
| `ohlc_clear` | Clear all data from an aggregator |
| `ohlc_stats` | Get aggregator statistics |
| `ohlc_list` | List all OHLC aggregators |

### MCP Resources

The server exposes two resources:

| Resource | URI | Description |
|----------|-----|-------------|
| Config | `streammachine://config` | Current configuration and environment variables |
| Status | `streammachine://status` | Current Redis connection and storage status |

### MCP Prompts

Built-in prompts for common tasks:

| Prompt | Description |
|--------|-------------|
| `streammachine-guide` | Interactive guide for using StreamMachine |
| `stream-processing-patterns` | Common patterns and implementations |

### Example MCP Usage

When connected via MCP, an LLM can:

```
# List available streams
Tool: stream_list
Arguments: {"pattern": "*"}

# Send a message
Tool: stream_send
Arguments: {
  "stream": "events",
  "message": {"type": "user_action", "user_id": "123"}
}

# Read from storage
Tool: storage_read
Arguments: {"key": "counter"}

# Write to storage
Tool: storage_write
Arguments: {"key": "counter", "value": 42}

# Check health
Tool: health_check
Arguments: {}
```

### Example OHLC Usage via MCP

```
# Create an OHLC aggregator
Tool: ohlc_create
Arguments: {
  "name": "market_data",
  "intervals": [60000, 300000]  # 1min, 5min
}

# Update with tick data
Tool: ohlc_update
Arguments: {
  "name": "market_data",
  "symbol": "AAPL",
  "price": 150.25,
  "volume": 1000
}

# Get candles
Tool: ohlc_get_candles
Arguments: {
  "name": "market_data",
  "symbol": "AAPL",
  "interval_ms": 60000
}

# Get completed candles (ready to emit)
Tool: ohlc_get_completed
Arguments: {
  "name": "market_data",
  "symbol": "AAPL",
  "interval_ms": 60000
}

# Flush completed candles from memory
Tool: ohlc_flush
Arguments: {
  "name": "market_data",
  "symbol": "AAPL",
  "interval_ms": 60000
}

# Get statistics
Tool: ohlc_stats
Arguments: {"name": "market_data"}
```

---

## Integration Checklist

When integrating StreamMachine into another project:

1. **Install package**: `pip install streammachine[all]`
2. **Set environment variables**: Configure `REDIS_URL` or individual Redis settings
3. **Create App instance**: `app = App(name="my_app")`
4. **Define agents**: Use `@app.agent()` decorator for stream consumers
5. **Define timers**: Use `@app.timer()` decorator for periodic tasks
6. **Start application**: Call `app.start()` in `if __name__ == "__main__":` block
7. **Handle signals**: Graceful shutdown is automatic, but you can call `await app.shutdown()` manually

### MCP Integration (for LLM applications)

To expose StreamMachine to LLM applications:

1. **Install with MCP support**: `pip install streammachine[mcp]`
2. **Run the MCP server**: `streammachine-mcp` or `python -m streammachine.mcp_server`
3. **Configure your LLM client**: Add the server to your MCP client configuration
4. **Use tools**: The LLM can now send messages, read storage, check health, etc.

---

## Common Patterns

| Pattern | Use Case |
|---------|----------|
| Producer-Consumer | Task queue processing |
| Pipeline | Multi-stage data transformation |
| Fan-Out | Multiple independent consumers need same data |
| Time Series | Real-time analytics with rolling windows |
| Stateful | Accumulate state across messages (counters, aggregations) |