# StreamMachine Architecture Guide

This document describes the architecture of StreamMachine, a Redis Streams processing framework for building fast, parallel data pipelines.

## Table of Contents

- [Overview](#overview)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Event Loop and Concurrency](#event-loop-and-concurrency)
- [Decorator Discovery](#decorator-discovery)
- [Consumer Groups](#consumer-groups)
- [Storage and State](#storage-and-state)
- [Performance Considerations](#performance-considerations)

## Overview

StreamMachine is built on top of:
- **Redis Streams** for message queuing
- **coredis** for async Redis client
- **uvloop** for high-performance event loop
- **Venusian** for decorator discovery
- **multiprocessing.Manager** for cross-process state

```
┌─────────────────────────────────────────────────────────────────┐
│                           App                                    │
│  ┌─────────────┐  ┌─────────────────┐  ┌─────────────────────┐  │
│  │  Registry   │  │   Event Loop    │  │   Process/Thread    │  │
│  │ (Venusian)  │  │    (uvloop)     │  │      Pools          │  │
│  └─────────────┘  └─────────────────┘  └─────────────────────┘  │
│         │                  │                      │             │
│         ▼                  ▼                      ▼             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   StreamConsumer                         │   │
│  │        XREADGROUP from consumer group                   │   │
│  │        Message parsing → Handler invocation             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                    │
│                            ▼                                    │
│                ┌─────────────────────┐                          │
│                │    Redis Streams    │                          │
│                │      (coredis)       │                          │
│                └─────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### App

The main application class that manages:
- Event loop creation and lifecycle
- Task discovery and registration
- Signal handling for graceful shutdown
- Resource cleanup

```python
app = App(name="my_app", to_scan=True)

@app.timer(1)
async def producer():
    await app.send("stream", {"data": "value"})

@app.agent("stream", group="consumers")
async def consumer(record: Message):
    print(record.message)

if __name__ == "__main__":
    app.start()
```

### StreamConsumer

Internal class that handles message consumption:
- Creates consumer group on the stream
- Reads messages via XREADGROUP
- Parses messages into `Message` objects
- Invokes handler function
- Handles errors gracefully

### RedisConnection

Async Redis client with connection pooling:
- Lazy connection initialization
- Connection pool management (coredis 6.x)
- Consumer group creation
- Pipeline operations (batch XADD)

### Storage

Cross-process shared state:
- Uses `multiprocessing.Manager` for state sharing
- Async API with per-key locking
- Singleton pattern for consistent state

### TimeSeriesBuffer

In-memory time series storage:
- Automatic pruning of old data
- Configurable max age and max rows
- Pandas DataFrame integration

## Data Flow

### Producer Flow

```
Timer/Agent → app.send(topic, data) → XADD → Redis Stream
```

1. Timer or agent produces data
2. `app.send()` adds timestamp and calls `XADD`
3. Message is stored in Redis Stream
4. Stream ID (timestamp-sequence) is returned

### Consumer Flow

```
Redis Stream → XREADGROUP → StreamConsumer → Message → Handler
```

1. StreamConsumer creates consumer group (if needed)
2. XREADGROUP blocks waiting for messages
3. Message is parsed into `Message` object
4. Handler function is invoked
5. On success, message is auto-acknowledged (configurable)

### Consumer Group Behavior

Multiple consumers in the same group share the message load:

```
                 Stream: "events"
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
    Consumer 1    Consumer 2    Consumer 3
    (Group A)     (Group A)     (Group A)
         │             │             │
    Message 1     Message 2     Message 3
    Message 4     Message 5     Message 6
         │             │             │
         └─────────────┼─────────────┘
                       ▼
                 Processed exactly once
                 across the group
```

Each message is delivered to exactly one consumer in the group. If a consumer fails, its pending messages are picked up by other consumers.

## Event Loop and Concurrency

### uvloop

StreamMachine uses uvloop for improved async performance:

```python
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
```

uvloop provides 2-4x faster event loop operations compared to the default asyncio event loop, which is critical for high-throughput stream processing.

### Concurrency Model

```
┌─────────────────────────────────────────────────────────────────┐
│                      Event Loop (uvloop)                         │
│                                                                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌────────────────────┐ │
│  │ Timer 1 │  │ Timer 2 │  │ Agent 1 │  │ Agent 2 (concurrency│ │
│  │         │  │         │  │         │  │   = 3)              │ │
│  │ (1 sec) │  │ (5 sec) │  │(stream) │  │   ┌────┐┌────┐┌────┐│ │
│  └─────────┘  └─────────┘  └─────────┘  │   │ T1 ││ T2 ││ T3 ││ │
│                                          │   └────┘└────┘└────┘│ │
│                                          └────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

Each timer and agent runs as a separate asyncio task. Agents with `concurrency=N` spawn N concurrent tasks.

### Multiprocess Agents

For CPU-bound work, use `processes=N` to spawn separate processes:

```python
@app.agent("heavy_work", processes=4)
async def cpu_intensive_handler(record: Message):
    # This runs in a separate process
    result = heavy_computation(record.message)
    await app.send("results", result)
```

Each process has:
- Its own event loop
- Its own Redis connection
- Shared Storage via `multiprocessing.Manager`

## Decorator Discovery

StreamMachine uses Venusian for deferred decorator discovery:

```python
import venusian

class AgentTaskDecorator:
    def __init__(self, stream: str, group: str = None):
        self.config = ConsumerConfig("agent", stream, group)

    def __call__(self, wrapped: Callable):
        class Wrapper:
            def __init__(self, wrapped_func):
                self.callback = wrapped

            def on_scan(self, scanner, name, obj):
                # Called during App._discover()
                scanner.registry.add(self.config)

            async def __call__(self, *args, **kwargs):
                return await self.callback(*args, **kwargs)

        w = Wrapper(wrapped)
        venusian.attach(w, w.on_scan)
        return w
```

This allows:
- Clean API: Just add `@app.agent()` decorators
- No circular imports
- Late binding: Decorators scanned on `app.start()`

## Consumer Groups

### Creating Groups

Groups are created automatically when first consumer starts:

```python
@app.agent("stream", group="my_group")
async def handler(record: Message):
    pass
```

First consumer creates group with `XGROUP CREATE stream my_group $ MKSTREAM`.

### Horizontal Scaling

To scale horizontally:

1. Run multiple instances with same group name:
   ```bash
   # Terminal 1
   python app.py  # Instance 1

   # Terminal 2
   python app.py  # Instance 2
   ```

2. Messages are distributed across instances

3. If an instance fails, other instances pick up pending messages

### Message Acknowledgment

By default, messages are auto-acknowledged after processing:

```python
consumer = await rc.consumer(
    streams=["stream"],
    consumer="consumer_1",
    group="my_group",
    auto_acknowledge=True,  # Default
)
```

For manual acknowledgment, set `auto_acknowledge=False` and call `XACK` after processing.

## Storage and State

### Shared State Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Storage (Singleton)                         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              multiprocessing.Manager                       │   │
│  │                     (Separate Process)                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │   │
│  │  │ shared_dict │  │command_queue│  │   sync_manager  │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                      │                      │           │
│         ▼                      ▼                      ▼           │
│  Process 1              Process 2              Process 3        │
│  (Agent)                (Agent)                (Timer)         │
└─────────────────────────────────────────────────────────────────┘
```

### Per-Key Locking

Storage uses per-key asyncio.Lock to prevent concurrent writes:

```python
async def write(self, key: str, value: Any):
    lock = self._get_lock(key)  # Get or create lock for this key
    async with lock:
        self.shared_dict[key] = value
```

This allows concurrent writes to different keys while serializing writes to the same key.

### Read Consistency

By default, reads don't acquire locks for performance:

```python
value = await storage.read("key")  # No lock
```

For strong read consistency, enable `lock_reading`:

```python
storage.lock_reading = True
value = await storage.read("key")  # Acquires lock
```

## Performance Considerations

### Connection Pooling

RedisConnection manages a connection pool:

```python
# Default: 10 connections
RedisConnection(max_connections=10)

# For high throughput:
RedisConnection(max_connections=50)
```

Each concurrent agent task needs a connection. Size the pool for:

```
max_connections >= sum(concurrency) + timers + overhead
```

### Batch Operations

Use `send_batch()` for bulk inserts:

```python
# Slow: Individual sends
for msg in messages:
    await app.send("stream", msg)

# Fast: Batch send
await app.send_batch("stream", messages)
```

Batch sends use Redis pipeline, reducing round trips.

### Time Series Processing

Use `TimeSeriesBuffer` for windowed analysis:

```python
from streammachine.models import TimeSeriesBuffer

buffer = TimeSeriesBuffer(max_age_seconds=300)  # 5 minutes

# Append data
buffer.append(df)

# Get current window
recent = buffer.get()  # Auto-prunes old data
```

### Cython Acceleration

Optional Cython module for faster decoding:

```python
# Install Cython extension
pip install cython
python setup.py build_ext --inplace

# Automatically used when available
from streammachine import streams_to_dataframe_fast
```

## Design Decisions

### Why coredis over redis-py?

- Async-first design
- Type hints throughout
- Better connection pool management
- Built-in stream patterns (GroupConsumer)

### Why multiprocessing.Manager over threading?

- Works across processes for `processes=N` agents
- Thread-safe for async operations
- Simple dict/Queue interface

### Why Venusian for decorator discovery?

- No circular imports
- Clean API (just add decorators)
- Late binding (scan on start)
- Works with any module structure