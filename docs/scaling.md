# Scaling Guide

This guide covers scaling StreamMachine applications horizontally and vertically.

## Vertical Scaling

### Increasing Concurrency

Scale within a single process by increasing `concurrency`:

```python
@app.agent("stream", group="workers", concurrency=10)
async def worker(record: Message):
    await process(record.message)
```

Each agent task runs as a separate coroutine. Increase concurrency until:
- CPU is saturated
- Memory is constrained
- Redis connection pool is exhausted

### Connection Pool Sizing

Match pool size to concurrency:

```python
# For N concurrent tasks, need at least N connections
# Plus overhead for timers and maintenance

total_concurrency = sum(agent.concurrency for agent in agents)
pool_size = total_concurrency + len(timers) + 10  # overhead

app = App(redis_max_connections=pool_size)
```

### Using Multiprocess

For CPU-bound work, use `processes=N`:

```python
@app.agent("stream", group="workers", processes=4)
async def cpu_intensive_worker(record: Message):
    # This runs in a separate process
    result = heavy_computation(record.message)
    await app.send("output", result)
```

Each process has:
- Its own event loop
- Its own Redis connection
- Shared Storage via `multiprocessing.Manager`

## Horizontal Scaling

### Running Multiple Instances

Scale horizontally by running multiple app instances:

```bash
# Terminal 1
python app.py

# Terminal 2
python app.py

# Terminal 3
python app.py
```

All instances share the same consumer group, so messages are distributed among them.

### Load Balancing

Redis Streams automatically load balances within a consumer group:

```
                 Stream: "events"
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   Instance 1    Instance 2    Instance 3
   (Consumer 1)   (Consumer 2)   (Consumer 3)
        │              │              │
   Message 1      Message 2      Message 3
   Message 4      Message 5      Message 6
```

Each message is delivered to exactly one consumer in the group.

### Redis Cluster

For high throughput, use Redis Cluster:

```python
# Configure Redis Cluster
REDIS_URL=redis://cluster-node-1:6379,redis://cluster-node-2:6379
```

StreamMachine works with Redis Cluster out of the box.

## Scaling Patterns

### Fanout Pattern

Multiple consumer groups each receive all messages:

```python
# Analytics pipeline
@app.agent("events", group="analytics")
async def analytics_consumer(record: Message):
    await process_analytics(record.message)

# Audit pipeline
@app.agent("events", group="audit")
async def audit_consumer(record: Message):
    await log_audit(record.message)

# Alert pipeline
@app.agent("events", group="alerts")
async def alert_consumer(record: Message):
    await check_alerts(record.message)
```

Each group processes all messages independently.

### Pipeline Pattern

Chain agents for multi-stage processing:

```python
@app.timer(1)
async def producer():
    await app.send("raw", {"data": "value"})

@app.agent("raw", group="validators")
async def validate(record: Message):
    if valid(record.message):
        await app.send("valid", record.message)

@app.agent("valid", group="transformers")
async def transform(record: Message):
    result = transform_data(record.message)
    await app.send("processed", result)

@app.agent("processed", group="output")
async def output(record: Message):
    await send_to_output(record.message)
```

### Backpressure Handling

Monitor queue depth and throttle producers:

```python
@app.timer(1)
async def smart_producer():
    # Check queue depth before producing
    depth = await get_queue_depth("stream")

    if depth > 1000:
        # Queue is full, slow down
        return

    if depth > 500:
        # Queue is getting full, reduce rate
        await asyncio.sleep(0.1)

    # Normal production
    await app.send("stream", {"data": "value"})
```

## Performance Tuning

### Batch Operations

Use batch sends for high throughput:

```python
# Slow: Individual sends
for msg in messages:
    await app.send("stream", msg)

# Fast: Batch send
await app.send_batch("stream", messages)
```

### Time Series Buffer

Use `TimeSeriesBuffer` for windowed analysis:

```python
from streammachine.models import TimeSeriesBuffer, streams_to_dataframe

buffer = TimeSeriesBuffer(max_age_seconds=60, max_rows=10000)

@app.agent("stream", group="processors")
async def process(record: Message):
    # Convert to DataFrame row
    df = streams_to_dataframe([(b"stream", [(record.key.encode(), record.data)])])
    buffer.append(df)

    # Get recent data (auto-pruned)
    recent = buffer.get()
```

### Cython Acceleration

Install Cython extension for faster decoding:

```bash
pip install cython
python setup.py build_ext --inplace
```

This accelerates `streams_to_dataframe_fast()`.

## Monitoring

### Metrics Collection

```python
@app.timer(10)
async def collect_metrics():
    health = await app.health_check()

    # Track metrics
    await app.storage.write("metrics", {
        "active_tasks": health["active_tasks"],
        "registered_agents": health["registered_agents"],
        "registered_timers": health["registered_timers"],
    })
```

### Queue Depth Monitoring

```python
@app.timer(5)
async def monitor_queue():
    await app.rc._ensure_pool()
    depth = await app.rc.client.xlen("stream")

    if depth > threshold:
        logger.warning(f"Queue depth high: {depth}")
```

## Scaling Checklist

1. **Connection Pool**: Size for concurrency + overhead
2. **Concurrency**: Increase until CPU/memory saturated
3. **Processes**: Use for CPU-bound work
4. **Instances**: Run multiple for horizontal scaling
5. **Groups**: Use for fanout patterns
6. **Batching**: Use `send_batch()` for bulk operations
7. **Monitoring**: Track queue depth, latency, errors