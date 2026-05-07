# StreamMachine Examples

This directory contains examples demonstrating various features of StreamMachine.

## Examples

| File | Description |
|------|-------------|
| `basic_usage.py` | Core features: agents, timers, storage, messaging |
| `multiple_consumers.py` | Multiple consumer groups for parallel processing |
| `storage_example.py` | Shared storage between agents and timers |
| `health_check_example.py` | Health checks and graceful shutdown |
| `batch_processing.py` | Efficient batch message sending |
| `benchmark_latency.py` | Message latency benchmarking |
| `benchmark_decode.py` | Cython vs Python decoding performance |

## Running Examples

Make sure you have Redis running locally on the default port (6379).

```bash
# Install the package in development mode
pip install -e .

# Run an example
python examples/basic_usage.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_MAX_CONNECTIONS` | `10` | Max connection pool size |

## Key Concepts

### App
The main application class that manages the event loop and discovers decorated functions.

### Agent
A consumer that processes messages from Redis Streams. Decorate with `@app.agent()`.

### Timer
A periodic task that runs at specified intervals. Decorate with `@app.timer(seconds)`.

### Storage
A shared singleton storage using `multiprocessing.Manager` for state sharing.

### Message
Dataclass containing message metadata and decoded payload.