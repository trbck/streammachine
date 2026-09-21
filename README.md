# StreamMachine

Async stream processing on [Redis Streams](https://redis.io/docs/data-types/streams/) for Python.
Register consumers and periodic producers with decorators, run them on one event loop,
and scale out by starting more processes in the same consumer group.

```python
from streammachine import App, Message

app = App(name="demo")

@app.timer(1)
async def producer():
    await app.send("greetings", {"message": "hello"})

@app.agent("greetings", group="greeters")
async def consumer(record: Message):
    print("received:", record.message)

if __name__ == "__main__":
    app.start()
```

## Features

- **Agents and timers**: `@app.agent(stream, group=...)` consumes a stream through a Redis consumer group, `@app.timer(seconds)` runs a coroutine periodically.
- **Resilient consumers**: handler errors are logged and skipped; Redis outages reconnect with bounded exponential backoff under a stable consumer name so pending entries are never orphaned.
- **Bounded streams**: `App(stream_maxlen=...)` or `STREAMMACHINE_STREAM_MAXLEN` trims every produced stream with approximate `MAXLEN`.
- **Shared state**: `app.storage` is a `multiprocessing.Manager` backed key/value store with per-key async locks.
- **DataFrames**: `streams_to_dataframe()` and `TimeSeriesBuffer` turn raw `XREAD` output into pandas frames with automatic pruning.
- **OHLC aggregation**: `create_ohlc_aggregator()` builds candles from tick streams, with an optional Cython build for higher throughput.
- **Optional extras**: a FastAPI monitoring dashboard, an [MCP](https://modelcontextprotocol.io) server exposing streams and storage as tools, and pickle-based object storage.

## Installation

```bash
pip install streammachine
```

Extras:

| Extra | Installs | Purpose |
|-------|----------|---------|
| `streammachine[dashboard]` | fastapi, uvicorn | Web dashboard on `http://localhost:8000` |
| `streammachine[mcp]` | mcp | `streammachine-mcp` server for LLM clients |
| `streammachine[objstorage]` | redis | `RedisObjectStorage` (pickle in Redis) |
| `streammachine[cython]` | cython | Build accelerators with `STREAMMACHINE_BUILD_CYTHON=1` |
| `streammachine[all]` | dashboard, mcp, objstorage | Everything above except Cython |

Requires Python 3.10+ and a Redis server (6.2 or newer recommended). `uvloop` is used automatically on Linux and macOS.

## Configuration

Connection settings are read from the environment:

| Variable | Default | Meaning |
|----------|---------|---------|
| `REDIS_URL` | `redis://localhost:6379` | Full connection URL |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_DB` | `localhost` / `6379` / `0` | Used when no URL is given |
| `REDIS_MAX_CONNECTIONS` | `10` | Pool size per connection |
| `STREAMMACHINE_DEFAULT_GROUP` | `eventengine` | Consumer group when `group=` is omitted |
| `STREAMMACHINE_STREAM_MAXLEN` | unset | Approximate max length for produced streams |

## How it works

1. Decorators attach metadata to your handlers (via [venusian](https://pypi.org/project/venusian/)); nothing runs at import time.
2. `app.start()` scans the calling module, creates one consumer task per agent (times `concurrency`), one task per timer, and starts the shared storage manager.
3. Each consumer joins its consumer group with `XREADGROUP`, wraps every entry in a `Message` (topic, stream id, decoded fields, send/receive timestamps) and awaits your handler.
4. `SIGINT`/`SIGTERM` trigger a graceful shutdown: timers stop, tasks are cancelled with a timeout, Redis connections and the storage manager are closed.

Run several copies of the same script to scale horizontally; Redis distributes entries across consumers in a group.

## Documentation

- [Getting started](https://github.com/trbck/streammachine/blob/master/docs/getting-started.md)
- [Configuration](https://github.com/trbck/streammachine/blob/master/docs/configuration.md)
- [Architecture](https://github.com/trbck/streammachine/blob/master/docs/architecture.md)
- [Scaling](https://github.com/trbck/streammachine/blob/master/docs/scaling.md)
- [Best practices](https://github.com/trbck/streammachine/blob/master/docs/best-practices.md)
- [Testing](https://github.com/trbck/streammachine/blob/master/docs/testing.md)
- [Examples](https://github.com/trbck/streammachine/blob/master/examples/README.md)
- [LLM_API.md](https://github.com/trbck/streammachine/blob/master/LLM_API.md): condensed API reference intended for pasting into an LLM context

## Development

```bash
git clone https://github.com/trbck/streammachine.git
cd streammachine
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,all]"
pytest                                  # unit tests (no Redis needed)
RUN_INTEGRATION_TESTS=1 pytest          # integration tests against a local Redis
ruff check src tests
```

Build and check the distribution:

```bash
python -m build
twine check dist/*
```

## License

Apache License 2.0. See [LICENSE](https://github.com/trbck/streammachine/blob/master/LICENSE).
