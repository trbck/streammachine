# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-09-17

First release on PyPI.

### Added
- `App` with `@app.agent` / `@app.timer` decorators, consumer groups, graceful shutdown.
- Consumer reconnect with exponential backoff on stream-read failures.
- Bounded stream retention via `stream_maxlen` / `STREAMMACHINE_STREAM_MAXLEN`.
- `Storage` shared state, `TimeSeriesBuffer`, DataFrame helpers, OHLC aggregation.
- Optional extras: `dashboard`, `mcp`, `objstorage`, `cython`.

### Fixed
- Redis connection pool is now entered before use, as required by coredis 6.x.
  Previously every command failed with "Connection pool is not initialized" on
  coredis 6; coredis 4.x keeps working unchanged.
- Importing `streammachine` no longer fails when an incompatible `mcp` SDK is installed.
- The Cython decoder was imported under the wrong module name and never used.
- `FastOHLCConsumer.emit_candles()` raised `AttributeError`; it now emits completed candles.
- The library no longer calls `logging.basicConfig()` or installs the uvloop event-loop
  policy at import time; uvloop is optional and used only inside `App.start()`.
- `App.send()` / `send_batch()` no longer mutate the caller's dictionaries.
- Agents declared with `processes=N` were silently never started; they now run
  in-process with a warning (multiprocess execution is not implemented yet).
- `pipeline_xadd()` (and therefore `App.send_batch()`) failed on coredis 6.x; it
  now supports both the old and the new pipeline API.
- `REDIS_URL` from the environment was ignored unless a URL was passed explicitly.
- `GroupConsumer` is imported from `coredis.patterns.streams` with a fallback to
  the old location, removing a deprecation warning on import.

### Changed
- Requires Python 3.10+; tested with coredis 4.24 and 6.9. `numpy` is no longer a direct
  dependency and `redis` moved to the `objstorage` extra.
- Cython accelerators are built only with `STREAMMACHINE_BUILD_CYTHON=1`; the
  generated C sources are no longer committed.
