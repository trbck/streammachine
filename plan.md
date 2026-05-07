# StreamMachine Development Plan

## Executive Summary

StreamMachine is an async-first Python framework for distributed stream processing using Redis Streams. This document combines the code review findings, improvement plan, and feature roadmap.

---

## Status Legend

- ✅ **Completed**: Implemented and tested
- 🔄 **In Progress**: Currently being worked on
- 📋 **Planned**: Scheduled for implementation
- ❌ **Not Started**: Not yet begun

---

## 1. Critical Issues (Must Fix)

### 1.1 Missing `main()` Function in `app.py` ✅
**Status:** Fixed - The `if __name__ == "__main__"` block now prints a helpful message.

### 1.2 Undefined `helpers` Variable in `util.py` ✅
**Status:** Fixed - The `helper` parameter was removed from decorators.

### 1.3 Undefined `value1` Variable in `example.py` ✅
**Status:** Fixed - The example has been updated with proper variable definitions.

### 1.4 Redis Lock Import Error in `objstorage/redisobjstore.py` ✅
**Status:** Fixed - Now uses `from redis.asyncio import Lock`.

---

## 2. High Priority Improvements

### 2.1 Dependencies File ✅
**Status:** Completed - `pyproject.toml` exists with all dependencies.

### 2.2 Hardcoded Redis Connection String ✅
**Status:** Completed - Now uses environment variables (`REDIS_URL`, `REDIS_HOST`, etc.).

### 2.3 Graceful Shutdown Handling ✅
**Status:** Completed - Signal handlers and `shutdown()` method implemented.

### 2.4 Storage Singleton Thread Safety ✅
**Status:** Completed - Double-checked locking pattern implemented.

### 2.5 Message Acknowledgment on Error ✅
**Status:** Completed - Error handling with logging in consumer loop.

### 2.6 Timer Error Handling ✅
**Status:** Completed - Try/except blocks around timer execution.

### 2.7 Connection Pool Configuration ✅
**Status:** Completed - Configurable via `REDIS_MAX_CONNECTIONS` env var.

### 2.8 Configuration Validation ✅
**Status:** Completed - `__post_init__` validation in dataclasses.

### 2.9 Context Manager for Redis Connection ✅
**Status:** Completed - `__aenter__` and `__aexit__` implemented.

### 2.10 Health Check ✅
**Status:** Completed - `health_check()` method added to App.

---

## 3. Medium Priority Improvements

### 3.1 Inconsistent Redis Client Usage 📋
**Issue:** Main package uses `coredis` while `objstorage` uses `redis.asyncio`.
**Plan:** Keep both as optional extras; document usage clearly.

### 3.2 Process Pool Not Utilized 📋
**Issue:** `ProcessPoolExecutor` is created but never used.
**Plan:** Implement multiprocess agent execution or remove dead code.

### 3.3 No Metrics Collection 📋
**Plan:** Add `AgentMetrics` dataclass and `MetricsCollector` class.

### 3.4 Add `__all__` to Public Modules ✅
**Status:** Completed - All modules have `__all__` definitions.

### 3.5 Use `asyncio.Event` for Graceful Shutdown ✅
**Status:** Completed - `_shutdown_event` used for coordinated shutdown.

---

## 4. Test Suite ✅
**Status:** Completed

Created comprehensive test suite:
- `tests/test_models.py` - 22 tests for data models
- `tests/test_storage.py` - 12 tests for Storage singleton
- `tests/test_app.py` - 12 tests for App class
- `tests/test_redisapi.py` - 10 tests for RedisConnection
- `tests/test_objstorage.py` - Integration tests for objstorage (requires Redis)

---

## 5. Project Structure ✅
**Status:** Reorganized

```
streammachine/
├── examples/                 # Example scripts
│   ├── README.md
│   ├── basic_usage.py
│   ├── multiple_consumers.py
│   ├── storage_example.py
│   ├── health_check_example.py
│   ├── batch_processing.py
│   ├── benchmark_latency.py
│   └── benchmark_decode.py
├── src/streammachine/        # Main package
│   ├── __init__.py
│   ├── app.py
│   ├── models.py
│   ├── redisapi.py
│   ├── storage.py
│   ├── util.py
│   ├── cython/               # Cython acceleration
│   │   ├── __init__.py
│   │   ├── cython_decode.pyx
│   │   └── cython_decode.c
│   └── objstorage/           # Optional object storage
│       ├── __init__.py
│       └── redisobjstore.py
├── tests/                    # Test suite
├── config/                   # Configuration files
├── pyproject.toml
├── setup.py
├── plan.md
└── README.md
```

---

## 6. Feature Roadmap

### 6.1 Core Features

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Message Batching & Windowing | High | 📋 | Batch messages with time/count windows |
| Dead Letter Queue (DLQ) | High | 📋 | Automatic routing of failed messages |
| Message Schema Validation | Medium | 📋 | Pydantic/JSON Schema validation |
| Circuit Breaker Pattern | High | 📋 | Protect from cascading failures |
| Priority Streams | Medium | 📋 | Priority-based consumption |

### 6.2 Developer Experience

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| CLI Tool | High | 📋 | Command-line interface for operations |
| Hot Reload | Medium | 📋 | Auto-reload during development |
| Debug Mode with Tracing | Medium | 📋 | Enhanced debugging capabilities |
| Application Scaffolding | Low | 📋 | Generate project structure |

### 6.3 Observability

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Prometheus Metrics | High | 📋 | Export metrics in Prometheus format |
| OpenTelemetry Integration | Medium | 📋 | Distributed tracing |
| Structured Logging | Medium | 📋 | JSON-formatted logs |
| Web Dashboard | Low | 📋 | Real-time monitoring dashboard |

### 6.4 Performance

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Connection Pooling | High | 📋 | Enhanced connection management |
| Message Compression | Medium | 📋 | Auto-compress large messages |
| Memory Pooling | Low | 📋 | Reduce GC pressure |
| Async Generator Consumers | Medium | 📋 | Stream-based message processing |

### 6.5 Reliability

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Exactly-Once Processing | High | 📋 | Idempotency tracking |
| Checkpointing | Medium | 📋 | Save processing state |
| Rate Limiting | Medium | 📋 | Token bucket rate limiting |
| Backpressure Handling | Medium | 📋 | Handle slow consumers |

### 6.6 Testing

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Mock Redis Backend | High | 📋 | In-memory Redis mock for testing |
| Test Utilities | Medium | 📋 | Helper functions for testing agents |
| Integration Test Framework | Medium | 📋 | Framework for real Redis tests |
| Load Testing | Low | 📋 | Built-in load testing utilities |

### 6.7 Time Series Storage

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| TimeSeriesStore Class | High | 📋 | High-performance time series storage |
| Fast DataFrame Conversion | High | 📋 | Optimized pandas conversion |
| Aggregation & Downsampling | Medium | 📋 | Time-based aggregations |

### 6.8 Advanced Features

| Feature | Priority | Status | Description |
|---------|----------|--------|-------------|
| Stream Joins | Medium | 📋 | Windowed stream joins |
| Transformation Pipelines | Medium | 📋 | Chain message transformations |
| Multi-Tenancy | Low | 📋 | Tenant isolation |
| Event Sourcing | Low | 📋 | Event sourcing patterns |

---

## 7. Implementation Phases

### Phase 1: Reliability & Observability (1-2 months)
- [ ] Circuit Breaker Pattern
- [ ] Dead Letter Queue Support
- [ ] Prometheus Metrics Export
- [ ] Mock Redis Backend for Testing
- [ ] Exactly-Once Processing

### Phase 2: Developer Experience (1-2 months)
- [ ] CLI Tool
- [ ] Message Schema Validation
- [ ] Message Batching & Windowing
- [ ] Test Utilities

### Phase 3: Performance (1 month)
- [ ] Connection Pooling Improvements
- [ ] Message Compression
- [ ] Async Generator Consumers

### Phase 4: Advanced Features (2-3 months)
- [ ] Stream Joins
- [ ] Message Transformation Pipelines
- [ ] Rate Limiting & Backpressure
- [ ] Checkpointing

### Phase 5: Polish & Documentation (1 month)
- [ ] Type Stubs
- [ ] API Documentation
- [ ] Web Dashboard
- [ ] Hot Reload

---

## 8. Code Quality Improvements

### 8.1 Documentation
- [ ] Add comprehensive docstrings to all public methods
- [ ] Create API reference documentation
- [ ] Add architecture decision records (ADRs)

### 8.2 Type Hints
- [ ] Complete type annotations in all modules
- [ ] Add type stubs (`.pyi` files)
- [ ] Enable strict mypy checking

### 8.3 Performance
- [ ] Profile and optimize hot paths
- [ ] Consider Cython for critical sections
- [ ] Add performance benchmarks to CI

---

## 9. Metrics for Success

| Metric | Current | Target |
|--------|---------|--------|
| Test Coverage | ~60% (56 tests) | 80%+ |
| Message Throughput | ~50K/s | 100K+/s |
| P99 Latency | 10ms | <5ms |
| Recovery Time | Manual | <30s |
| Documentation | Basic | Comprehensive |

---

## 10. Recent Changes

### 2024-02-22
- ✅ Fixed all critical issues
- ✅ Reorganized project structure
- ✅ Created comprehensive test suite (56 tests passing)
- ✅ Added examples directory with multiple example scripts
- ✅ Moved cython files to `src/streammachine/cython/`
- ✅ Moved objstorage to `src/streammachine/objstorage/`
- ✅ Updated `__init__.py` with proper exports
- ✅ Added version information to package
- ✅ Fixed coredis deprecation warning (`coredis.patterns.streams`)
- ✅ Merged plan.md and features.md into this document

---

## Contributing

When implementing features:
1. Add comprehensive tests
2. Update documentation
3. Maintain backward compatibility
4. Follow existing code style
5. Add type hints
6. Update this plan.md

---

## References

- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [coredis Library](https://github.com/nicholasamorim/coredis)
- [uvloop](https://github.com/MagicStack/uvloop)
- [OpenTelemetry Python](https://opentelemetry.io/docs/instrumentation/python/)
- [Prometheus Client](https://github.com/prometheus/client_python)