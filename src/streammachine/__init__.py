"""
StreamMachine - Redis Streams Processing Framework

A simple, lightweight Redis Streams processing library built on coredis.
Provides decorator-based agent/timer registration with venusian discovery.

Quick Start:
    >>> from streammachine import App, Message
    >>>
    >>> app = App(name="my_app")
    >>>
    >>> # Producer: send a message every second
    >>> @app.timer(1)
    >>> async def producer():
    >>>     await app.send("greetings", {"message": "hello"})
    >>>
    >>> # Consumer: process messages from the stream
    >>> @app.agent("greetings", group="greeters")
    >>> async def consumer(record: Message):
    >>>     print(f"Received: {record.message}")
    >>>
    >>> if __name__ == "__main__":
    >>>     app.start()

Key Concepts:
    - Agent: A consumer that reads from Redis Streams and processes messages
    - Timer: A periodic task that can produce messages to streams
    - Consumer Group: Multiple consumers sharing the message load
    - Storage: Cross-process shared state via multiprocessing.Manager

Public API:
    Core:
        App: Main application class for stream processing
        StreamConsumer: Low-level stream consumer (used internally)
        Message: Message wrapper with topic, key, data, and metadata

    Configuration:
        AppConfig: Application-level configuration
        ConsumerConfig: Agent configuration
        TimerConfig: Timer configuration

    Redis:
        RedisConnection: Async Redis client with connection pooling

    Storage:
        Storage: Singleton for cross-process state sharing

    Data Processing:
        TimeSeriesBuffer: In-memory sliding window for time series
        streams_to_dataframe: Convert Redis stream output to pandas DataFrame
        streams_to_dataframe_fast: Optimized conversion (Cython accelerated)

    Utilities:
        dataclass_list_to_dataframe: Convert dataclass list to DataFrame
        dataframe_to_dataclass_list: Convert DataFrame to dataclass list

Environment Variables:
    REDIS_URL: Full Redis connection URL (e.g., redis://localhost:6379/0)
    REDIS_HOST: Redis host (default: localhost)
    REDIS_PORT: Redis port (default: 6379)
    REDIS_DB: Redis database number (default: 0)
    REDIS_MAX_CONNECTIONS: Connection pool size (default: 10)
    STREAMMACHINE_RECORDS: Default batch size (default: 10000)
    STREAMMACHINE_COUNT: Messages per read (default: 10)
    STREAMMACHINE_DEFAULT_GROUP: Default consumer group name (default: eventengine)
"""

__version__ = "0.1.0"

from .app import App, StreamConsumer
from .models import (
    Message,
    AppConfig,
    ConsumerConfig,
    TimerConfig,
    StreamTopic,
    dataclass_list_to_dataframe,
    dataframe_to_dataclass_list,
    streams_to_dataframe,
    streams_to_dataframe_fast,
    prune_old_dataframe_rows,
    TimeSeriesBuffer,
    StreamOutput,
)
from .redisapi import RedisConnection
from .storage import Storage

# Optional imports
try:
    from .objstorage.redisobjstore import RedisObjectStorage
except ImportError:
    RedisObjectStorage = None  # type: ignore

try:
    from .cython import decode_dict_bytes_to_utf8, _has_cython_decode
except ImportError:
    decode_dict_bytes_to_utf8 = None  # type: ignore
    _has_cython_decode = False

# Fast OHLC (optional Cython acceleration)
try:
    from .fast_ohlc import (
        FastOHLC,
        FastOHLCConsumer,
        FastOHLC_Python,
        CandleData,
        create_ohlc_aggregator,
        parse_stream_id_timestamp,
        format_candle_for_redis,
        _HAS_FAST_OHLC_CYTHON,
    )
except ImportError:
    FastOHLC = None  # type: ignore
    FastOHLCConsumer = None  # type: ignore
    FastOHLC_Python = None  # type: ignore
    CandleData = None  # type: ignore
    create_ohlc_aggregator = None  # type: ignore
    parse_stream_id_timestamp = None  # type: ignore
    format_candle_for_redis = None  # type: ignore
    _HAS_FAST_OHLC_CYTHON = False

# Fast consumer (optional Cython acceleration)
try:
    from .cython import (
        FastStreamConsumer,
        ParsedMessage,
        parse_stream_entries,
        _has_fast_consumer,
    )
except ImportError:
    FastStreamConsumer = None  # type: ignore
    ParsedMessage = None  # type: ignore
    parse_stream_entries = None  # type: ignore
    _has_fast_consumer = False

# MCP server (optional, requires 'mcp' extra)
try:
    from .mcp_server import server as mcp_server, run_server as mcp_run_server, main as mcp_main
except ImportError:
    mcp_server = None  # type: ignore
    mcp_run_server = None  # type: ignore
    mcp_main = None  # type: ignore

# FastMCP server (optional, for use with `mcp dev` command)
try:
    from .mcp_fast import mcp as mcp_fast
except Exception:
    mcp_fast = None  # type: ignore

# Dashboard (optional, requires FastAPI)
try:
    from .dashboard import (
        DashboardManager,
        start_dashboard,
        stop_dashboard,
        create_app,
        get_dashboard_html,
    )
except ImportError:
    DashboardManager = None  # type: ignore
    start_dashboard = None  # type: ignore
    stop_dashboard = None  # type: ignore
    create_app = None  # type: ignore
    get_dashboard_html = None  # type: ignore

__all__ = [
    # Version
    "__version__",
    # Core classes
    "App",
    "StreamConsumer",
    "Message",
    "AppConfig",
    "ConsumerConfig",
    "TimerConfig",
    "StreamTopic",
    # Redis
    "RedisConnection",
    # Storage
    "Storage",
    # Utilities
    "dataclass_list_to_dataframe",
    "dataframe_to_dataclass_list",
    # Redis Streams to DataFrame
    "streams_to_dataframe",
    "streams_to_dataframe_fast",
    "prune_old_dataframe_rows",
    "TimeSeriesBuffer",
    "StreamOutput",
    # Optional - Redis object storage
    "RedisObjectStorage",
    # Optional - Cython decode
    "decode_dict_bytes_to_utf8",
    "_has_cython_decode",
    # Optional - Fast OHLC
    "FastOHLC",
    "FastOHLCConsumer",
    "FastOHLC_Python",
    "CandleData",
    "create_ohlc_aggregator",
    "parse_stream_id_timestamp",
    "format_candle_for_redis",
    "_HAS_FAST_OHLC_CYTHON",
    # Optional - Fast consumer
    "FastStreamConsumer",
    "ParsedMessage",
    "parse_stream_entries",
    "_has_fast_consumer",
    # MCP Server (optional)
    "mcp_server",
    "mcp_run_server",
    "mcp_main",
    "mcp_fast",
    # Dashboard (optional)
    "DashboardManager",
    "start_dashboard",
    "stop_dashboard",
    "create_app",
    "get_dashboard_html",
]