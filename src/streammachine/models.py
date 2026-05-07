"""
StreamMachine Models Module

This module provides data models and utilities for Redis Streams processing:
- Message: Wrapper for stream messages with metadata
- TimeSeriesBuffer: In-memory sliding window for time series data
- Stream conversion functions: Convert Redis stream output to pandas DataFrames
- Configuration dataclasses: AppConfig, ConsumerConfig, TimerConfig

Performance Notes:
- streams_to_dataframe_fast() uses Cython acceleration when available
- TimeSeriesBuffer prunes old data automatically to limit memory usage
- Message decoding uses Cython for bytes->string conversion if compiled

Redis Stream ID Format:
    Stream IDs are "milliseconds-sequence" (e.g., "1638360000000-0")
    - milliseconds: Unix timestamp in milliseconds
    - sequence: Sequence number for messages in same millisecond
    This allows precise ordering and timestamp extraction.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union
import pandas as pd

# Try to import the Cython-accelerated decoder
try:
    from cython_decode import decode_dict_bytes_to_utf8
    _has_cython_decode = True
except ImportError:
    decode_dict_bytes_to_utf8 = None
    _has_cython_decode = False

# Configuration from environment variables
REDIS_CONNECTION_STRING: str = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))

# Stream processing defaults
RECORDS: int = int(os.getenv("STREAMMACHINE_RECORDS", "10000"))
COUNT: int = int(os.getenv("STREAMMACHINE_COUNT", "10"))  # Number of messages the redis connection is to collect at once.
DEFAULT_CONSUMER_GROUP: str = os.getenv("STREAMMACHINE_DEFAULT_GROUP", "eventengine")

T = TypeVar('T')

def dataclass_list_to_dataframe(instances: List[Any]) -> pd.DataFrame:
    """
    Convert a list of dataclass instances to a pandas DataFrame.
    """
    if not instances:
        return pd.DataFrame()
    if not is_dataclass(instances[0]):
        raise ValueError("All instances must be dataclasses.")
    return pd.DataFrame([asdict(obj) for obj in instances])

def dataframe_to_dataclass_list(df: pd.DataFrame, cls: Type[T]) -> List[T]:
    """
    Convert a pandas DataFrame to a list of dataclass instances of type cls.

    Args:
        df: Pandas DataFrame to convert
        cls: Dataclass type to convert to

    Returns:
        List of dataclass instances

    Raises:
        ValueError: If cls is not a dataclass or DataFrame is missing required fields
    """
    if not hasattr(cls, '__dataclass_fields__'):
        raise ValueError("cls must be a dataclass type.")

    if df.empty:
        return []

    expected_fields = set(cls.__dataclass_fields__.keys())
    actual_fields = set(df.columns)
    missing = expected_fields - actual_fields

    if missing:
        raise ValueError(f"DataFrame missing required fields: {missing}")

    extra = actual_fields - expected_fields
    if extra:
        import logging
        logging.getLogger(__name__).warning(
            f"DataFrame has extra fields that will be ignored: {extra}"
        )

    # Only use fields that exist in the dataclass
    field_columns = list(cls.__dataclass_fields__.keys())
    return [cls(**{k: row[k] for k in field_columns if k in row})
            for row in df.to_dict(orient='records')]


# =============================================================================
# Redis Streams to DataFrame Conversion
# =============================================================================

# Redis stream ID format: "milliseconds-sequence" (e.g., "1638360000000-0")
# The milliseconds part is the timestamp when the message was added to the stream.

StreamOutput = List[Tuple[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]


def _decode_bytes_dict(d: Dict[bytes, bytes]) -> Dict[str, str]:
    """Fast decode of bytes dict to string dict using Cython if available."""
    if _has_cython_decode and decode_dict_bytes_to_utf8 is not None:
        return decode_dict_bytes_to_utf8(d)
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in d.items()}


def _parse_stream_id(stream_id: Union[bytes, str]) -> Tuple[float, int]:
    """
    Parse Redis stream ID into timestamp and sequence number.

    Args:
        stream_id: Redis stream ID like b"1638360000000-0" or "1638360000000-0"

    Returns:
        Tuple of (timestamp_ms, sequence_number)
    """
    if isinstance(stream_id, bytes):
        stream_id = stream_id.decode("utf-8")
    ts_str, seq_str = stream_id.rsplit("-", 1)
    return float(ts_str), int(seq_str)


def streams_to_dataframe(
    streams: StreamOutput,
    stream_name_column: str = "stream",
    id_column: str = "id",
    timestamp_column: str = "timestamp_ms",
    include_sequence: bool = False,
) -> pd.DataFrame:
    """
    Fast conversion of Redis XREAD/XREADGROUP output to pandas DataFrame.

    This function efficiently converts the raw output from Redis streams
    (XREAD/XREADGROUP commands) into a pandas DataFrame with decoded string
    keys and values.

    The conversion uses list comprehension for speed and decodes bytes
    using Cython acceleration if available.

    Args:
        streams: Raw Redis streams output in format:
            [(b'stream_name', [(b'id', {b'key': b'val', ...}), ...]), ...]
        stream_name_column: Column name for the stream name (default: "stream")
        id_column: Column name for the full stream ID (default: "id")
        timestamp_column: Column name for the timestamp in milliseconds (default: "timestamp_ms")
        include_sequence: If True, add a sequence number column (default: False)

    Returns:
        DataFrame with columns: stream, id, timestamp_ms, [sequence], and all
        decoded message fields from the stream data.

    Example:
        >>> result = await client.xread(streams={"mystream": "0-0"}, count=100)
        >>> df = streams_to_dataframe(result)
        >>> print(df.columns)
        Index(['stream', 'id', 'timestamp_ms', 'field1', 'field2'], dtype='object')

    Performance:
        For 100k messages, this runs in ~100-200ms with Cython decode,
        or ~300-400ms with pure Python. Use the cython_decode extension
        for maximum speed on high-throughput streams.
    """
    if not streams:
        return pd.DataFrame()

    # Single-pass extraction with list comprehension
    # This is the fast path from the StackOverflow solution
    rows = []
    for stream_name, messages in streams:
        stream_str = stream_name.decode("utf-8") if isinstance(stream_name, bytes) else stream_name
        for msg_id, msg_data in messages:
            # Parse stream ID for timestamp
            timestamp_ms, seq = _parse_stream_id(msg_id)
            msg_id_str = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else msg_id

            # Decode message data efficiently
            decoded_data = _decode_bytes_dict(msg_data)

            # Build row dict
            row = {
                stream_name_column: stream_str,
                id_column: msg_id_str,
                timestamp_column: timestamp_ms,
            }
            if include_sequence:
                row["sequence"] = seq

            # Add all message fields
            row.update(decoded_data)
            rows.append(row)

    # Use from_records for fast DataFrame creation
    return pd.DataFrame.from_records(rows)


def _decode_if_bytes(value: Union[bytes, str]) -> str:
    """Decode bytes to str if necessary, pass through str unchanged."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def streams_to_dataframe_fast(
    streams: StreamOutput,
    stream_name_column: str = "stream",
    id_column: str = "id",
    timestamp_column: str = "timestamp_ms",
) -> pd.DataFrame:
    """
    Ultra-fast conversion of Redis streams output to DataFrame.

    This is an optimized version that minimizes overhead by:
    1. Pre-allocating lists
    2. Using direct dict construction
    3. Minimizing function calls in the hot path

    Use this for maximum throughput when you need the fastest possible
    conversion. For more flexibility, use streams_to_dataframe().

    Args:
        streams: Raw Redis streams output (supports both bytes and str keys/values)
        stream_name_column: Column name for stream name
        id_column: Column name for stream ID
        timestamp_column: Column name for timestamp

    Returns:
        DataFrame with stream, id, timestamp_ms, and all message fields.
    """
    if not streams:
        return pd.DataFrame()

    # Build all rows as list of tuples, then convert at once
    all_rows = []

    for stream_name, messages in streams:
        # Handle both bytes and str (decoded responses from Redis)
        stream_str = _decode_if_bytes(stream_name)
        for msg_id, msg_data in messages:
            msg_id_str = _decode_if_bytes(msg_id)
            timestamp_ms = float(msg_id_str.rsplit("-", 1)[0])

            # Build row as dict, then extend with decoded data
            row = {
                stream_name_column: stream_str,
                id_column: msg_id_str,
                timestamp_column: timestamp_ms,
            }

            # Decode values - handle both bytes and str
            if _has_cython_decode and decode_dict_bytes_to_utf8 is not None and isinstance(msg_data, dict):
                # Cython path only works with all-bytes keys and values
                # Check all items, not just the first key
                all_bytes = msg_data and all(
                    isinstance(k, bytes) and isinstance(v, bytes)
                    for k, v in msg_data.items()
                )
                if all_bytes:
                    row.update(decode_dict_bytes_to_utf8(msg_data))
                else:
                    # Already decoded or mixed types - decode per-item
                    for k, v in msg_data.items():
                        row[_decode_if_bytes(k)] = _decode_if_bytes(v) if isinstance(v, bytes) else v
            else:
                for k, v in msg_data.items():
                    row[_decode_if_bytes(k)] = _decode_if_bytes(v) if isinstance(v, bytes) else v

            all_rows.append(row)

    return pd.DataFrame.from_records(all_rows)


def prune_old_dataframe_rows(
    df: pd.DataFrame,
    cutoff_seconds: float,
    timestamp_column: str = "timestamp_ms",
    current_time: Optional[float] = None,
) -> pd.DataFrame:
    """
    Remove rows older than cutoff_seconds from current time.

    This is useful for time series data where you want to keep only
    recent data in memory, removing old entries that are no longer
    relevant for analysis or display.

    Args:
        df: DataFrame with a timestamp column (in milliseconds)
        cutoff_seconds: Maximum age of rows to keep (in seconds)
        timestamp_column: Name of the timestamp column (default: "timestamp_ms")
        current_time: Current time in seconds (default: time.time())

    Returns:
        DataFrame with only rows newer than cutoff_seconds

    Example:
        >>> df = streams_to_dataframe(stream_data)
        >>> # Keep only last 60 seconds
        >>> recent_df = prune_old_dataframe_rows(df, cutoff_seconds=60)
    """
    if df.empty or timestamp_column not in df.columns:
        return df

    if current_time is None:
        current_time = time.time()

    # Convert cutoff to milliseconds for comparison
    cutoff_ms = (current_time - cutoff_seconds) * 1000

    return df[df[timestamp_column] >= cutoff_ms].reset_index(drop=True)


class TimeSeriesBuffer:
    """
    An in-memory buffer for time series data with automatic pruning.

    This class maintains a DataFrame of time series data and automatically
    removes old rows when they exceed a configurable age threshold.

    Use this for streaming analytics where you need to maintain a
    sliding window of recent data for analysis or aggregation.

    Args:
        max_age_seconds: Maximum age of data to keep (in seconds)
        timestamp_column: Column name for timestamp field (default: "timestamp_ms")
        max_rows: Optional maximum number of rows to keep (default: None)

    Example:
        >>> buffer = TimeSeriesBuffer(max_age_seconds=300)  # 5 minutes
        >>> df = streams_to_dataframe(stream_data)
        >>> buffer.append(df)
        >>> recent = buffer.get()  # Only last 5 minutes of data
    """

    def __init__(
        self,
        max_age_seconds: float,
        timestamp_column: str = "timestamp_ms",
        max_rows: Optional[int] = None,
    ):
        self.max_age_seconds = max_age_seconds
        self.timestamp_column = timestamp_column
        self.max_rows = max_rows
        self._df: pd.DataFrame = pd.DataFrame()

    def append(self, df: pd.DataFrame) -> None:
        """
        Append new data to the buffer.

        After appending, old rows are automatically pruned and if max_rows
        is set, excess rows are removed from the beginning.

        Args:
            df: DataFrame to append (must have timestamp_column)
        """
        if df.empty:
            return

        if self._df.empty:
            self._df = df.copy()
        else:
            self._df = pd.concat([self._df, df], ignore_index=True)

        # Prune old data
        self._prune()

    def _prune(self) -> None:
        """Remove old and excess rows."""
        # Time-based pruning
        if not self._df.empty and self.timestamp_column in self._df.columns:
            self._df = prune_old_dataframe_rows(
                self._df,
                self.max_age_seconds,
                self.timestamp_column,
            )

        # Row count pruning
        if self.max_rows is not None and len(self._df) > self.max_rows:
            self._df = self._df.iloc[-self.max_rows:].reset_index(drop=True)

    def get(self) -> pd.DataFrame:
        """
        Get the current buffer contents, pruning stale rows first.

        This ensures that even if the stream goes idle, stale data is
        removed when you read from the buffer.

        Returns:
            DataFrame with all buffered data within max_age_seconds (may be empty)
        """
        # Prune on read to handle idle streams
        self._prune()
        return self._df.copy()

    def clear(self) -> None:
        """Clear all buffered data."""
        self._df = pd.DataFrame()

    def __len__(self) -> int:
        """Return number of rows in buffer (after pruning stale data)."""
        self._prune()  # Ensure accurate count by pruning first
        return len(self._df)

    @property
    def last_timestamp(self) -> Optional[float]:
        """Get the most recent timestamp in the buffer."""
        self._prune()  # Ensure we're checking current data
        if self._df.empty or self.timestamp_column not in self._df.columns:
            return None
        return float(self._df[self.timestamp_column].iloc[-1])


@dataclass
class Message:
    """
    Message to be sent to redis streams.
    """
    topic: Optional[str] = None
    key: Optional[str] = None
    sent: Optional[float] = None
    received: Optional[float] = None
    consumer_id: Optional[str] = None
    data: Optional[Tuple[str, Dict]] = None

    @property
    def message(self) -> Dict[str, str]:
        """Decode message data from bytes to utf-8 strings (Cython-accelerated if available)."""
        if not self.data:
            return {}
        d = dict(self.data)
        if _has_cython_decode and decode_dict_bytes_to_utf8 is not None:
            return decode_dict_bytes_to_utf8(d)
        # Fallback to pure Python
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in d.items()}

    @property
    def timer(self) -> str:
        """Return a string with the time taken for the task in ms."""
        if self.sent is not None and self.received is not None:
            return f"{self.topic}: task {(float(self.received) - float(self.sent)) * 1000:.2f} ms"
        return ""

    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class AppConfig:
    """
    Application configuration dataclass.
    """
    name: str = ""
    to_scan: bool = True
    max_processes: int = 5
    max_threads: int = 5
    webserver_port: int = 8000
    webserver_host: str = "localhost"
    debug: bool = False
    redis_url: str = REDIS_CONNECTION_STRING
    redis_max_connections: int = REDIS_MAX_CONNECTIONS
    # Dashboard configuration
    dashboard_enabled: bool = True
    dashboard_port: int = 8000
    dashboard_host: str = "localhost"
    dashboard_refresh_interval: int = 5  # seconds

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.max_processes < 1:
            raise ValueError("max_processes must be >= 1")
        if self.max_threads < 1:
            raise ValueError("max_threads must be >= 1")
        if self.webserver_port < 1 or self.webserver_port > 65535:
            raise ValueError("webserver_port must be between 1 and 65535")
        if self.dashboard_port < 1 or self.dashboard_port > 65535:
            raise ValueError("dashboard_port must be between 1 and 65535")
        if self.dashboard_refresh_interval < 1:
            raise ValueError("dashboard_refresh_interval must be >= 1")
        if self.redis_max_connections < 1:
            raise ValueError("redis_max_connections must be >= 1")

@dataclass
class ConsumerConfig:
    """
    Configuration for a stream consumer agent.
    """
    decorator_type: str
    topic: str
    group: str = DEFAULT_CONSUMER_GROUP
    concurrency: int = 1
    processes: Optional[int] = None
    max_retries: int = 3
    retry_delay_ms: int = 100
    obj_name: Optional[str] = None
    inner_vars: Optional[Any] = None
    mod: Optional[Any] = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.processes is not None and self.processes < 1:
            raise ValueError("processes must be >= 1 if specified")

@dataclass
class TimerConfig:
    """
    Configuration for a timer task.
    """
    decorator_type: str
    t: int
    obj_name: Optional[str] = None
    inner_vars: Optional[Any] = None
    mod: Optional[Any] = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.t < 0:
            raise ValueError("timer interval must be >= 0")

@dataclass
class StreamTopic:
    """
    Stream topic configuration.
    """
    stream: str
    model: str
    group: Optional[str] = None

# --- Cythonization candidates ---
# If you have any CPU-bound data processing, mark here for Cythonization.
# Example:
# def heavy_processing(...):
#     ... # Move to .pyx and use nogil for true parallelism


# Public API
__all__ = [
    # Configuration constants
    "REDIS_CONNECTION_STRING",
    "REDIS_HOST",
    "REDIS_PORT",
    "REDIS_DB",
    "REDIS_MAX_CONNECTIONS",
    "RECORDS",
    "COUNT",
    "DEFAULT_CONSUMER_GROUP",
    # Utility functions
    "dataclass_list_to_dataframe",
    "dataframe_to_dataclass_list",
    # Redis Streams to DataFrame conversion
    "streams_to_dataframe",
    "streams_to_dataframe_fast",
    "prune_old_dataframe_rows",
    "TimeSeriesBuffer",
    # Type alias
    "StreamOutput",
    # Dataclasses
    "Message",
    "AppConfig",
    "ConsumerConfig",
    "TimerConfig",
    "StreamTopic",
]


