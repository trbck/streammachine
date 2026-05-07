# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
"""
Cython-accelerated stream consumer that bypasses Python overhead.

This module provides a high-performance consumer that processes Redis
stream bytes directly, avoiding the overhead of:
- Message object creation
- Python string decoding
- Dict construction for each message

The consumer yields parsed dicts instead of Message objects, which
can be passed directly to FastOHLC for aggregation.

Performance:
- Zero-copy bytes parsing
- Minimal Python object creation
- Batch processing for XREADGROUP output

Usage:
    from streammachine.cython.fast_consumer import FastStreamConsumer

    consumer = FastStreamConsumer(
        streams=["ticks"],
        group="tick_workers",
        price_field="price",
        volume_field="volume"
    )

    async for symbol, price, volume, timestamp in consumer.consume():
        # Process tick data directly
        ohlc.update_tick(symbol, price, volume, timestamp)
"""

import asyncio
import uuid
import time
from typing import AsyncIterator, Dict, List, Optional, Any, Callable

from cpython.bytes cimport PyBytes_AsStringAndSize
from libc.stdint cimport uint64_t


cdef class ParsedMessage:
    """
    Lightweight parsed message from Redis stream.

    This class provides direct access to parsed tick data without
    the overhead of creating full Message objects.
    """
    cdef:
        public bytes stream
        public bytes entry_id
        public dict fields
        public uint64_t timestamp_ms

    def __init__(self, bytes stream, bytes entry_id, dict fields, uint64_t timestamp_ms):
        self.stream = stream
        self.entry_id = entry_id
        self.fields = fields
        self.timestamp_ms = timestamp_ms

    cpdef bytes get_field(self, bytes field_name, bytes default = None):
        """Get a field value as bytes."""
        return self.fields.get(field_name, default)

    cpdef double get_field_float(self, bytes field_name, double default = 0.0):
        """Get a field value as float, parsing from bytes if needed."""
        cdef:
            object val
            bytes bval

        val = self.fields.get(field_name)
        if val is None:
            return default

        if isinstance(val, bytes):
            return float(val.decode('utf-8'))
        return float(val)

    cpdef uint64_t get_field_int(self, bytes field_name, uint64_t default = 0):
        """Get a field value as integer, parsing from bytes if needed."""
        cdef:
            object val
            bytes bval

        val = self.fields.get(field_name)
        if val is None:
            return default

        if isinstance(val, bytes):
            return int(val.decode('utf-8'))
        return int(val)

    @property
    def timestamp(self) -> int:
        """Timestamp in milliseconds from stream ID."""
        return self.timestamp_ms

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "stream": self.stream.decode('utf-8') if self.stream else None,
            "entry_id": self.entry_id.decode('utf-8') if self.entry_id else None,
            "timestamp_ms": self.timestamp_ms,
            "fields": {k.decode('utf-8') if isinstance(k, bytes) else k:
                       v.decode('utf-8') if isinstance(v, bytes) else v
                       for k, v in self.fields.items()}
        }


cdef class FastStreamConsumer:
    """
    High-performance consumer that processes Redis stream bytes directly.

    This consumer bypasses the standard Message object creation overhead
    and yields ParsedMessage objects that can be passed directly to
    FastOHLC for aggregation.

    Features:
    - Zero-copy parsing of tick fields
    - Direct access to bytes values
    - Batch processing support
    - Async iteration for streaming

    Example:
        >>> consumer = FastStreamConsumer(streams=["ticks"], group="workers")
        >>> async for msg in consumer.consume():
        ...     price = msg.get_field_float(b"price")
        ...     volume = msg.get_field_float(b"volume")
        ...     ohlc.update_tick(msg.stream, price, volume, msg.timestamp_ms)
    """

    cdef:
        object _client
        list _streams
        str _group
        str _consumer_id
        bint _running
        int _count
        int _block_ms
        object _redis_connection

    def __init__(
        self,
        streams: List[str],
        group: str = "default_group",
        consumer_id: str = None,
        count: int = 100,
        block_ms: int = 1000
    ):
        """
        Initialize the fast stream consumer.

        Args:
            streams: List of stream names to consume from
            group: Consumer group name
            consumer_id: Unique consumer ID (default: auto-generated UUID)
            count: Maximum messages per XREADGROUP call
            block_ms: Block timeout in milliseconds
        """
        self._streams = [s.encode('utf-8') if isinstance(s, str) else s for s in streams]
        self._group = group
        self._consumer_id = consumer_id or str(uuid.uuid4())
        self._running = False
        self._count = count
        self._block_ms = block_ms
        self._client = None
        self._redis_connection = None

    async def _ensure_client(self):
        """Ensure Redis connection is established."""
        if self._client is not None:
            return self._client

        # Import here to avoid circular dependency
        from ..redisapi import RedisConnection

        self._redis_connection = RedisConnection()
        await self._redis_connection._ensure_pool()
        self._client = self._redis_connection.client

        # Create consumer group if not exists
        for stream in self._streams:
            try:
                await self._client.xgroup_create(
                    stream,
                    self._group,
                    id="0",
                    mkstream=True
                )
            except Exception:
                # Group might already exist
                pass

        return self._client

    async def _close(self):
        """Close the Redis connection."""
        if self._redis_connection:
            await self._redis_connection.close()
            self._redis_connection = None
            self._client = None

    cdef ParsedMessage _parse_entry(self, bytes stream_name, object entry):
        """
        Parse a Redis stream entry into a ParsedMessage.

        This is the fast path that avoids creating Message objects
        and directly parses the fields.
        """
        cdef:
            bytes entry_id
            dict fields
            uint64_t timestamp_ms

        # Extract entry ID and fields
        if hasattr(entry, 'identifier'):
            # coredis GroupConsumer entry format
            entry_id = entry.identifier
            fields = dict(entry.field_values)
        else:
            # Raw tuple format (stream_name, [(id, fields), ...])
            entry_id = entry[0]
            fields = dict(entry[1])

        # Parse timestamp from stream ID
        if isinstance(entry_id, bytes):
            id_str = entry_id.decode('utf-8')
        else:
            id_str = str(entry_id)

        timestamp_ms = int(id_str.split('-')[0])

        return ParsedMessage(stream_name, entry_id, fields, timestamp_ms)

    async def consume(self) -> AsyncIterator[ParsedMessage]:
        """
        Consume messages from the stream and yield ParsedMessage objects.

        This is an async generator that continuously reads from the
        configured streams using XREADGROUP.

        Yields:
            ParsedMessage objects with direct field access

        Example:
            >>> async for msg in consumer.consume():
            ...     print(f"Stream: {msg.stream}, TS: {msg.timestamp_ms}")
        """
        cdef:
            object client
            list results
            bytes stream
            object entry
            ParsedMessage msg

        await self._ensure_client()
        client = self._client

        self._running = True

        try:
            # Get GroupConsumer from coredis
            consumer = await self._redis_connection.consumer(
                [s.decode('utf-8') if isinstance(s, bytes) else s for s in self._streams],
                self._consumer_id,
                self._group
            )

            async for stream, entry in consumer:
                msg = self._parse_entry(stream, entry)
                yield msg

        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            await self._close()

    async def consume_batch(
        self,
        count: int = None,
        block_ms: int = None
    ) -> List[ParsedMessage]:
        """
        Consume a batch of messages and return as a list.

        This is useful for batch processing where you want to
        process multiple messages at once.

        Args:
            count: Maximum messages to fetch (default: self._count)
            block_ms: Block timeout (default: self._block_ms)

        Returns:
            List of ParsedMessage objects
        """
        cdef:
            list messages = []
            object client
            list results
            bytes stream
            object entry
            ParsedMessage msg

        count = count or self._count
        block_ms = block_ms or self._block_ms

        await self._ensure_client()
        client = self._client

        # Use XREADGROUP directly for batch
        stream_ids = {s: ">" for s in self._streams}

        try:
            results = await client.xreadgroup(
                groupname=self._group,
                consumername=self._consumer_id,
                streams=stream_ids,
                count=count,
                block=block_ms
            )

            if results:
                for stream, entries in results:
                    for entry in entries:
                        msg = self._parse_entry(stream, entry)
                        messages.append(msg)

        except asyncio.CancelledError:
            pass

        return messages

    async def ack(self, stream: bytes, entry_id: bytes):
        """
        Acknowledge a message.

        Args:
            stream: Stream name as bytes
            entry_id: Entry ID as bytes
        """
        if self._client:
            await self._client.xack(stream, self._group, entry_id)

    async def ack_batch(self, messages: List[ParsedMessage]):
        """
        Acknowledge multiple messages at once.

        Args:
            messages: List of ParsedMessage objects to acknowledge
        """
        if not self._client or not messages:
            return

        # Group by stream for efficient ACK
        cdef:
            dict by_stream = {}

        for msg in messages:
            if msg.stream not in by_stream:
                by_stream[msg.stream] = []
            by_stream[msg.stream].append(msg.entry_id)

        for stream, ids in by_stream.items():
            if ids:
                await self._client.xack(stream, self._group, *ids)

    @property
    def is_running(self) -> bool:
        """Check if consumer is actively running."""
        return self._running

    @property
    def consumer_id(self) -> str:
        """Get the consumer ID."""
        return self._consumer_id

    @property
    def group(self) -> str:
        """Get the consumer group name."""
        return self._group

    @property
    def streams(self) -> List[str]:
        """Get the list of stream names."""
        return [s.decode('utf-8') if isinstance(s, bytes) else s for s in self._streams]


# =============================================================================
# Helper Functions for Direct Stream Processing
# =============================================================================

def parse_stream_entries(
    entries: List,
    price_field: str = "price",
    volume_field: str = "volume",
    symbol_field: str = None,
    timestamp_from_id: bool = True
) -> List[Dict[str, Any]]:
    """
    Parse Redis XREADGROUP output into tick data dictionaries.

    This is a utility function for batch processing without creating
    a full consumer instance.

    Args:
        entries: Raw output from XREADGROUP [(stream, [(id, fields), ...]), ...]
        price_field: Field name for price
        volume_field: Field name for volume
        symbol_field: Field name for symbol (default: use stream name)
        timestamp_from_id: Extract timestamp from stream ID (default: True)

    Returns:
        List of dictionaries with: symbol, price, volume, timestamp_ms, fields

    Example:
        >>> result = await client.xreadgroup(...)
        >>> ticks = parse_stream_entries(result, "price", "volume")
        >>> for tick in ticks:
        ...     ohlc.update_tick(tick['symbol'], tick['price'],
        ...                      tick['volume'], tick['timestamp_ms'])
    """
    cdef:
        list ticks = []
        bytes stream_name
        bytes entry_id
        dict fields
        double price
        double volume
        uint64_t timestamp_ms
        str id_str

    # Convert field names to bytes
    price_key = price_field.encode('utf-8')
    volume_key = volume_field.encode('utf-8')
    symbol_key = symbol_field.encode('utf-8') if symbol_field else None

    for stream, messages in entries:
        # Handle bytes or str stream name
        if isinstance(stream, bytes):
            stream_name = stream
            stream_str = stream.decode('utf-8')
        else:
            stream_str = stream
            stream_name = stream.encode('utf-8')

        for entry in messages:
            # Handle different entry formats
            if hasattr(entry, 'identifier'):
                # coredis GroupConsumer entry format
                entry_id = entry.identifier
                fields = dict(entry.field_values)
            else:
                # Tuple format (id, fields)
                entry_id = entry[0]
                fields = dict(entry[1])

            # Parse price
            price_val = fields.get(price_key)
            if price_val is None:
                continue

            if isinstance(price_val, bytes):
                price = float(price_val.decode('utf-8'))
            else:
                price = float(price_val)

            # Parse volume
            volume_val = fields.get(volume_key)
            if volume_val is None:
                volume = 0.0
            elif isinstance(volume_val, bytes):
                volume = float(volume_val.decode('utf-8'))
            else:
                volume = float(volume_val)

            # Parse timestamp
            if isinstance(entry_id, bytes):
                id_str = entry_id.decode('utf-8')
            else:
                id_str = str(entry_id)

            timestamp_ms = int(id_str.split('-')[0])

            # Determine symbol
            if symbol_key:
                symbol_val = fields.get(symbol_key)
                if symbol_val is None:
                    symbol = stream_str
                elif isinstance(symbol_val, bytes):
                    symbol = symbol_val.decode('utf-8')
                else:
                    symbol = str(symbol_val)
            else:
                symbol = stream_str

            ticks.append({
                "symbol": symbol,
                "symbol_bytes": stream_name,
                "price": price,
                "volume": volume,
                "timestamp_ms": timestamp_ms,
                "entry_id": entry_id if isinstance(entry_id, bytes) else entry_id.encode('utf-8'),
                "fields": fields
            })

    return ticks