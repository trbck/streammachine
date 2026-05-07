"""
Real-time OHLC aggregation with Cython acceleration.

This example demonstrates high-throughput tick aggregation to OHLC candles
using the FastOHLC module. It processes tick data from a 'ticks' stream
and emits completed candles to interval-specific streams.

Performance targets:
- Single core: 300k-500k ticks/second (Cython), 50k-100k ticks/second (Python)
- Latency: <5µs per tick (Cython), <20µs per tick (Python)

Requirements:
    pip install streammachine[cython]  # For Cython acceleration
    pip install streammachine           # Falls back to Python implementation

Usage:
    # Start Redis first
    redis-server

    # Run the consumer
    python examples/fast_ohlc_consumer.py

Environment variables:
    REDIS_URL: Redis connection URL (default: redis://localhost:6379/0)
"""

import asyncio
import time
from typing import Dict, Any

from streammachine import App, Message
from streammachine.fast_ohlc import (
    FastOHLC,
    create_ohlc_aggregator,
    format_candle_for_redis,
    _HAS_FAST_OHLC_CYTHON,
)


# =============================================================================
# Configuration
# =============================================================================

# Stream names
INPUT_STREAM = "ticks"
OUTPUT_STREAM_PREFIX = "candles"

# Candle intervals in milliseconds: 1 minute, 5 minutes, 15 minutes
INTERVALS_MS = [60000, 300000, 900000]

# Consumer group
CONSUMER_GROUP = "ohlc_workers"

# Field names in tick data
PRICE_FIELD = "price"
VOLUME_FIELD = "volume"
SYMBOL_FIELD = "symbol"

# How often to check for completed candles (seconds)
FLUSH_INTERVAL_S = 1.0


# =============================================================================
# OHLC Aggregator Agent
# =============================================================================

class OHLCAggregatorAgent:
    """
    High-performance OHLC aggregation agent.

    This agent consumes tick data and aggregates into OHLC candles.
    Completed candles are emitted to interval-specific streams.

    Candle Stream Format:
        Stream: candles_1m, candles_5m, candles_15m, etc.
        Fields:
            symbol: str           # Trading symbol
            interval_ms: str       # Interval in milliseconds
            candle_start_ms: str   # Unix timestamp of candle start
            open: str              # Opening price
            high: str               # Highest price
            low: str                # Lowest price
            close: str              # Closing price
            volume: str             # Total volume
            trade_count: str        # Number of trades
    """

    def __init__(
        self,
        app: App,
        intervals: list = None,
        input_stream: str = INPUT_STREAM,
        output_prefix: str = OUTPUT_STREAM_PREFIX
    ):
        """
        Initialize the OHLC aggregator.

        Args:
            app: StreamMachine App instance
            intervals: List of intervals in milliseconds
            input_stream: Stream to consume ticks from
            output_prefix: Prefix for output streams
        """
        self.app = app
        self.intervals = intervals or INTERVALS_MS
        self.input_stream = input_stream
        self.output_prefix = output_prefix

        # Create aggregator (uses Cython if available)
        self.aggregator = create_ohlc_aggregator(intervals=self.intervals)

        # Track last flush time
        self._last_flush = time.time()

        # Implementation detection
        impl_type = "Cython" if _HAS_FAST_OHLC_CYTHON else "Python"
        print(f"OHLC Aggregator initialized with {impl_type} implementation")
        print(f"  Intervals: {[f'{i//60000}m' for i in self.intervals]}")

    async def process_tick(self, record: Message) -> None:
        """
        Process a single tick message.

        Args:
            record: Message from the ticks stream
        """
        msg = record.message

        # Extract tick data
        try:
            price = float(msg.get(PRICE_FIELD, 0))
            volume = float(msg.get(VOLUME_FIELD, 0))
            symbol = msg.get(SYMBOL_FIELD, self.input_stream)

            # Get timestamp from stream ID or message
            if record.key:
                # Stream ID format: "timestamp-sequence"
                timestamp_ms = int(record.key.split('-')[0])
            else:
                timestamp_ms = int(time.time() * 1000)

            # Update aggregator
            if isinstance(symbol, str):
                symbol = symbol.encode('utf-8')

            self.aggregator.update_tick(symbol, price, volume, timestamp_ms)

        except Exception as e:
            print(f"Error processing tick: {e}")

    async def emit_completed_candles(self) -> None:
        """
        Emit completed candles to output streams.

        A candle is "complete" when the current time is past
        the candle's end time.
        """
        current_time_ms = int(time.time() * 1000)
        current_time_s = time.time()

        # Only flush periodically
        if current_time_s - self._last_flush < FLUSH_INTERVAL_S:
            return

        self._last_flush = current_time_s

        # For each interval, emit completed candles
        for interval_ms in self.intervals:
            stream_name = self._get_stream_name(interval_ms)

            # Get all symbols we have data for
            # Note: In production, you'd track symbols explicitly
            # This is a simplified example
            for symbol in [b"AAPL", b"GOOGL", b"MSFT"]:  # Example symbols
                completed = self.aggregator.get_completed_candles(
                    symbol, interval_ms, current_time_ms
                )

                for candle in completed:
                    await self._emit_candle(stream_name, candle, symbol, interval_ms)

                # Flush completed candles from memory
                if completed:
                    self.aggregator.flush_interval(symbol, interval_ms, current_time_ms)

    async def _emit_candle(
        self,
        stream_name: str,
        candle,
        symbol: bytes,
        interval_ms: int
    ) -> None:
        """
        Emit a single candle to a stream.

        Args:
            stream_name: Target stream name
            candle: CandleView or CandleData object
            symbol: Trading symbol
            interval_ms: Interval in milliseconds
        """
        try:
            # Format candle for Redis
            data = format_candle_for_redis(candle, symbol, interval_ms)

            # Add timestamp for latency tracking
            data["sent"] = str(time.time())

            # Send to stream
            await self.app.send(stream_name, data)

            print(
                f"Emitted candle: {symbol.decode('utf-8')} "
                f"{interval_ms//60000}m "
                f"O={candle.open:.2f} H={candle.high:.2f} "
                f"L={candle.low:.2f} C={candle.close:.2f} "
                f"V={candle.volume:.0f} N={candle.trade_count}"
            )

        except Exception as e:
            print(f"Error emitting candle: {e}")

    def _get_stream_name(self, interval_ms: int) -> str:
        """Get output stream name for an interval."""
        if interval_ms >= 86400000:  # >= 1 day
            suffix = f"{interval_ms // 86400000}d"
        elif interval_ms >= 3600000:  # >= 1 hour
            suffix = f"{interval_ms // 3600000}h"
        elif interval_ms >= 60000:  # >= 1 minute
            suffix = f"{interval_ms // 60000}m"
        else:
            suffix = f"{interval_ms}ms"

        return f"{self.output_prefix}_{suffix}"

    def get_stats(self) -> Dict[str, Any]:
        """Get aggregator statistics."""
        return {
            "tick_count": self.aggregator.tick_count,
            "intervals": self.intervals,
            "implementation": "Cython" if _HAS_FAST_OHLC_CYTHON else "Python",
        }


# =============================================================================
# Tick Producer (for testing)
# =============================================================================

async def tick_producer(app: App, symbols: list = None):
    """
    Produce random tick data for testing.

    Args:
        app: StreamMachine App instance
        symbols: List of symbols to generate ticks for
    """
    import random

    symbols = symbols or ["AAPL", "GOOGL", "MSFT"]
    base_prices = {s: 100.0 + random.random() * 50 for s in symbols}

    while True:
        for symbol in symbols:
            # Generate random price movement
            price = base_prices[symbol] + random.uniform(-1, 1)
            volume = random.uniform(100, 10000)

            # Send tick
            await app.send(INPUT_STREAM, {
                "symbol": symbol,
                "price": str(price),
                "volume": str(volume),
                "timestamp_ms": str(int(time.time() * 1000)),
            })

            # Update base price for next tick
            base_prices[symbol] = price

        # Small delay to avoid overwhelming
        await asyncio.sleep(0.001)  # 1ms delay ~1000 ticks/sec


# =============================================================================
# Main Application
# =============================================================================

# Create app with dashboard for monitoring
app = App(
    name="ohlc_realtime",
    dashboard_enabled=True,
    dashboard_port=8000,
)

# Create aggregator agent
aggregator = OHLCAggregatorAgent(app)


@app.agent(INPUT_STREAM, group=CONSUMER_GROUP)
async def process_ticks(record: Message):
    """
    Process incoming tick data and aggregate into OHLC candles.

    This agent is the hot path - every tick flows through here.
    Uses Cython acceleration when available for maximum throughput.
    """
    await aggregator.process_tick(record)


@app.timer(FLUSH_INTERVAL_S)
async def flush_candles():
    """
    Periodically emit completed candles.

    This runs on a timer to check for candles that have
    completed their interval.
    """
    await aggregator.emit_completed_candles()


@app.timer(10)
async def print_stats():
    """Print aggregator statistics periodically."""
    stats = aggregator.get_stats()
    print(f"Stats: {stats['tick_count']} ticks processed "
          f"({stats['implementation']} implementation)")


# Optional: Enable test tick producer
# Uncomment to generate test ticks
# @app.timer(0.001)
# async def produce_test_ticks():
#     await tick_producer(app)


if __name__ == "__main__":
    print("=" * 60)
    print("OHLC Real-Time Aggregation Example")
    print("=" * 60)
    print()
    print(f"Input stream: {INPUT_STREAM}")
    print(f"Output prefix: {OUTPUT_STREAM_PREFIX}")
    print(f"Intervals: {[f'{i//60000}m' for i in INTERVALS_MS]}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print()
    print(f"Implementation: {'Cython (fast)' if _HAS_FAST_OHLC_CYTHON else 'Python (fallback)'}")
    print()
    print("Dashboard: http://localhost:8000")
    print()
    print("To send test ticks:")
    print("  redis-cli XADD ticks * symbol AAPL price 150.50 volume 1000")
    print()
    print("To view candles:")
    print("  redis-cli XRANGE candles_1m - +")
    print()
    print("=" * 60)

    app.start()