"""
Tick to OHLC Aggregation Example

This example demonstrates real-time tick aggregation into OHLC candles:
- Receiving tick data from streams
- Aggregating into time-based candles (1-minute, 5-minute, etc.)
- Using TimeSeriesBuffer for windowed analysis

Run with: python tick_to_ohlc.py
"""
import asyncio
import time
from collections import defaultdict
from streammachine import App, Message
from streammachine.models import TimeSeriesBuffer

app = App(name="tick_to_ohlc", to_scan=True)


# =============================================================================
# OHLC Candle Data Structure
# =============================================================================

class OHLC:
    """OHLC (Open, High, Low, Close) candle."""

    def __init__(self, symbol: str, start_time: float):
        self.symbol = symbol
        self.start_time = start_time
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0
        self.tick_count = 0

    def update(self, price: float, size: int):
        """Update candle with new tick."""
        if self.open is None:
            self.open = price

        self.high = price if self.high is None else max(self.high, price)
        self.low = price if self.low is None else min(self.low, price)
        self.close = price
        self.volume += size
        self.tick_count += 1

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "tick_count": self.tick_count,
            "start_time": self.start_time,
        }


# =============================================================================
# Candle Manager
# =============================================================================

class CandleManager:
    """Manage OHLC candles across multiple symbols and intervals."""

    def __init__(self):
        # symbol -> interval -> OHLC
        self.candles: dict = defaultdict(lambda: defaultdict(dict))
        self.intervals = [60, 300]  # 1-minute and 5-minute candles

    def update(self, symbol: str, price: float, size: int, timestamp: float):
        """Update candles with new tick."""
        for interval in self.intervals:
            # Calculate candle start time
            candle_start = (timestamp // interval) * interval

            # Get or create candle
            if candle_start not in self.candles[symbol][interval]:
                self.candles[symbol][interval][candle_start] = OHLC(
                    symbol, candle_start
                )

            # Update candle
            self.candles[symbol][interval][candle_start].update(price, size)

    def get_completed_candles(self, current_time: float) -> list:
        """Get candles that have completed."""
        completed = []

        for symbol, intervals in self.candles.items():
            for interval, candles in intervals.items():
                for start_time, candle in list(candles.items()):
                    # Candle is complete if start_time + interval < current_time
                    if start_time + interval < current_time:
                        completed.append((interval, candle.to_dict()))
                        # Remove completed candle
                        del self.candles[symbol][interval][start_time]

        return completed


# Global candle manager
candle_manager = CandleManager()

# Time series buffers for different intervals
buffer_1m = TimeSeriesBuffer(max_age_seconds=3600, max_rows=10000)
buffer_5m = TimeSeriesBuffer(max_age_seconds=14400, max_rows=10000)


# =============================================================================
# Tick Producer (Simulated)
# =============================================================================

@app.timer(0.5)
async def produce_ticks():
    """Produce simulated tick data."""
    import random

    symbols = ["AAPL", "MSFT", "GOOGL"]
    base_prices = {"AAPL": 150, "MSFT": 300, "GOOGL": 120}

    for symbol in symbols:
        # Simulate realistic price movement
        base = base_prices[symbol]
        price = base + random.uniform(-2, 2)
        size = random.randint(100, 1000)

        await app.send("ticks", {
            "symbol": symbol,
            "price": price,
            "size": size,
            "timestamp": time.time(),
        })


# =============================================================================
# Tick Processor
# =============================================================================

@app.agent("ticks", group="tick_processors")
async def process_tick(record: Message):
    """Process incoming ticks and aggregate into candles."""
    msg = record.message

    symbol = msg.get("symbol")
    price = float(msg.get("price", 0))
    size = int(msg.get("size", 0))
    timestamp = float(msg.get("timestamp", time.time()))

    # Update candles
    candle_manager.update(symbol, price, size, timestamp)

    # Forward tick to symbol-specific stream
    await app.send(f"ticks_{symbol}", msg)


# =============================================================================
# Candle Emitter
# =============================================================================

@app.timer(1)
async def emit_candles():
    """Emit completed candles."""
    current_time = time.time()
    completed = candle_manager.get_completed_candles(current_time)

    for interval, candle in completed:
        stream_name = f"candles_{interval}s"
        await app.send(stream_name, candle)
        print(f"[Candle] {candle['symbol']} {interval}s: "
              f"O={candle['open']:.2f} H={candle['high']:.2f} "
              f"L={candle['low']:.2f} C={candle['close']:.2f} "
              f"V={candle['volume']}")


# =============================================================================
# Candle Consumers
# =============================================================================

@app.agent("candles_60s", group="candle_handlers")
async def handle_1m_candles(record: Message):
    """Handle 1-minute candles."""
    msg = record.message
    # Add to time series buffer for analysis
    # buffer_1m.append(...)

    print(f"[1M] {msg.get('symbol')}: Close {msg.get('close'):.2f}, "
          f"Volume {msg.get('volume')}")


@app.agent("candles_300s", group="candle_handlers")
async def handle_5m_candles(record: Message):
    """Handle 5-minute candles."""
    msg = record.message
    print(f"[5M] {msg.get('symbol')}: Close {msg.get('close'):.2f}, "
          f"Volume {msg.get('volume')}")


# =============================================================================
# Statistics
# =============================================================================

@app.timer(10)
async def show_statistics():
    """Show aggregation statistics."""
    print("\n" + "=" * 50)
    print("Candle Statistics")
    print("=" * 50)

    for symbol, intervals in candle_manager.candles.items():
        for interval, candles in intervals.items():
            print(f"  {symbol} {interval}s: {len(candles)} active candles")

    print("=" * 50 + "\n")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Tick to OHLC Aggregation Example")
    print("=" * 60)
    print("\nThis example demonstrates:")
    print("  - Receiving tick data from streams")
    print("  - Aggregating into 1-minute and 5-minute candles")
    print("  - Emitting completed candles")
    print("  - Multiple interval support")
    print("\nArchitecture:")
    print("  Ticks stream → process_tick → candle_manager")
    print("    → candles_60s stream → handle_1m_candles")
    print("    → candles_300s stream → handle_5m_candles")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")