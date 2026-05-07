"""
OHLC (Open-High-Low-Close) Aggregation Example

This example demonstrates:
- Producing stock tick/trade data to a Redis stream
- Consuming ticks and aggregating into OHLC candles using TimeSeriesBuffer
- Real-time candlestick chart data generation

Run with: python ohlc_aggregation.py
"""
import asyncio
import random
import time
from dataclasses import dataclass

import pandas as pd

from streammachine import (
    RedisConnection,
    streams_to_dataframe,
    TimeSeriesBuffer,
)


@dataclass
class OHLC:
    """Open-High-Low-Close candle with volume."""
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    trade_count: int

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "close": self.close_price,
            "volume": self.volume,
            "trade_count": self.trade_count,
        }


def aggregate_ohlc_from_df(df: pd.DataFrame, interval_seconds: int = 60) -> pd.DataFrame:
    """
    Aggregate tick data into OHLC candles.

    Args:
        df: DataFrame with timestamp_ms, price, and volume columns
        interval_seconds: Candle interval in seconds (default: 60 = 1 minute)

    Returns:
        DataFrame with OHLC columns aggregated by interval
    """
    if df.empty:
        return pd.DataFrame()

    # Convert timestamp_ms to datetime and floor to interval
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["timestamp_ms"], unit="ms")
    df["interval"] = df["datetime"].dt.floor(f"{interval_seconds}s")

    # Aggregate by interval
    ohlc = df.groupby("interval").agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("volume", "sum"),
        trade_count=("price", "count"),
    ).reset_index()

    # Convert interval back to timestamp_ms for compatibility
    ohlc["timestamp_ms"] = ohlc["interval"].astype("int64") // 1_000_000

    return ohlc[["timestamp_ms", "open", "high", "low", "close", "volume", "trade_count"]]


class TickProducer:
    """Simulates stock tick data producer."""

    SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

    def __init__(self, rc: RedisConnection, stream_prefix: str = "ticks"):
        self.rc = rc
        self.stream_prefix = stream_prefix
        self.prices = {symbol: random.uniform(100, 500) for symbol in self.SYMBOLS}

    async def produce_tick(self, symbol: str = None) -> dict:
        """Produce a single tick and send to stream."""
        if symbol is None:
            symbol = random.choice(self.SYMBOLS)

        # Simulate price movement (random walk)
        price_change = random.gauss(0, 0.5)  # Small random walk
        self.prices[symbol] = max(1, self.prices[symbol] + price_change)

        # Random volume
        volume = random.randint(100, 10000)

        # Create tick data
        tick = {
            b"symbol": symbol.encode(),
            b"price": f"{self.prices[symbol]:.2f}".encode(),
            b"volume": str(volume).encode(),
            b"timestamp": f"{time.time()}".encode(),
        }

        # Send to stream
        stream_name = f"{self.stream_prefix}:{symbol}"
        await self.rc.client.xadd(stream_name, tick)

        return {
            "symbol": symbol,
            "price": self.prices[symbol],
            "volume": volume,
            "stream": stream_name,
        }


class OHLCConsumer:
    """Consumes tick data and maintains OHLC candles."""

    def __init__(self, interval_seconds: int = 60, max_age_seconds: float = 300):
        self.interval_seconds = interval_seconds
        # Buffer to hold recent tick data (5 minutes by default)
        self.buffer = TimeSeriesBuffer(
            max_age_seconds=max_age_seconds,
            max_rows=10000,  # Safety limit
        )
        # Track last emitted candle timestamp to avoid duplicate emissions
        self._last_emitted_ts: float | None = None

    def process_tick(self, tick_df: pd.DataFrame) -> dict | None:
        """
        Process incoming tick DataFrame and return new OHLC candle if one completed.

        Args:
            tick_df: DataFrame from streams_to_dataframe with tick data

        Returns:
            Dictionary with OHLC data if a NEW candle completed, None otherwise.
            Each candle is emitted only once, when it first completes.
        """
        if tick_df.empty:
            return None

        # Add to buffer
        self.buffer.append(tick_df)

        # Get all recent data and aggregate
        recent = self.buffer.get()
        if recent.empty:
            return None

        # Aggregate into OHLC candles
        ohlc_df = aggregate_ohlc_from_df(recent, self.interval_seconds)

        if ohlc_df.empty:
            return None

        # Get the latest completed candle (not the current forming one)
        # A candle is "complete" when we're past its interval
        now_ms = time.time() * 1000
        current_interval_start = (int(now_ms // 1000) // self.interval_seconds) * self.interval_seconds * 1000

        # Find candles that have completed (interval start < current interval)
        completed = ohlc_df[ohlc_df["timestamp_ms"] < current_interval_start]

        if completed.empty:
            return None

        # Get the most recent completed candle
        latest_ts = completed["timestamp_ms"].iloc[-1]
        latest = completed.iloc[-1]

        # Only emit if this is a NEW candle (timestamp advanced)
        if self._last_emitted_ts is not None and latest_ts <= self._last_emitted_ts:
            return None

        # Update last emitted and return the candle
        self._last_emitted_ts = latest_ts

        return {
            "timestamp": latest_ts,
            "open": latest["open"],
            "high": latest["high"],
            "low": latest["low"],
            "close": latest["close"],
            "volume": int(latest["volume"]),
            "trade_count": int(latest["trade_count"]),
        }

    def get_current_buffer(self) -> pd.DataFrame:
        """Get current tick buffer."""
        return self.buffer.get()


async def demo_producer_consumer():
    """Demo: Single producer feeding tick data."""
    print("\n" + "=" * 60)
    print("Demo: Tick Producer with OHLC Aggregation")
    print("=" * 60)

    rc = RedisConnection(url="redis://localhost:6379/0")

    try:
        async with rc:
            producer = TickProducer(rc)

            # Produce some ticks
            print("\n[Producer] Sending tick data...")
            for i in range(10):
                result = await producer.produce_tick()
                print(f"  Tick {i+1}: {result['symbol']} @ ${result['price']:.2f} "
                      f"(vol: {result['volume']})")
                await asyncio.sleep(0.1)

            print("\n[Producer] Ticks sent successfully!")

    except Exception as e:
        print(f"\nRedis connection error: {e}")
        print("This demo requires a running Redis instance at localhost:6379")


async def demo_ohlc_aggregation():
    """Demo: Show OHLC aggregation without Redis."""
    print("\n" + "=" * 60)
    print("Demo: OHLC Aggregation (Offline)")
    print("=" * 60)

    # Create mock tick data
    base_time = time.time()
    base_price = 150.0

    # Simulate 1 minute of tick data
    ticks = []
    for i in range(50):
        # Simulate price movement
        base_price += random.gauss(0, 0.3)
        tick_time = (base_time + i * 0.05) * 1000  # Ticks every 0.05 seconds

        ticks.append({
            "timestamp_ms": tick_time,
            "price": base_price,
            "volume": random.randint(100, 1000),
        })

    # Add some ticks from previous minute (to show completed candles)
    for i in range(20):
        base_price += random.gauss(0, 0.2)
        tick_time = (base_time - 60 + i * 0.05) * 1000  # Previous minute

        ticks.append({
            "timestamp_ms": tick_time,
            "price": base_price,
            "volume": random.randint(100, 1000),
        })

    df = pd.DataFrame(ticks)
    print(f"\nCreated {len(df)} mock ticks")
    print(f"Time range: {df['timestamp_ms'].min() / 1000:.2f} to {df['timestamp_ms'].max() / 1000:.2f}")

    # Aggregate into 1-minute OHLC candles
    ohlc_df = aggregate_ohlc_from_df(df, interval_seconds=60)

    print(f"\nAggregated into {len(ohlc_df)} OHLC candles:")
    print("-" * 80)

    for _, row in ohlc_df.iterrows():
        timestamp = pd.to_datetime(row["timestamp_ms"], unit="ms").strftime("%H:%M:%S")
        print(f"  {timestamp} | O:{row['open']:.2f} H:{row['high']:.2f} "
              f"L:{row['low']:.2f} C:{row['close']:.2f} | "
              f"Vol:{int(row['volume']):,} Trades:{int(row['trade_count'])}")


async def demo_timeseries_buffer():
    """Demo: TimeSeriesBuffer with sliding window."""
    print("\n" + "=" * 60)
    print("Demo: TimeSeriesBuffer Sliding Window")
    print("=" * 60)

    buffer = TimeSeriesBuffer(max_age_seconds=5.0)

    # Add data over time
    print("\nAdding data points...")
    for i in range(10):
        now = time.time()
        df = pd.DataFrame({
            "timestamp_ms": [now * 1000],
            "value": [i * 10],
        })
        buffer.append(df)
        await asyncio.sleep(0.5)  # Simulate time passing

        # Show buffer state
        current = buffer.get()
        print(f"  Added value {i*10}, buffer now has {len(buffer)} rows "
              f"(oldest age: {(now - current['timestamp_ms'].min()/1000):.1f}s ago)")

    print(f"\nFinal buffer size: {len(buffer)} rows")
    print(f"Buffer time range: {buffer.get()['value'].min()} to {buffer.get()['value'].max()}")


async def demo_stream_to_dataframe():
    """Demo: streams_to_dataframe conversion."""
    print("\n" + "=" * 60)
    print("Demo: Redis Stream to DataFrame Conversion")
    print("=" * 60)

    # Simulate Redis XREAD output
    now = int(time.time() * 1000)

    mock_stream_output = [
        (b"ticks:AAPL", [
            (f"{now}-0".encode(), {b"symbol": b"AAPL", b"price": b"150.25", b"volume": b"1000"}),
            (f"{now}-1".encode(), {b"symbol": b"AAPL", b"price": b"150.30", b"volume": b"500"}),
            (f"{now}-2".encode(), {b"symbol": b"AAPL", b"price": b"150.20", b"volume": b"750"}),
        ]),
        (b"ticks:GOOGL", [
            (f"{now}-0".encode(), {b"symbol": b"GOOGL", b"price": b"2800.50", b"volume": b"200"}),
        ]),
    ]

    print("\nRaw Redis stream output:")
    print(f"  {mock_stream_output}")

    # Convert to DataFrame
    df = streams_to_dataframe(mock_stream_output)

    print("\nConverted DataFrame:")
    print(df[["stream", "id", "timestamp_ms", "symbol", "price", "volume"]].to_string())

    # Show fast conversion comparison
    import time as time_module

    # Create larger dataset for benchmark
    large_stream = [(
        b"ticks:AAPL",
        [(f"{now + i}-0".encode(), {b"price": f"{150 + i * 0.01}".encode(), b"volume": b"100"})
         for i in range(10000)]
    )]

    # Benchmark regular version
    start = time_module.perf_counter()
    df1 = streams_to_dataframe(large_stream)
    regular_time = time_module.perf_counter() - start

    # Benchmark fast version
    start = time_module.perf_counter()
    df2 = streams_to_dataframe_fast(large_stream)
    fast_time = time_module.perf_counter() - start

    print(f"\nBenchmark (10k messages):")
    print(f"  streams_to_dataframe: {regular_time*1000:.2f}ms")
    print(f"  Fast conversion produces same structure: {set(df1.columns) == set(df2.columns)}")


def print_usage():
    """Print usage examples."""
    print("\n" + "=" * 60)
    print("OHLC Aggregation - Usage Examples")
    print("=" * 60)

    print("""
# Basic usage - convert Redis stream output to DataFrame
from streammachine import streams_to_dataframe

# After XREAD from Redis
result = await client.xread(streams={"ticks:AAPL": "0-0"}, count=100)
df = streams_to_dataframe(result)

# Aggregate ticks into 1-minute OHLC candles
from streammachine.examples.ohlc_aggregation import aggregate_ohlc_from_df

ohlc_df = aggregate_ohlc_from_df(df, interval_seconds=60)

# Use TimeSeriesBuffer for sliding window
from streammachine import TimeSeriesBuffer

buffer = TimeSeriesBuffer(max_age_seconds=300)  # 5 minutes

# In your consumer loop:
async for stream, entry in consumer:
    df = streams_to_dataframe([(stream, [entry])])
    buffer.append(df)
    ohlc = aggregate_ohlc_from_df(buffer.get())
""")


# Import streams_to_dataframe_fast for benchmark
from streammachine import streams_to_dataframe_fast


async def main():
    """Run all demos."""
    print("StreamMachine OHLC Aggregation Demo")
    print("=" * 60)

    # Run offline demos first (don't need Redis)
    await demo_ohlc_aggregation()
    await demo_stream_to_dataframe()
    await demo_timeseries_buffer()

    # Try Redis demo (will fail gracefully if no Redis)
    await demo_producer_consumer()

    print_usage()


if __name__ == "__main__":
    asyncio.run(main())