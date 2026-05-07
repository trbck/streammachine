"""
Time Series DataFrame Example

This example demonstrates fast Redis Streams to DataFrame conversion
with automatic time-based pruning for sliding window analytics.

Features:
- streams_to_dataframe(): Fast conversion of Redis stream output
- TimeSeriesBuffer: In-memory buffer with automatic old data removal
- prune_old_dataframe_rows(): Manual pruning of old rows

Run with: python timeseries_dataframe.py
"""
import asyncio
import time
from datetime import datetime

import pandas as pd

from streammachine import (
    RedisConnection,
    streams_to_dataframe,
    streams_to_dataframe_fast,
    prune_old_dataframe_rows,
    TimeSeriesBuffer,
)


async def produce_time_series_data(rc: RedisConnection, stream_name: str, count: int = 10):
    """Produce test data with timestamps to a Redis stream."""
    for i in range(count):
        timestamp = time.time()
        await rc.client.xadd(stream_name, {
            b"value": f"{i}".encode(),
            b"temperature": f"{20 + i * 0.5}".encode(),
            b"sensor_id": b"sensor_01",
            b"timestamp": f"{timestamp}".encode(),
        })
        print(f"[Producer] Added message {i} at {timestamp:.3f}")
        await asyncio.sleep(0.1)  # Small delay between messages


async def consume_and_convert(rc: RedisConnection, stream_name: str, group: str):
    """Consume from stream and convert to DataFrame."""
    consumer = await rc.consumer(
        [stream_name],
        consumer="df_consumer",
        group=group,
        start_from_backlog=True,
    )

    # Create a time series buffer that keeps last 5 seconds of data
    buffer = TimeSeriesBuffer(
        max_age_seconds=5.0,  # Keep 5 seconds of data
        timestamp_column="timestamp_ms",
        max_rows=100,  # Also cap at 100 rows
    )

    print("\n[Consumer] Starting consumption...")
    print("=" * 60)

    message_count = 0
    async for stream, entry in consumer:
        message_count += 1

        # Convert single message to DataFrame
        # In practice, you'd batch multiple messages for efficiency
        single_stream = [(stream, [entry])]
        df = streams_to_dataframe(single_stream)

        # Add to buffer (old data is automatically pruned)
        buffer.append(df)

        # Show current buffer state
        current_df = buffer.get()
        print(f"\n[Message {message_count}] Received from stream: {stream.decode()}")
        print(f"  ID: {entry[0].decode()}")
        print(f"  Data: {df.iloc[0].to_dict()}")
        print(f"  Buffer size: {len(buffer)} rows")

        # Every 5 messages, show the buffer contents
        if message_count % 5 == 0:
            print(f"\n--- Buffer snapshot (last 5 sec) ---")
            if not current_df.empty:
                # Convert timestamp_ms to readable time for display
                display_df = current_df.copy()
                display_df['time'] = pd.to_datetime(
                    display_df['timestamp_ms'], unit='ms'
                ).dt.strftime('%H:%M:%S.%f')
                print(display_df[['time', 'value', 'temperature', 'sensor_id']].tail(5))
            print("-" * 40)

        if message_count >= 20:
            print("\n[Consumer] Reached message limit, stopping...")
            break


async def demo_fast_conversion():
    """Demonstrate the difference between regular and fast conversion."""
    print("\n" + "=" * 60)
    print("Benchmarking: streams_to_dataframe vs streams_to_dataframe_fast")
    print("=" * 60)

    # Create mock data (simulating Redis stream output)
    n_messages = 10000
    mock_stream = [(
        b"test_stream",
        [
            (f"{int(time.time() * 1000)}-{i}".encode(),
             {b"field1": f"value{i}".encode(), b"field2": f"{i * 10}".encode()})
            for i in range(n_messages)
        ]
    )]

    # Benchmark regular version
    start = time.perf_counter()
    df1 = streams_to_dataframe(mock_stream)
    regular_time = time.perf_counter() - start
    print(f"\nstreams_to_dataframe: {regular_time*1000:.2f}ms for {n_messages} messages")
    print(f"  DataFrame shape: {df1.shape}")

    # Benchmark fast version
    start = time.perf_counter()
    df2 = streams_to_dataframe_fast(mock_stream)
    fast_time = time.perf_counter() - start
    print(f"\nstreams_to_dataframe_fast: {fast_time*1000:.2f}ms for {n_messages} messages")
    print(f"  DataFrame shape: {df2.shape}")

    if fast_time > 0:
        print(f"\nSpeedup: {regular_time/fast_time:.2f}x")


async def demo_time_pruning():
    """Demonstrate time-based row pruning."""
    print("\n" + "=" * 60)
    print("Demo: Time-based row pruning")
    print("=" * 60)

    # Create a DataFrame with timestamps from the past
    now = time.time()
    timestamps_ms = [(now - age) * 1000 for age in [1, 5, 10, 30, 60, 120, 300]]

    df = pd.DataFrame({
        'timestamp_ms': timestamps_ms,
        'value': range(len(timestamps_ms)),
        'age_seconds': [1, 5, 10, 30, 60, 120, 300],
    })

    print("\nOriginal DataFrame:")
    print(df)

    # Prune to keep only last 60 seconds
    pruned = prune_old_dataframe_rows(df, cutoff_seconds=60)
    print("\nAfter pruning (keep last 60 seconds):")
    print(pruned)

    # Prune to keep only last 10 seconds
    pruned = prune_old_dataframe_rows(df, cutoff_seconds=10)
    print("\nAfter pruning (keep last 10 seconds):")
    print(pruned)


async def main():
    """Run all demos."""
    print("StreamMachine Time Series DataFrame Demo")
    print("=" * 60)

    # Initialize Redis connection
    rc = RedisConnection(url="redis://localhost:6379/0")

    try:
        async with rc:
            # Ensure consumer group exists
            stream_name = "timeseries_demo"
            group_name = "ts_group"

            # Create stream and group (ignore if exists)
            try:
                await rc.client.xgroup_create(stream_name, group_name, "$", mkstream=True)
            except Exception:
                pass  # Group likely already exists

            # Produce some test data
            print("\n--- Producing test data ---")
            await produce_time_series_data(rc, stream_name, count=15)

            # Consume and convert to DataFrame
            print("\n--- Consuming and converting ---")
            await consume_and_convert(rc, stream_name, group_name)

    except Exception as e:
        print(f"\nRedis connection error: {e}")
        print("This demo requires a running Redis instance at localhost:6379")
        print("\nRunning offline demos instead...\n")

        # Run offline demos
        await demo_fast_conversion()
        await demo_time_pruning()
        return

    # Run benchmarks (offline, doesn't need Redis)
    await demo_fast_conversion()
    await demo_time_pruning()


if __name__ == "__main__":
    asyncio.run(main())