"""
Time Series Windowing Example for StreamMachine

This example demonstrates TimeSeriesBuffer for in-memory time series analysis:
- Multiple windows for different timeframes
- Sliding window analytics
- Real-time aggregation

Run with: python timeseries_windowing.py
"""
import asyncio
import time
from streammachine import App, Message
from streammachine.models import TimeSeriesBuffer, streams_to_dataframe


# =============================================================================
# Multiple Window Configuration
# =============================================================================

# Short-term window for immediate analysis (30 seconds)
short_window = TimeSeriesBuffer(max_age_seconds=30, max_rows=1000)

# Medium-term window for minute-level analysis (5 minutes)
medium_window = TimeSeriesBuffer(max_age_seconds=300, max_rows=5000)

# Long-term window for hour-level analysis (1 hour)
long_window = TimeSeriesBuffer(max_age_seconds=3600, max_rows=50000)


# =============================================================================
# Aggregation Functions
# =============================================================================

def calculate_stats(df):
    """Calculate statistics for a DataFrame."""
    if df.empty:
        return {"count": 0, "mean": 0, "min": 0, "max": 0}

    # Assume 'value' column exists
    values = df["value"].astype(float)

    return {
        "count": len(values),
        "mean": values.mean(),
        "min": values.min(),
        "max": values.max(),
        "std": values.std() if len(values) > 1 else 0,
    }


def calculate_rate(df, window_seconds):
    """Calculate events per second."""
    if df.empty:
        return 0

    return len(df) / window_seconds


# =============================================================================
# Data Processing
# =============================================================================

app = App(name="timeseries_example", to_scan=True)


@app.timer(0.5)
async def sensor_producer():
    """Produce sensor data."""
    import random

    await app.send("sensor_data", {
        "sensor_id": f"sensor_{random.randint(1, 5)}",
        "value": random.uniform(20, 30),  # Temperature
        "unit": "celsius",
        "timestamp": time.time(),
    })


@app.agent("sensor_data", group="analyzers")
async def data_analyzer(record: Message):
    """Analyze sensor data with multiple windows."""
    # Parse message
    msg = record.message

    try:
        # Create row for TimeSeriesBuffer
        row_df = streams_to_dataframe([
            (b"sensor_data", [(record.key.encode(), {
                b"timestamp_ms": str(time.time() * 1000).encode(),
                b"sensor_id": msg.get("sensor_id", "").encode(),
                b"value": str(msg.get("value", 0)).encode(),
            })])
        ])

        # Add to all windows
        short_window.append(row_df)
        medium_window.append(row_df)
        long_window.append(row_df)

        # Get current stats from each window
        short_stats = calculate_stats(short_window.get())
        medium_stats = calculate_stats(medium_window.get())

        # Only log occasionally
        if short_stats["count"] % 10 == 0:
            print(f"\n[Analyzer] Sensor: {msg.get('sensor_id')}")
            print(f"  Value: {msg.get('value'):.2f}")
            print(f"  Short window (30s): {short_stats}")
            print(f"  Medium window (5m): {medium_stats}")

    except Exception as e:
        print(f"[Analyzer] Error: {e}")


# =============================================================================
# Window Statistics Reporting
# =============================================================================

@app.timer(10)
async def report_window_stats():
    """Report statistics for all windows."""
    print("\n" + "=" * 50)
    print("Window Statistics")
    print("=" * 50)

    # Short window
    short_df = short_window.get()
    short_stats = calculate_stats(short_df)
    short_rate = calculate_rate(short_df, 30)

    print(f"\n[Short Window] 30 seconds:")
    print(f"  Count: {short_stats['count']}")
    print(f"  Rate: {short_rate:.2f} events/sec")
    print(f"  Mean: {short_stats['mean']:.2f}")
    print(f"  Range: [{short_stats['min']:.2f}, {short_stats['max']:.2f}]")

    # Medium window
    medium_df = medium_window.get()
    medium_stats = calculate_stats(medium_df)
    medium_rate = calculate_rate(medium_df, 300)

    print(f"\n[Medium Window] 5 minutes:")
    print(f"  Count: {medium_stats['count']}")
    print(f"  Rate: {medium_rate:.2f} events/sec")
    print(f"  Mean: {medium_stats['mean']:.2f}")
    print(f"  Std: {medium_stats['std']:.2f}")

    # Long window
    long_df = long_window.get()
    long_stats = calculate_stats(long_df)

    print(f"\n[Long Window] 1 hour:")
    print(f"  Count: {long_stats['count']}")
    print(f"  Mean: {long_stats['mean']:.2f}")

    # Memory usage estimate
    print(f"\n[Memory]")
    print(f"  Short window: {len(short_df)} rows")
    print(f"  Medium window: {len(medium_df)} rows")
    print(f"  Long window: {len(long_df)} rows")

    print("=" * 50 + "\n")


# =============================================================================
# Per-Sensor Windows (Advanced)
# =============================================================================

# Maintain separate windows per sensor
sensor_windows: dict = {}


@app.agent("sensor_data", group="per_sensor_analyzers")
async def per_sensor_analyzer(record: Message):
    """Maintain separate windows per sensor."""
    msg = record.message
    sensor_id = msg.get("sensor_id", "unknown")

    # Create window if not exists
    if sensor_id not in sensor_windows:
        sensor_windows[sensor_id] = TimeSeriesBuffer(max_age_seconds=60, max_rows=500)

    # Add data
    window = sensor_windows[sensor_id]

    try:
        row_df = streams_to_dataframe([
            (b"sensor_data", [(record.key.encode(), {
                b"timestamp_ms": str(time.time() * 1000).encode(),
                b"value": str(msg.get("value", 0)).encode(),
            })])
        ])
        window.append(row_df)
    except Exception:
        pass


@app.timer(30)
async def report_per_sensor_stats():
    """Report per-sensor statistics."""
    print("\n[Per-Sensor Stats]")
    for sensor_id, window in sensor_windows.items():
        df = window.get()
        if not df.empty:
            stats = calculate_stats(df)
            print(f"  {sensor_id}: mean={stats['mean']:.2f}, count={stats['count']}")


# =============================================================================
# Sliding Window Aggregation
# =============================================================================

@app.timer(5)
async def sliding_window_aggregation():
    """Calculate real-time aggregations."""
    import pandas as pd

    # Get data from short window
    df = short_window.get()

    if df.empty:
        return

    try:
        # Calculate rolling statistics
        if "value" in df.columns:
            values = df["value"].astype(float)

            # Rolling mean (last 10 values)
            if len(values) >= 10:
                rolling_mean = values.rolling(10).mean().iloc[-1]
                print(f"\n[Rolling] Last 10 values mean: {rolling_mean:.2f}")

            # Exponential moving average
            ema = values.ewm(span=5).mean().iloc[-1]
            print(f"[EMA] Exponential moving average: {ema:.2f}")

    except Exception as e:
        print(f"[Aggregation] Error: {e}")


if __name__ == "__main__":
    print("Starting time series windowing example...")
    print("This example demonstrates:")
    print("  - Multiple time windows (30s, 5m, 1h)")
    print("  - Per-sensor window tracking")
    print("  - Real-time aggregation")
    print("  - Sliding window statistics")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")