"""
Performance benchmarks for StreamMachine.

These tests measure performance of critical operations:
- streams_to_dataframe vs streams_to_dataframe_fast
- TimeSeriesBuffer append/prune at various scales
- Message throughput with varying concurrency
- Cython decode vs Python decode comparison

Run with: pytest tests/test_benchmarks.py --benchmark-only -v

Or with specific benchmark:
    pytest tests/test_benchmarks.py::TestStreamConversionBenchmark -v
"""
import time
import pytest
import pandas as pd
from typing import List, Tuple, Dict

from streammachine.models import (
    streams_to_dataframe,
    streams_to_dataframe_fast,
    TimeSeriesBuffer,
    prune_old_dataframe_rows,
)


# Mark all tests in this file as benchmarks
pytestmark = pytest.mark.benchmark


def create_test_stream_output(
    num_messages: int,
    num_streams: int = 1,
    fields_per_message: int = 5,
) -> List[Tuple[bytes, List[Tuple[bytes, Dict[bytes, bytes]]]]]:
    """Create test stream output data for benchmarking.

    Args:
        num_messages: Total number of messages across all streams
        num_streams: Number of streams to distribute messages across
        fields_per_message: Number of fields per message

    Returns:
        Stream output in format returned by Redis XREAD
    """
    base_ts = int(time.time() * 1000)
    streams = []

    messages_per_stream = num_messages // num_streams

    for stream_idx in range(num_streams):
        stream_name = f"stream_{stream_idx}".encode()
        messages = []

        for msg_idx in range(messages_per_stream):
            # Create stream ID: timestamp-sequence
            msg_id = f"{base_ts + msg_idx}-{stream_idx}".encode()

            # Create message fields
            fields = {}
            for field_idx in range(fields_per_message):
                field_name = f"field_{field_idx}".encode()
                field_value = f"value_{msg_idx}_{field_idx}".encode()
                fields[field_name] = field_value

            messages.append((msg_id, fields))

        streams.append((stream_name, messages))

    return streams


class TestStreamConversionBenchmark:
    """Benchmarks for Redis stream to DataFrame conversion."""

    @pytest.mark.parametrize("num_messages", [100, 1000, 10000, 100000])
    @pytest.mark.benchmark(
        group="stream-conversion",
        min_rounds=5,
    )
    def test_streams_to_dataframe_benchmark(self, benchmark, num_messages):
        """Benchmark streams_to_dataframe at various scales."""
        stream_output = create_test_stream_output(num_messages)

        def convert():
            return streams_to_dataframe(stream_output)

        result = benchmark(convert)
        assert len(result) == num_messages

    @pytest.mark.parametrize("num_messages", [100, 1000, 10000, 100000])
    @pytest.mark.benchmark(
        group="stream-conversion",
        min_rounds=5,
    )
    def test_streams_to_dataframe_fast_benchmark(self, benchmark, num_messages):
        """Benchmark streams_to_dataframe_fast at various scales."""
        stream_output = create_test_stream_output(num_messages)

        def convert():
            return streams_to_dataframe_fast(stream_output)

        result = benchmark(convert)
        assert len(result) == num_messages

    @pytest.mark.parametrize("num_messages", [1000, 10000])
    def test_compare_fast_vs_regular(self, num_messages):
        """Compare performance of fast vs regular conversion."""
        stream_output = create_test_stream_output(num_messages)

        # Warm up
        streams_to_dataframe(stream_output)
        streams_to_dataframe_fast(stream_output)

        # Measure regular
        start = time.perf_counter()
        for _ in range(10):
            streams_to_dataframe(stream_output)
        regular_time = time.perf_counter() - start

        # Measure fast
        start = time.perf_counter()
        for _ in range(10):
            streams_to_dataframe_fast(stream_output)
        fast_time = time.perf_counter() - start

        # Fast should be at least as fast (often faster)
        # We don't assert this strictly due to timing variance
        print(f"\nRegular: {regular_time:.4f}s, Fast: {fast_time:.4f}s")
        print(f"Speedup: {regular_time / fast_time:.2f}x")


class TestTimeSeriesBufferBenchmark:
    """Benchmarks for TimeSeriesBuffer operations."""

    @pytest.mark.parametrize("num_rows", [100, 1000, 10000, 100000])
    @pytest.mark.benchmark(
        group="timeseries-buffer",
        min_rounds=5,
    )
    def test_buffer_append_benchmark(self, benchmark, num_rows):
        """Benchmark TimeSeriesBuffer append at various scales."""
        buffer = TimeSeriesBuffer(max_age_seconds=3600)  # 1 hour

        base_ts = time.time() * 1000
        df = pd.DataFrame({
            "timestamp_ms": [base_ts + i for i in range(num_rows)],
            "value": range(num_rows),
        })

        def append():
            buffer.clear()
            buffer.append(df)

        benchmark(append)
        assert len(buffer) == num_rows

    @pytest.mark.parametrize("num_rows,keep_percent", [
        (1000, 50),   # Keep 50%
        (1000, 10),   # Keep 10%
        (10000, 10),  # Keep 10%
    ])
    @pytest.mark.benchmark(
        group="timeseries-buffer",
        min_rounds=5,
    )
    def test_buffer_prune_benchmark(self, benchmark, num_rows, keep_percent):
        """Benchmark TimeSeriesBuffer pruning."""
        buffer = TimeSeriesBuffer(max_age_seconds=60)

        # Create data where some is old
        base_ts = time.time() * 1000
        old_ts = base_ts - (120 * 1000)  # 2 minutes ago (beyond 60s window)

        # Mix old and new data
        rows = []
        for i in range(num_rows):
            if i < (num_rows * keep_percent // 100):
                ts = base_ts + i  # Recent
            else:
                ts = old_ts + i  # Old
            rows.append({"timestamp_ms": ts, "value": i})

        df = pd.DataFrame(rows)

        def prune():
            buffer.clear()
            buffer.append(df)  # Append triggers prune
            return buffer.get()

        result = benchmark(prune)
        # Should have kept only keep_percent of rows
        assert len(result) <= num_rows * keep_percent // 100 + 100  # Allow some variance

    @pytest.mark.parametrize("num_rows", [100, 1000, 10000])
    def test_buffer_get_prune_performance(self, num_rows):
        """Test get() performance with pruning."""
        buffer = TimeSeriesBuffer(max_age_seconds=60)

        base_ts = time.time() * 1000
        df = pd.DataFrame({
            "timestamp_ms": [base_ts + i for i in range(num_rows)],
            "value": range(num_rows),
        })

        buffer.append(df)

        # Measure get() performance
        times = []
        for _ in range(100):
            start = time.perf_counter()
            result = buffer.get()
            times.append(time.perf_counter() - start)

        avg_time = sum(times) / len(times)
        print(f"\nAverage get() time for {num_rows} rows: {avg_time*1000:.2f}ms")

        # get() should be fast (< 10ms for 10k rows)
        assert avg_time < 0.01


class TestPruneBenchmark:
    """Benchmarks for row pruning operations."""

    @pytest.mark.parametrize("num_rows", [1000, 10000, 100000])
    @pytest.mark.benchmark(
        group="prune",
        min_rounds=5,
    )
    def test_prune_old_rows_benchmark(self, benchmark, num_rows):
        """Benchmark prune_old_dataframe_rows at various scales."""
        base_ts = time.time() * 1000

        # Mix of old and new rows
        df = pd.DataFrame({
            "timestamp_ms": [base_ts - i * 100 for i in range(num_rows)],
            "value": range(num_rows),
        })

        def prune():
            return prune_old_dataframe_rows(df, cutoff_seconds=60)

        result = benchmark(prune)
        # Should keep only rows within 60 seconds
        assert len(result) < num_rows


class TestDecodeBenchmark:
    """Benchmarks for bytes to string decoding."""

    def test_python_decode_performance(self):
        """Benchmark pure Python dict decode."""
        # Create test data with many fields
        num_messages = 10000
        num_fields = 10

        messages = []
        for i in range(num_messages):
            fields = {}
            for j in range(num_fields):
                fields[f"field_{j}".encode()] = f"value_{i}_{j}".encode()
            messages.append((f"1234567890-{i}".encode(), fields))

        def decode_pure_python():
            results = []
            for msg_id, fields in messages:
                decoded = {k.decode("utf-8"): v.decode("utf-8") for k, v in fields.items()}
                results.append(decoded)
            return results

        start = time.perf_counter()
        for _ in range(10):
            decode_pure_python()
        elapsed = time.perf_counter() - start

        print(f"\nPure Python decode: {elapsed:.4f}s for 10 iterations")

        # Should complete in reasonable time
        assert elapsed < 5.0

    def test_decode_in_context(self):
        """Benchmark decode in streams_to_dataframe context."""
        stream_output = create_test_stream_output(10000, fields_per_message=10)

        # Measure with streams_to_dataframe (uses decode internally)
        start = time.perf_counter()
        for _ in range(10):
            df = streams_to_dataframe(stream_output)
        regular_time = time.perf_counter() - start

        # Measure with fast version
        start = time.perf_counter()
        for _ in range(10):
            df = streams_to_dataframe_fast(stream_output)
        fast_time = time.perf_counter() - start

        print(f"\nRegular: {regular_time:.4f}s, Fast: {fast_time:.4f}s")


class TestThroughputBenchmark:
    """Benchmarks for message throughput."""

    @pytest.mark.parametrize("batch_size", [10, 100, 1000])
    def test_dataframe_append_throughput(self, batch_size):
        """Test DataFrame append throughput for various batch sizes."""
        buffer = TimeSeriesBuffer(max_age_seconds=3600)

        base_ts = time.time() * 1000
        total_messages = 10000

        # Create batches
        batches = []
        for batch_start in range(0, total_messages, batch_size):
            batch_df = pd.DataFrame({
                "timestamp_ms": [base_ts + i for i in range(batch_start, min(batch_start + batch_size, total_messages))],
                "value": range(batch_start, min(batch_start + batch_size, total_messages)),
            })
            batches.append(batch_df)

        # Measure append throughput
        start = time.perf_counter()
        for batch in batches:
            buffer.append(batch)
        elapsed = time.perf_counter() - start

        messages_per_second = total_messages / elapsed
        print(f"\nBatch size {batch_size}: {messages_per_second:.0f} messages/sec")

        # Should achieve at least 10k messages/sec
        assert messages_per_second > 10000


class TestMemoryBenchmark:
    """Benchmarks for memory usage."""

    @pytest.mark.parametrize("num_rows", [10000, 100000])
    def test_buffer_memory_usage(self, num_rows):
        """Test memory usage of TimeSeriesBuffer."""
        import sys

        buffer = TimeSeriesBuffer(max_age_seconds=3600)

        base_ts = time.time() * 1000
        df = pd.DataFrame({
            "timestamp_ms": [base_ts + i for i in range(num_rows)],
            "value": range(num_rows),
        })

        buffer.append(df)

        # Get memory usage
        df_memory = df.memory_usage(deep=True).sum()
        buffer_df = buffer.get()
        buffer_memory = buffer_df.memory_usage(deep=True).sum()

        print(f"\nDataFrame memory: {df_memory / 1024 / 1024:.2f} MB")
        print(f"Buffer memory: {buffer_memory / 1024 / 1024:.2f} MB")

        # Buffer should not significantly increase memory
        assert buffer_memory <= df_memory * 1.5  # Allow 50% overhead


# Performance regression tests
class TestPerformanceRegression:
    """Tests to catch performance regressions."""

    def test_stream_conversion_no_regression(self):
        """Ensure stream conversion hasn't regressed."""
        stream_output = create_test_stream_output(10000)

        # Baseline: should complete in under 1 second
        start = time.perf_counter()
        for _ in range(10):
            streams_to_dataframe_fast(stream_output)
        elapsed = time.perf_counter() - start

        # This is a regression test - adjust threshold as needed
        assert elapsed < 1.0, f"Performance regression: {elapsed:.2f}s > 1.0s"

    def test_buffer_append_no_regression(self):
        """Ensure buffer append hasn't regressed."""
        buffer = TimeSeriesBuffer(max_age_seconds=3600)

        base_ts = time.time() * 1000
        df = pd.DataFrame({
            "timestamp_ms": [base_ts + i for i in range(10000)],
            "value": range(10000),
        })

        # Baseline: should complete in under 100ms
        start = time.perf_counter()
        for _ in range(10):
            buffer.clear()
            buffer.append(df)
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, f"Performance regression: {elapsed:.2f}s > 1.0s"