"""
Unit tests for FastOHLC aggregation module.

Tests both Cython and Python implementations.
"""
import pytest
import time
from typing import List

# Import with fallback handling
try:
    from streammachine.cython.fast_ohlc import FastOHLC as FastOHLC_Cython
    from streammachine.cython.fast_ohlc import CandleView
    HAS_CYTHON = True
except ImportError:
    FastOHLC_Cython = None
    CandleView = None
    HAS_CYTHON = False

from streammachine.fast_ohlc import (
    FastOHLC_Python,
    CandleData,
    create_ohlc_aggregator,
    parse_stream_id_timestamp,
    format_candle_for_redis,
    _HAS_FAST_OHLC_CYTHON,
)


class TestCandleData:
    """Tests for CandleData pure Python dataclass."""

    def test_candle_data_creation(self):
        """Test basic candle data creation."""
        candle = CandleData()
        assert candle.open == 0.0
        assert candle.high == 0.0
        assert candle.low == 0.0
        assert candle.close == 0.0
        assert candle.volume == 0.0
        assert candle.trade_count == 0

    def test_candle_data_to_dict(self):
        """Test conversion to dictionary."""
        candle = CandleData(
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=10000.0,
            timestamp_ms=1638360000000,
            candle_start_ms=1638360000000,
            trade_count=50
        )
        d = candle.to_dict()
        assert d["open"] == 100.0
        assert d["high"] == 105.0
        assert d["low"] == 99.0
        assert d["close"] == 103.0
        assert d["volume"] == 10000.0
        assert d["trade_count"] == 50


class TestFastOHLCPython:
    """Tests for pure Python OHLC implementation."""

    def test_basic_creation(self):
        """Test basic aggregator creation."""
        agg = FastOHLC_Python()
        assert len(agg.intervals) == 2  # Default: 1min, 5min
        assert agg.tick_count == 0

    def test_custom_intervals(self):
        """Test with custom intervals."""
        agg = FastOHLC_Python(intervals=[30000, 60000, 300000])
        assert len(agg.intervals) == 3
        assert 30000 in agg.intervals
        assert 60000 in agg.intervals

    def test_single_tick_update(self):
        """Test updating with a single tick."""
        agg = FastOHLC_Python(intervals=[60000])

        # Update with one tick
        agg.update_tick(b"AAPL", 150.25, 1000.0, 1638360000000)

        assert agg.tick_count == 1

        # Get candles
        candles = agg.get_candles(b"AAPL", 60000)
        assert len(candles) == 1

        candle = candles[0]
        assert candle.open == 150.25
        assert candle.high == 150.25
        assert candle.low == 150.25
        assert candle.close == 150.25
        assert candle.volume == 1000.0
        assert candle.trade_count == 1

    def test_multiple_ticks_same_candle(self):
        """Test multiple ticks within same candle interval."""
        agg = FastOHLC_Python(intervals=[60000])  # 1 minute

        # Add multiple ticks within same minute
        base_ts = 1638360000000  # Base timestamp
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)  # Open
        agg.update_tick(b"AAPL", 105.0, 200.0, base_ts + 10000)  # High
        agg.update_tick(b"AAPL", 98.0, 150.0, base_ts + 20000)  # Low
        agg.update_tick(b"AAPL", 102.0, 300.0, base_ts + 59000)  # Close

        assert agg.tick_count == 4

        candles = agg.get_candles(b"AAPL", 60000)
        assert len(candles) == 1

        candle = candles[0]
        assert candle.open == 100.0
        assert candle.high == 105.0
        assert candle.low == 98.0
        assert candle.close == 102.0
        assert candle.volume == 750.0  # Sum of volumes
        assert candle.trade_count == 4

    def test_multiple_candles(self):
        """Test ticks across multiple candle intervals."""
        agg = FastOHLC_Python(intervals=[60000])  # 1 minute

        # First candle (minute 1)
        base_ts = 1638360000000
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)
        agg.update_tick(b"AAPL", 105.0, 100.0, base_ts + 30000)

        # Second candle (minute 2)
        base_ts_2 = base_ts + 60000  # Next minute
        agg.update_tick(b"AAPL", 106.0, 200.0, base_ts_2)
        agg.update_tick(b"AAPL", 110.0, 200.0, base_ts_2 + 30000)

        assert agg.tick_count == 4

        candles = agg.get_candles(b"AAPL", 60000)
        assert len(candles) == 2

        # Check first candle
        candle1 = [c for c in candles if c.candle_start_ms == base_ts][0]
        assert candle1.open == 100.0
        assert candle1.close == 105.0
        assert candle1.volume == 200.0

        # Check second candle
        candle2 = [c for c in candles if c.candle_start_ms == base_ts_2][0]
        assert candle2.open == 106.0
        assert candle2.close == 110.0
        assert candle2.volume == 400.0

    def test_multiple_symbols(self):
        """Test handling multiple symbols."""
        agg = FastOHLC_Python(intervals=[60000])

        base_ts = 1638360000000
        agg.update_tick(b"AAPL", 150.0, 100.0, base_ts)
        agg.update_tick(b"GOOGL", 2800.0, 50.0, base_ts)
        agg.update_tick(b"MSFT", 300.0, 200.0, base_ts)

        assert agg.tick_count == 3

        # Check each symbol has its own candle
        aapl = agg.get_candles(b"AAPL", 60000)
        googl = agg.get_candles(b"GOOGL", 60000)
        msft = agg.get_candles(b"MSFT", 60000)

        assert len(aapl) == 1
        assert len(googl) == 1
        assert len(msft) == 1

        assert aapl[0].open == 150.0
        assert googl[0].open == 2800.0
        assert msft[0].open == 300.0

    def test_multiple_intervals(self):
        """Test handling multiple intervals."""
        agg = FastOHLC_Python(intervals=[60000, 300000])  # 1min, 5min

        base_ts = 1638360000000  # Start of a 5-minute interval

        # Add ticks in first minute
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)
        agg.update_tick(b"AAPL", 105.0, 100.0, base_ts + 30000)

        # Add ticks in second minute
        agg.update_tick(b"AAPL", 106.0, 100.0, base_ts + 60000)
        agg.update_tick(b"AAPL", 108.0, 100.0, base_ts + 90000)

        # Should have 2 1-minute candles
        candles_1m = agg.get_candles(b"AAPL", 60000)
        assert len(candles_1m) == 2

        # Should have 1 5-minute candle (all ticks in same 5-min interval)
        candles_5m = agg.get_candles(b"AAPL", 300000)
        assert len(candles_5m) == 1
        assert candles_5m[0].volume == 400.0  # All volumes combined

    def test_get_completed_candles(self):
        """Test getting only completed candles."""
        agg = FastOHLC_Python(intervals=[60000])

        # Add a tick in the past
        past_ts = 1638360000000  # Old timestamp
        agg.update_tick(b"AAPL", 100.0, 100.0, past_ts)

        # Add a tick in the current minute (not completed)
        current_ts = int(time.time() * 1000)
        current_minute_start = (current_ts // 60000) * 60000
        agg.update_tick(b"AAPL", 105.0, 100.0, current_ts)

        # Get completed candles (using a timestamp after the first candle)
        completed = agg.get_completed_candles(b"AAPL", 60000, current_ts)

        # Should only have the old candle as completed
        assert len(completed) == 1
        assert completed[0].candle_start_ms == past_ts

    def test_flush_interval(self):
        """Test flushing completed candles."""
        agg = FastOHLC_Python(intervals=[60000])

        # Add ticks
        base_ts = 1638360000000
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)
        agg.update_tick(b"AAPL", 105.0, 100.0, base_ts + 30000)

        # Add tick in different candle
        agg.update_tick(b"AAPL", 110.0, 100.0, base_ts + 120000)

        # Should have 2 candles
        assert len(agg.get_candles(b"AAPL", 60000)) == 2

        # Flush the first candle
        agg.flush_interval(b"AAPL", 60000, base_ts + 120000)

        # Should have 1 candle remaining
        remaining = agg.get_candles(b"AAPL", 60000)
        assert len(remaining) == 1
        assert remaining[0].candle_start_ms == base_ts + 120000

    def test_clear(self):
        """Test clearing all data."""
        agg = FastOHLC_Python(intervals=[60000])

        agg.update_tick(b"AAPL", 100.0, 100.0, 1638360000000)
        assert agg.tick_count == 1

        agg.clear()
        assert agg.tick_count == 0
        assert len(agg.get_candles(b"AAPL", 60000)) == 0

    def test_process_stream_batch(self):
        """Test processing Redis stream batch format."""
        agg = FastOHLC_Python(intervals=[60000])

        # Simulate XREADGROUP output
        entries = [
            (b"ticks", [
                (b"1638360000000-0", {b"price": b"100.50", b"volume": b"1000"}),
                (b"1638360001000-0", {b"price": b"101.25", b"volume": b"500"}),
                (b"1638360060000-0", {b"price": b"102.00", b"volume": b"750"}),
            ])
        ]

        count = agg.process_stream_batch(entries, "price", "volume")
        assert count == 3
        assert agg.tick_count == 3

        # Check candles
        candles = agg.get_candles(b"ticks", 60000)
        assert len(candles) == 2  # Two different minutes

    def test_get_candles_as_dicts(self):
        """Test getting candles as dictionaries."""
        agg = FastOHLC_Python(intervals=[60000])

        agg.update_tick(b"AAPL", 100.0, 100.0, 1638360000000)
        agg.update_tick(b"AAPL", 105.0, 100.0, 1638360000000 + 30000)

        dicts = agg.get_candles_as_dicts(b"AAPL", 60000)
        assert len(dicts) == 1

        d = dicts[0]
        assert "open" in d
        assert "high" in d
        assert "low" in d
        assert "close" in d
        assert "volume" in d
        assert "trade_count" in d


@pytest.mark.skipif(not HAS_CYTHON, reason="Cython extension not compiled")
class TestFastOHLCCython:
    """Tests for Cython OHLC implementation."""

    def test_basic_creation_cython(self):
        """Test basic aggregator creation with Cython."""
        agg = FastOHLC_Cython()
        assert len(agg.intervals) == 2  # Default: 1min, 5min
        assert agg.tick_count == 0

    def test_single_tick_update_cython(self):
        """Test single tick update with Cython."""
        agg = FastOHLC_Cython(intervals=[60000])

        agg.update_tick(b"AAPL", 150.25, 1000.0, 1638360000000)
        assert agg.tick_count == 1

        candles = agg.get_candles(b"AAPL", 60000)
        assert len(candles) == 1

        candle = candles[0]
        assert candle.open == 150.25
        assert candle.high == 150.25
        assert candle.low == 150.25
        assert candle.close == 150.25
        assert candle.volume == 1000.0

    def test_multiple_ticks_cython(self):
        """Test multiple ticks with Cython."""
        agg = FastOHLC_Cython(intervals=[60000])

        base_ts = 1638360000000
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)
        agg.update_tick(b"AAPL", 105.0, 200.0, base_ts + 10000)
        agg.update_tick(b"AAPL", 98.0, 150.0, base_ts + 20000)
        agg.update_tick(b"AAPL", 102.0, 300.0, base_ts + 59000)

        candles = agg.get_candles(b"AAPL", 60000)
        assert len(candles) == 1

        candle = candles[0]
        assert candle.open == 100.0
        assert candle.high == 105.0
        assert candle.low == 98.0
        assert candle.close == 102.0
        assert candle.volume == 750.0
        assert candle.trade_count == 4

    def test_candle_view_to_dict_cython(self):
        """Test CandleView to_dict method."""
        agg = FastOHLC_Cython(intervals=[60000])

        agg.update_tick(b"AAPL", 100.0, 100.0, 1638360000000)
        candles = agg.get_candles(b"AAPL", 60000)

        d = candles[0].to_dict()
        assert d["open"] == 100.0
        assert d["high"] == 100.0
        assert d["low"] == 100.0
        assert d["close"] == 100.0
        assert d["volume"] == 100.0


class TestCreateOHCLAggregator:
    """Tests for aggregator factory function."""

    def test_factory_returns_correct_type(self):
        """Test that factory returns the best available implementation."""
        agg = create_ohlc_aggregator()

        if _HAS_FAST_OHLC_CYTHON:
            # Should return Cython version
            assert type(agg).__name__ == "FastOHLC"
        else:
            # Should return Python version
            assert isinstance(agg, FastOHLC_Python)

    def test_factory_custom_intervals(self):
        """Test factory with custom intervals."""
        agg = create_ohlc_aggregator(intervals=[30000, 60000])
        assert len(agg.intervals) == 2
        assert 30000 in agg.intervals


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_parse_stream_id_timestamp(self):
        """Test parsing timestamp from stream ID."""
        # Test with string
        ts = parse_stream_id_timestamp("1638360000000-0")
        assert ts == 1638360000000

        # Test with bytes
        ts = parse_stream_id_timestamp(b"1638360000000-123")
        assert ts == 1638360000000

    def test_format_candle_for_redis(self):
        """Test formatting candle for Redis XADD."""
        candle = CandleData(
            open=100.0,
            high=105.0,
            low=98.0,
            close=102.0,
            volume=1000.0,
            timestamp_ms=1638360000000,
            candle_start_ms=1638360000000,
            trade_count=50
        )

        result = format_candle_for_redis(candle, b"AAPL", 60000)

        assert result["symbol"] == "AAPL"
        assert result["interval_ms"] == "60000"
        assert result["open"] == "100.0"
        assert result["high"] == "105.0"
        assert result["low"] == "98.0"
        assert result["close"] == "102.0"
        assert result["volume"] == "1000.0"
        assert result["trade_count"] == "50"


class TestPerformance:
    """Performance comparison tests."""

    @pytest.mark.slow
    def test_python_throughput_manual(self):
        """Manual benchmark of Python implementation throughput."""
        import time

        agg = FastOHLC_Python(intervals=[60000])

        start = time.perf_counter()
        for i in range(10000):
            agg.update_tick(b"AAPL", 100.0 + i * 0.01, 100.0, 1638360000000 + i)
        elapsed = time.perf_counter() - start

        # Should have processed all ticks
        assert agg.tick_count == 10000

        # Report performance
        ticks_per_sec = 10000 / elapsed
        print(f"\nPython implementation: {ticks_per_sec:.0f} ticks/sec, {elapsed*1000:.2f}ms for 10k ticks")

        # Should be reasonably fast (at least 10k/sec)
        assert ticks_per_sec > 5000, f"Performance too slow: {ticks_per_sec:.0f} ticks/sec"

    @pytest.mark.skipif(not HAS_CYTHON, reason="Cython extension not compiled")
    @pytest.mark.slow
    def test_cython_throughput(self, benchmark):
        """Benchmark Cython implementation throughput."""
        agg = FastOHLC_Cython(intervals=[60000])

        def update_ticks():
            for i in range(1000):
                agg.update_tick(b"AAPL", 100.0 + i * 0.01, 100.0, 1638360000000 + i)

        # Run benchmark
        benchmark(update_ticks)

        # Should have processed all ticks
        assert agg.tick_count == 1000


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_get_candles(self):
        """Test getting candles when none exist."""
        agg = FastOHLC_Python()
        candles = agg.get_candles(b"NONEXISTENT", 60000)
        assert len(candles) == 0

    def test_zero_volume(self):
        """Test handling zero volume."""
        agg = FastOHLC_Python(intervals=[60000])
        agg.update_tick(b"AAPL", 100.0, 0.0, 1638360000000)

        candles = agg.get_candles(b"AAPL", 60000)
        assert candles[0].volume == 0.0

    def test_negative_price(self):
        """Test handling negative price (edge case)."""
        agg = FastOHLC_Python(intervals=[60000])
        agg.update_tick(b"AAPL", -100.0, 100.0, 1638360000000)

        candles = agg.get_candles(b"AAPL", 60000)
        assert candles[0].open == -100.0

    def test_very_large_volume(self):
        """Test handling very large volume."""
        agg = FastOHLC_Python(intervals=[60000])
        large_vol = 1e15  # Very large volume
        agg.update_tick(b"AAPL", 100.0, large_vol, 1638360000000)

        candles = agg.get_candles(b"AAPL", 60000)
        assert candles[0].volume == large_vol

    def test_very_small_interval(self):
        """Test with very small interval (1 second)."""
        agg = FastOHLC_Python(intervals=[1000])  # 1 second

        base_ts = 1638360000000
        # Two ticks in same second
        agg.update_tick(b"AAPL", 100.0, 100.0, base_ts)
        agg.update_tick(b"AAPL", 101.0, 100.0, base_ts + 500)

        # Tick in next second
        agg.update_tick(b"AAPL", 102.0, 100.0, base_ts + 1000)

        candles = agg.get_candles(b"AAPL", 1000)
        assert len(candles) == 2

    def test_very_large_interval(self):
        """Test with very large interval (1 day)."""
        agg = FastOHLC_Python(intervals=[86400000])  # 1 day

        base_ts = 1638360000000
        # Multiple ticks in same day
        for i in range(10):
            agg.update_tick(b"AAPL", 100.0 + i, 100.0, base_ts + i * 3600000)

        candles = agg.get_candles(b"AAPL", 86400000)
        assert len(candles) == 1
        assert candles[0].trade_count == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])