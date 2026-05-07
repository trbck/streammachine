"""
Tests for streammachine.models module.
"""
import pytest
import pandas as pd
from dataclasses import asdict

from streammachine.models import (
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
)


class TestMessage:
    """Tests for Message dataclass."""

    def test_message_creation(self):
        """Test basic message creation."""
        msg = Message(
            topic="test_topic",
            key="test_key",
            sent=1234567890.0,
            received=1234567890.5,
            data={(b'key', b'value')},
        )
        assert msg.topic == "test_topic"
        assert msg.key == "test_key"
        assert msg.sent == 1234567890.0
        assert msg.received == 1234567890.5

    def test_message_message_property(self, sample_message_data):
        """Test message property decodes bytes to strings."""
        msg = Message(data=sample_message_data)
        decoded = msg.message
        assert decoded['key1'] == 'value1'
        assert decoded['key2'] == 'value2'

    def test_message_timer_property(self):
        """Test timer property calculates latency."""
        msg = Message(
            topic="test",
            sent=100.0,
            received=100.5,
        )
        assert "500.00 ms" in msg.timer

    def test_message_timer_property_no_times(self):
        """Test timer property returns empty string when times are missing."""
        msg = Message(topic="test")
        assert msg.timer == ""

    def test_message_to_dict(self):
        """Test to_dict method."""
        msg = Message(topic="test", key="key1")
        d = msg.to_dict()
        assert d['topic'] == "test"
        assert d['key'] == "key1"


class TestAppConfig:
    """Tests for AppConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = AppConfig()
        assert config.name == ""
        assert config.to_scan is True
        assert config.max_processes == 5
        assert config.max_threads == 5

    def test_custom_values(self):
        """Test custom configuration values."""
        config = AppConfig(
            name="test_app",
            max_processes=10,
            debug=True,
        )
        assert config.name == "test_app"
        assert config.max_processes == 10
        assert config.debug is True

    def test_validation_max_processes(self):
        """Test validation for max_processes."""
        with pytest.raises(ValueError, match="max_processes must be >= 1"):
            AppConfig(max_processes=0)

    def test_validation_max_threads(self):
        """Test validation for max_threads."""
        with pytest.raises(ValueError, match="max_threads must be >= 1"):
            AppConfig(max_threads=0)

    def test_validation_webserver_port(self):
        """Test validation for webserver_port."""
        with pytest.raises(ValueError, match="webserver_port must be between"):
            AppConfig(webserver_port=0)
        with pytest.raises(ValueError, match="webserver_port must be between"):
            AppConfig(webserver_port=70000)


class TestConsumerConfig:
    """Tests for ConsumerConfig dataclass."""

    def test_default_values(self):
        """Test default consumer configuration."""
        config = ConsumerConfig(
            decorator_type="agent",
            topic="test_topic",
        )
        assert config.group == "eventengine"  # DEFAULT_CONSUMER_GROUP
        assert config.concurrency == 1
        assert config.max_retries == 3

    def test_validation_concurrency(self):
        """Test validation for concurrency."""
        with pytest.raises(ValueError, match="concurrency must be >= 1"):
            ConsumerConfig(decorator_type="agent", topic="test", concurrency=0)

    def test_validation_max_retries(self):
        """Test validation for max_retries."""
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            ConsumerConfig(decorator_type="agent", topic="test", max_retries=-1)


class TestTimerConfig:
    """Tests for TimerConfig dataclass."""

    def test_valid_timer(self):
        """Test valid timer configuration."""
        config = TimerConfig(decorator_type="timer", t=10)
        assert config.t == 10

    def test_validation_negative_time(self):
        """Test validation for negative time."""
        with pytest.raises(ValueError, match="timer interval must be >= 0"):
            TimerConfig(decorator_type="timer", t=-1)


class TestStreamTopic:
    """Tests for StreamTopic dataclass."""

    def test_stream_topic_creation(self):
        """Test stream topic creation."""
        topic = StreamTopic(stream="test_stream", model="TestModel")
        assert topic.stream == "test_stream"
        assert topic.model == "TestModel"
        assert topic.group is None

    def test_stream_topic_with_group(self):
        """Test stream topic with group."""
        topic = StreamTopic(stream="test", model="Model", group="my_group")
        assert topic.group == "my_group"


class TestDataclassConversions:
    """Tests for dataclass conversion utilities."""

    def test_dataclass_list_to_dataframe(self):
        """Test converting list of dataclasses to DataFrame."""
        messages = [
            Message(topic="topic1", key="key1"),
            Message(topic="topic2", key="key2"),
        ]
        df = dataclass_list_to_dataframe(messages)
        assert len(df) == 2
        assert 'topic' in df.columns
        assert 'key' in df.columns

    def test_dataclass_list_to_dataframe_empty(self):
        """Test converting empty list returns empty DataFrame."""
        df = dataclass_list_to_dataframe([])
        assert df.empty

    def test_dataclass_list_to_dataframe_non_dataclass(self):
        """Test error when not dataclass."""
        with pytest.raises(ValueError):
            dataclass_list_to_dataframe([{"a": 1}])

    def test_dataframe_to_dataclass_list(self):
        """Test converting DataFrame to list of dataclasses."""
        # ConsumerConfig has many fields, need to include all for the conversion
        df = pd.DataFrame([
            {
                "decorator_type": "agent",
                "topic": "test1",
                "group": "group1",
                "concurrency": 1,
                "processes": None,
                "max_retries": 3,
                "retry_delay_ms": 100,
                "obj_name": None,
                "inner_vars": None,
                "mod": None,
            },
            {
                "decorator_type": "agent",
                "topic": "test2",
                "group": "group1",
                "concurrency": 2,
                "processes": None,
                "max_retries": 3,
                "retry_delay_ms": 100,
                "obj_name": None,
                "inner_vars": None,
                "mod": None,
            },
        ])
        configs = dataframe_to_dataclass_list(df, ConsumerConfig)
        assert len(configs) == 2
        assert all(isinstance(c, ConsumerConfig) for c in configs)
        assert configs[0].topic == "test1"
        assert configs[1].concurrency == 2

    def test_dataframe_to_dataclass_list_empty(self):
        """Test converting empty DataFrame returns empty list."""
        df = pd.DataFrame()
        result = dataframe_to_dataclass_list(df, ConsumerConfig)
        assert result == []

    def test_dataframe_to_dataclass_list_missing_fields(self):
        """Test error when DataFrame missing required fields."""
        df = pd.DataFrame([{"topic": "test"}])  # Missing decorator_type
        with pytest.raises(ValueError, match="missing required fields"):
            dataframe_to_dataclass_list(df, ConsumerConfig)

    def test_dataframe_to_dataclass_list_extra_fields_warning(self, caplog):
        """Test warning when DataFrame has extra fields."""
        import logging
        caplog.set_level(logging.WARNING)
        df = pd.DataFrame([
            {
                "decorator_type": "agent",
                "topic": "test",
                "group": "group1",
                "concurrency": 1,
                "processes": None,
                "max_retries": 3,
                "retry_delay_ms": 100,
                "obj_name": None,
                "inner_vars": None,
                "mod": None,
                "extra_field": "ignored",  # This should trigger warning
            }
        ])
        configs = dataframe_to_dataclass_list(df, ConsumerConfig)
        assert len(configs) == 1
        assert "extra fields" in caplog.text


class TestStreamsToDataFrame:
    """Tests for Redis streams to DataFrame conversion."""

    @pytest.fixture
    def sample_stream_output(self):
        """Create sample Redis stream output for testing."""
        import time
        ts = int(time.time() * 1000)
        return [
            (
                b"mystream",
                [
                    (f"{ts}-0".encode(), {b"sensor": b"temp_01", b"value": b"23.5"}),
                    (f"{ts}-1".encode(), {b"sensor": b"temp_02", b"value": b"24.1"}),
                    (f"{ts}-2".encode(), {b"sensor": b"temp_03", b"value": b"22.8"}),
                ]
            )
        ]

    @pytest.fixture
    def multi_stream_output(self):
        """Create multi-stream Redis output for testing."""
        import time
        ts = int(time.time() * 1000)
        return [
            (
                b"stream_a",
                [
                    (f"{ts}-0".encode(), {b"key": b"a1", b"data": b"value_a1"}),
                    (f"{ts}-1".encode(), {b"key": b"a2", b"data": b"value_a2"}),
                ]
            ),
            (
                b"stream_b",
                [
                    (f"{ts}-0".encode(), {b"key": b"b1", b"data": b"value_b1"}),
                ]
            ),
        ]

    def test_streams_to_dataframe_basic(self, sample_stream_output):
        """Test basic conversion of stream output to DataFrame."""
        df = streams_to_dataframe(sample_stream_output)

        assert len(df) == 3
        assert "stream" in df.columns
        assert "id" in df.columns
        assert "timestamp_ms" in df.columns
        assert "sensor" in df.columns
        assert "value" in df.columns

        # Check all streams are "mystream"
        assert (df["stream"] == "mystream").all()
        # Check values are decoded
        assert df.iloc[0]["sensor"] == "temp_01"
        assert df.iloc[0]["value"] == "23.5"

    def test_streams_to_dataframe_empty(self):
        """Test conversion of empty stream output."""
        df = streams_to_dataframe([])
        assert df.empty

    def test_streams_to_dataframe_multi_stream(self, multi_stream_output):
        """Test conversion with multiple streams."""
        df = streams_to_dataframe(multi_stream_output)

        assert len(df) == 3
        # Check both streams are present
        streams = set(df["stream"])
        assert "stream_a" in streams
        assert "stream_b" in streams

    def test_streams_to_dataframe_custom_columns(self, sample_stream_output):
        """Test custom column names."""
        df = streams_to_dataframe(
            sample_stream_output,
            stream_name_column="source",
            id_column="msg_id",
            timestamp_column="ts",
        )

        assert "source" in df.columns
        assert "msg_id" in df.columns
        assert "ts" in df.columns

    def test_streams_to_dataframe_with_sequence(self, sample_stream_output):
        """Test including sequence number."""
        df = streams_to_dataframe(sample_stream_output, include_sequence=True)

        assert "sequence" in df.columns
        assert df.iloc[0]["sequence"] == 0
        assert df.iloc[1]["sequence"] == 1
        assert df.iloc[2]["sequence"] == 2

    def test_streams_to_dataframe_fast(self, sample_stream_output):
        """Test fast conversion produces same results as regular."""
        df1 = streams_to_dataframe(sample_stream_output)
        df2 = streams_to_dataframe_fast(sample_stream_output)

        # Should have same columns and values
        assert list(df1.columns) == list(df2.columns)
        assert len(df1) == len(df2)

        # Compare values (excluding exact column order)
        for col in df1.columns:
            assert (df1[col] == df2[col]).all()

    def test_streams_to_dataframe_fast_empty(self):
        """Test fast conversion of empty stream output."""
        df = streams_to_dataframe_fast([])
        assert df.empty

    def test_streams_to_dataframe_fast_decoded_strings(self):
        """Test fast conversion handles decoded (str) inputs from Redis client.

        Regression test: Some Redis clients return decoded strings instead of bytes.
        streams_to_dataframe_fast should handle both bytes and str.
        """
        import time

        # Simulate decoded output from Redis client (all strings, no bytes)
        ts = int(time.time() * 1000)
        decoded_output = [
            (
                "mystream",  # str instead of bytes
                [
                    (f"{ts}-0", {"key": "value1", "num": "123"}),  # str keys/values
                    (f"{ts}-1", {"key": "value2", "num": "456"}),
                ]
            )
        ]

        df = streams_to_dataframe_fast(decoded_output)

        assert len(df) == 2
        assert df.iloc[0]["stream"] == "mystream"
        assert df.iloc[0]["key"] == "value1"
        assert df.iloc[0]["num"] == "123"

    def test_streams_to_dataframe_fast_mixed_types(self):
        """Test fast conversion handles mixed bytes/str inputs."""
        import time

        ts = int(time.time() * 1000)
        # Mix of bytes and str
        mixed_output = [
            (
                b"stream1",
                [
                    (f"{ts}-0".encode(), {b"key": b"value"}),  # all bytes
                ]
            ),
            (
                "stream2",
                [
                    (f"{ts}-1", {"key": "value"}),  # all str
                ]
            )
        ]

        df = streams_to_dataframe_fast(mixed_output)

        assert len(df) == 2
        assert set(df["stream"]) == {"stream1", "stream2"}
        assert list(df["key"]) == ["value", "value"]

    def test_streams_to_dataframe_fast_mixed_within_message(self):
        """Test fast conversion handles mixed bytes/str within a SINGLE message.

        Regression test: Previously, checking only the first key would route
        partially-mixed dicts to the Cython decoder, which expects all-bytes.
        """
        import time

        ts = int(time.time() * 1000)
        # Single message with mixed bytes and str keys/values within the same dict
        mixed_within_message = [
            (
                b"stream",
                [
                    # Mix of bytes and str keys/values in the SAME message dict
                    (f"{ts}-0".encode(), {
                        b"bytes_key": b"bytes_value",
                        "str_key": "str_value",
                        b"mixed_key": "str_value_for_bytes_key",
                    }),
                ]
            )
        ]

        df = streams_to_dataframe_fast(mixed_within_message)

        assert len(df) == 1
        assert df.iloc[0]["stream"] == "stream"
        assert df.iloc[0]["bytes_key"] == "bytes_value"
        assert df.iloc[0]["str_key"] == "str_value"
        assert df.iloc[0]["mixed_key"] == "str_value_for_bytes_key"


class TestPruneOldDataFrameRows:
    """Tests for time-based row pruning."""

    @pytest.fixture
    def time_series_df(self):
        """Create a DataFrame with timestamp column."""
        import pandas as pd
        # Use a fixed reference time for deterministic tests
        now = 1000.0  # Fixed reference time
        return pd.DataFrame({
            "timestamp_ms": [(now - age) * 1000 for age in [1, 5, 10, 30, 60, 120]],
            "value": range(6),
            "age_seconds": [1, 5, 10, 30, 60, 120],
        }), now

    def test_prune_keeps_recent_data(self, time_series_df):
        """Test that recent data is kept."""
        df, now = time_series_df
        pruned = prune_old_dataframe_rows(df, cutoff_seconds=60, current_time=now)
        # Should keep rows with age_seconds 1, 5, 10, 30, 60
        assert len(pruned) == 5
        assert pruned["age_seconds"].max() == 60

    def test_prune_removes_old_data(self, time_series_df):
        """Test that old data is removed."""
        df, now = time_series_df
        pruned = prune_old_dataframe_rows(df, cutoff_seconds=30, current_time=now)
        # Should keep rows with age_seconds 1, 5, 10, 30
        assert len(pruned) == 4
        assert pruned["age_seconds"].max() == 30

    def test_prune_empty_dataframe(self):
        """Test pruning empty DataFrame."""
        df = pd.DataFrame()
        pruned = prune_old_dataframe_rows(df, cutoff_seconds=60)
        assert pruned.empty

    def test_prune_missing_timestamp_column(self):
        """Test pruning DataFrame without timestamp column."""
        df = pd.DataFrame({"value": [1, 2, 3]})
        pruned = prune_old_dataframe_rows(df, cutoff_seconds=60)
        assert len(pruned) == 3  # Returns unchanged

    def test_prune_with_custom_column(self):
        """Test pruning with custom timestamp column name."""
        import time
        import pandas as pd
        now = time.time()
        df = pd.DataFrame({
            "custom_ts": [(now - age) * 1000 for age in [1, 60, 120]],
            "value": [1, 2, 3],
        })
        pruned = prune_old_dataframe_rows(
            df, cutoff_seconds=60, timestamp_column="custom_ts"
        )
        assert len(pruned) == 1  # Only 1-second-old row

    def test_prune_with_explicit_current_time(self, time_series_df):
        """Test pruning with explicit current_time parameter."""
        df, now = time_series_df
        # All rows are within 60 seconds when using the fixture's reference time
        pruned = prune_old_dataframe_rows(
            df,
            cutoff_seconds=60,
            current_time=now,
        )
        # Rows with age_seconds <= 60 are kept: 1, 5, 10, 30, 60
        assert len(pruned) == 5


class TestTimeSeriesBuffer:
    """Tests for TimeSeriesBuffer class."""

    def test_buffer_append_and_get(self):
        """Test basic append and get operations."""
        import time
        import pandas as pd

        buffer = TimeSeriesBuffer(max_age_seconds=60.0)

        df = pd.DataFrame({
            "timestamp_ms": [time.time() * 1000],
            "value": [42],
        })

        buffer.append(df)
        result = buffer.get()

        assert len(result) == 1
        assert result.iloc[0]["value"] == 42

    def test_buffer_pruning(self):
        """Test that old data is pruned."""
        import time
        import pandas as pd

        # Very short buffer (0.1 seconds)
        buffer = TimeSeriesBuffer(max_age_seconds=0.1)

        # Add old data
        old_time = (time.time() - 10) * 1000  # 10 seconds ago
        old_df = pd.DataFrame({
            "timestamp_ms": [old_time],
            "value": ["old"],
        })
        buffer.append(old_df)

        # Add new data
        new_df = pd.DataFrame({
            "timestamp_ms": [time.time() * 1000],
            "value": ["new"],
        })
        buffer.append(new_df)

        # Only new data should remain
        result = buffer.get()
        assert len(result) == 1
        assert result.iloc[0]["value"] == "new"

    def test_buffer_max_rows(self):
        """Test max_rows limit."""
        import time
        import pandas as pd

        buffer = TimeSeriesBuffer(max_age_seconds=3600, max_rows=5)

        # Add 10 rows
        for i in range(10):
            df = pd.DataFrame({
                "timestamp_ms": [time.time() * 1000],
                "value": [i],
            })
            buffer.append(df)

        # Should only keep last 5
        assert len(buffer) == 5

    def test_buffer_clear(self):
        """Test clear operation."""
        import time
        import pandas as pd

        buffer = TimeSeriesBuffer(max_age_seconds=60.0)

        df = pd.DataFrame({
            "timestamp_ms": [time.time() * 1000],
            "value": [1],
        })
        buffer.append(df)
        assert len(buffer) == 1

        buffer.clear()
        assert len(buffer) == 0

    def test_buffer_last_timestamp(self):
        """Test last_timestamp property."""
        import time
        import pandas as pd

        buffer = TimeSeriesBuffer(max_age_seconds=60.0)
        assert buffer.last_timestamp is None

        ts = time.time() * 1000
        df = pd.DataFrame({
            "timestamp_ms": [ts],
            "value": [1],
        })
        buffer.append(df)

        assert buffer.last_timestamp == ts

    def test_buffer_empty(self):
        """Test empty buffer behavior."""
        buffer = TimeSeriesBuffer(max_age_seconds=60.0)
        assert len(buffer) == 0
        assert buffer.get().empty
        assert buffer.last_timestamp is None

    def test_buffer_prune_on_read_idle_stream(self):
        """Test that stale rows are removed on read even without new appends.

        Regression test: When stream goes idle, get() should still prune
        old data to maintain the sliding window contract.
        """
        import time
        import pandas as pd
        from unittest.mock import patch

        # Use fixed timestamps for deterministic test (no sleep needed)
        start_time = 1000.0

        with patch('time.time', return_value=start_time):
            buffer = TimeSeriesBuffer(max_age_seconds=60.0)

            # Add data at start_time
            df = pd.DataFrame({
                "timestamp_ms": [start_time * 1000],
                "value": ["fresh"],
            })
            buffer.append(df)

            # Data should be present at start_time
            assert len(buffer) == 1
            assert len(buffer.get()) == 1

        # Now simulate time passing beyond max_age_seconds
        # Move time forward by 61 seconds (beyond the 60s max_age)
        with patch('time.time', return_value=start_time + 61):
            # Without any append, get() should prune stale data
            result = buffer.get()
            assert result.empty, "Stale data should be pruned on read"

            # len() should also reflect pruned state
            assert len(buffer) == 0

            # last_timestamp should be None after pruning
            assert buffer.last_timestamp is None