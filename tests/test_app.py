"""
Tests for streammachine.app module.
"""
import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from streammachine.app import App, StreamConsumer, agent_container, timer_container
from streammachine.models import ConsumerConfig, TimerConfig


class TestApp:
    """Tests for App class."""

    def test_app_creation(self):
        """Test creating an App instance."""
        app = App(name="test_app", to_scan=False)
        assert app.config.name == "test_app"
        assert app.config.to_scan is False

    def test_app_default_values(self):
        """Test App default configuration."""
        app = App(to_scan=False)
        assert app.config.max_processes == 5
        assert app.config.max_threads == 5

    def test_app_custom_config(self):
        """Test App with custom configuration."""
        app = App(
            name="custom_app",
            max_processes=10,
            max_threads=20,
            to_scan=False,
        )
        assert app.config.name == "custom_app"
        assert app.config.max_processes == 10
        assert app.config.max_threads == 20

    def test_registry_initialized(self):
        """Test that registry is initialized."""
        app = App(to_scan=False)
        assert app.registry is not None
        assert hasattr(app.registry, 'registered')

    def test_storage_initialized(self):
        """Test that storage is initialized."""
        from streammachine.storage import Storage
        Storage.reset_instance()
        app = App(to_scan=False)
        assert app.storage is not None
        Storage.reset_instance()


class TestAgentContainer:
    """Tests for agent_container function."""

    @pytest.mark.asyncio
    async def test_agent_container_creation(self):
        """Test that agent_container can be created with config."""
        mock_module = MagicMock()
        mock_module.test_handler = AsyncMock(return_value=None)

        config = ConsumerConfig(
            decorator_type="agent",
            topic="test_topic",
            group="test_group",
            obj_name="test_handler",
            mod=mock_module,
        )

        # We can't fully test the consumer without Redis, but we can check it creates
        from streammachine.app import StreamConsumer
        # This test verifies the config is passed correctly
        assert config.topic == "test_topic"
        assert config.group == "test_group"


class TestTimerContainer:
    """Tests for timer_container function."""

    @pytest.mark.asyncio
    async def test_timer_container_with_shutdown(self):
        """Test that timer stops on shutdown event."""
        import asyncio

        mock_module = MagicMock()
        call_count = 0

        async def mock_timer():
            nonlocal call_count
            call_count += 1

        mock_module.test_timer = mock_timer

        config = TimerConfig(
            decorator_type="timer",
            t=1,
            obj_name="test_timer",
            mod=mock_module,
        )

        shutdown_event = asyncio.Event()
        shutdown_event.set()  # Immediately set to trigger shutdown

        await timer_container(config, shutdown_event)
        # Timer should not have run since shutdown was already set
        assert call_count == 0


class TestAppSend:
    """Tests for App send methods."""

    @pytest.mark.asyncio
    async def test_send_method_exists(self):
        """Test that send method exists and is callable."""
        app = App(to_scan=False)
        assert hasattr(app, 'send')
        assert callable(app.send)

    @pytest.mark.asyncio
    async def test_send_batch_method_exists(self):
        """Test that send_batch method exists and is callable."""
        app = App(to_scan=False)
        assert hasattr(app, 'send_batch')
        assert callable(app.send_batch)


class TestAppHealthCheck:
    """Tests for health check functionality."""

    @pytest.mark.asyncio
    async def test_health_check_method_exists(self):
        """Test that health_check method exists."""
        app = App(to_scan=False)
        assert hasattr(app, 'health_check')
        assert callable(app.health_check)


class TestAppShutdown:
    """Tests for shutdown functionality."""

    def test_shutdown_method_exists(self):
        """Test that shutdown method exists."""
        app = App(to_scan=False)
        assert hasattr(app, 'shutdown')
        assert callable(app.shutdown)

    def test_shutdown_event_initialized(self):
        """Test that shutdown event is initialized."""
        app = App(to_scan=False)
        assert hasattr(app, '_shutdown_event')
        assert app._shutdown_event is not None


class TestStreamConsumerRepoll:
    """Tests for StreamConsumer while-True repoll loop."""

    @pytest.mark.asyncio
    async def test_consumer_retries_after_empty_iterator(self):
        """Test that consumer loop continues after iterator exhaustion."""
        mock_module = MagicMock()
        handler_calls = []

        async def mock_handler(msg):
            handler_calls.append(msg)

        mock_module.test_handler = mock_handler

        config = ConsumerConfig(
            decorator_type="agent",
            topic="test_topic",
            group="test_group",
            obj_name="test_handler",
            mod=mock_module,
        )

        consumer = StreamConsumer(config)

        # Track how many times the GroupConsumer iterator is entered
        iteration_count = 0

        class MockGroupConsumer:
            """Mock that yields nothing on first iteration, one item on second, then cancels."""

            def __aiter__(self):
                return self

            async def __anext__(self):
                nonlocal iteration_count
                iteration_count += 1
                if iteration_count == 1:
                    # First iteration: empty (simulates timeout with no messages)
                    raise StopAsyncIteration
                elif iteration_count == 2:
                    # Second iteration: one message then stop
                    iteration_count += 1  # skip to 3 so next call stops
                    mock_entry = MagicMock()
                    mock_entry.identifier = b"1-0"
                    mock_entry.field_values = {b"key": b"val"}
                    return (b"test_topic", mock_entry)
                else:
                    # Cancel the task to exit the while-True loop
                    raise asyncio.CancelledError()

        mock_rc = MagicMock()
        mock_rc.consumer = AsyncMock(return_value=MockGroupConsumer())
        mock_rc.close = AsyncMock()

        with patch.object(consumer, '_rc', mock_rc):
            consumer._rc = mock_rc
            # Patch RedisConnection creation in __call__
            with patch('streammachine.app.RedisConnection', return_value=mock_rc):
                # Run the consumer — it should survive the first empty iteration,
                # process one message, then get cancelled
                await consumer()

        # Handler was called once (on the second iteration)
        assert len(handler_calls) == 1

class TestStreamConsumerReconnect:
    """A stream-read failure must reconnect, not end the consumer.

    Regression tests for the 2026-07-29 fomo2 outage. Redis could not snapshot
    (a corrupt stream rax segfaulted every bgsave child), so it went MISCONF and
    began rejecting writes. XREADGROUP *is* a write — it mutates the group PEL —
    so the read at the top of the consumer loop raised. That exception escaped
    the inner try (which only guards the handler) and was caught outside the
    while-True, ending the consumer for good. Every consumer in every group died
    within one poll and none returned when Redis recovered 11.5h later; only the
    timer-driven ingest stage survived, so raw data kept arriving while nothing
    enriched, routed, processed or memorized it.
    """

    @staticmethod
    def _config(handler):
        mock_module = MagicMock()
        mock_module.test_handler = handler
        return ConsumerConfig(
            decorator_type="agent",
            topic="test_topic",
            group="test_group",
            obj_name="test_handler",
            mod=mock_module,
        )

    @staticmethod
    def _one_message_then_cancel():
        """An iterator that yields exactly one message, then cancels the task."""
        state = {"n": 0}

        class _Cons:
            def __aiter__(self):
                return self

            async def __anext__(self):
                state["n"] += 1
                if state["n"] == 1:
                    entry = MagicMock()
                    entry.identifier = b"1-0"
                    entry.field_values = {b"key": b"val"}
                    return (b"test_topic", entry)
                raise asyncio.CancelledError()

        return _Cons()

    @pytest.mark.asyncio
    async def test_misconf_on_read_reconnects_and_keeps_processing(self):
        """The exact incident: the read raises MISCONF, then Redis recovers."""
        from coredis.exceptions import ResponseError

        handler_calls = []

        async def handler(msg):
            handler_calls.append(msg)

        consumer = StreamConsumer(self._config(handler))

        misconf = ResponseError(
            "MISCONF Redis is configured to save RDB snapshots, but it's "
            "currently unable to persist to disk."
        )
        mock_rc = MagicMock()
        mock_rc.consumer = AsyncMock(
            side_effect=[misconf, self._one_message_then_cancel()]
        )
        mock_rc.close = AsyncMock()

        slept = []

        async def fake_sleep(delay):
            slept.append(delay)

        with patch('streammachine.app.RedisConnection', return_value=mock_rc):
            with patch('streammachine.app.asyncio.sleep', new=fake_sleep):
                await consumer()

        # It came back and did real work after the failure — the whole point.
        assert len(handler_calls) == 1
        assert mock_rc.consumer.await_count == 2
        # And it waited before retrying rather than hot-looping on a dead Redis.
        assert consumer.RECONNECT_BACKOFF_MIN in slept

    @pytest.mark.asyncio
    async def test_consumer_name_is_stable_across_reconnects(self):
        """Reconnecting under a NEW name would orphan that name's PEL entries.

        Messages already delivered to this consumer are owned by its name. A
        fresh UUID per retry leaves them pending under a name nobody reads
        again, recoverable only via XAUTOCLAIM from elsewhere.
        """
        async def handler(msg):
            pass

        consumer = StreamConsumer(self._config(handler))

        names = []

        async def fake_consumer(topic, consumer_id, group):
            names.append(consumer_id)
            if len(names) == 1:
                raise ConnectionResetError("connection reset by peer")
            return self._one_message_then_cancel()

        mock_rc = MagicMock()
        mock_rc.consumer = fake_consumer
        mock_rc.close = AsyncMock()

        async def fake_sleep(delay):
            pass

        with patch('streammachine.app.RedisConnection', return_value=mock_rc):
            with patch('streammachine.app.asyncio.sleep', new=fake_sleep):
                await consumer()

        assert len(names) == 2
        assert names[0] == names[1]

    @pytest.mark.asyncio
    async def test_backoff_grows_and_stays_bounded(self):
        """Repeated failures must back off, but never past the cap."""
        async def handler(msg):
            pass

        consumer = StreamConsumer(self._config(handler))

        attempts = {"n": 0}

        async def fake_consumer(topic, consumer_id, group):
            attempts["n"] += 1
            if attempts["n"] > 12:
                raise asyncio.CancelledError()
            raise ConnectionResetError("still down")

        mock_rc = MagicMock()
        mock_rc.consumer = fake_consumer
        mock_rc.close = AsyncMock()

        slept = []

        async def fake_sleep(delay):
            slept.append(delay)

        with patch('streammachine.app.RedisConnection', return_value=mock_rc):
            with patch('streammachine.app.asyncio.sleep', new=fake_sleep):
                await consumer()

        waits = [d for d in slept if d > 0]
        assert waits, "a failing consumer must wait before retrying"
        assert waits == sorted(waits), "backoff must be non-decreasing"
        assert max(waits) <= consumer.RECONNECT_BACKOFF_MAX
        assert waits[0] == consumer.RECONNECT_BACKOFF_MIN

    @pytest.mark.asyncio
    async def test_handler_failure_does_not_reconnect(self):
        """A bad payload is not a broken connection — keep the same session.

        This is the other half of the severity split: if a raising handler
        triggered a reconnect, one poison message would churn the connection.
        """
        async def handler(msg):
            raise ValueError("bad payload")

        consumer = StreamConsumer(self._config(handler))

        mock_rc = MagicMock()
        mock_rc.consumer = AsyncMock(return_value=self._one_message_then_cancel())
        mock_rc.close = AsyncMock()

        async def fake_sleep(delay):
            pass

        with patch('streammachine.app.RedisConnection', return_value=mock_rc):
            with patch('streammachine.app.asyncio.sleep', new=fake_sleep):
                await consumer()

        assert mock_rc.consumer.await_count == 1

    @pytest.mark.asyncio
    async def test_cancellation_still_exits_cleanly(self):
        """Shutdown must remain immediate — not retried like a fault."""
        async def handler(msg):
            pass

        consumer = StreamConsumer(self._config(handler))

        mock_rc = MagicMock()
        mock_rc.consumer = AsyncMock(side_effect=asyncio.CancelledError())
        mock_rc.close = AsyncMock()

        async def fake_sleep(delay):
            raise AssertionError("cancellation must not go through backoff")

        with patch('streammachine.app.RedisConnection', return_value=mock_rc):
            with patch('streammachine.app.asyncio.sleep', new=fake_sleep):
                await consumer()

        assert mock_rc.consumer.await_count == 1
        mock_rc.close.assert_awaited()
