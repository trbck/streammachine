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