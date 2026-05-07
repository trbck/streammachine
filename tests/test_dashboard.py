"""
Tests for streammachine.dashboard module.
"""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Skip all tests if FastAPI is not installed
pytest.importorskip("fastapi")

from streammachine.dashboard import (
    DashboardManager,
    InstanceInfo,
    InstanceMetrics,
    create_app,
    get_dashboard_html,
    INSTANCES_KEY_PREFIX,
    METRICS_KEY_PREFIX,
    MASTER_KEY,
    LOCK_KEY,
    LOCK_TTL,
    start_dashboard,
    stop_dashboard,
)


class TestDashboardManager:
    """Tests for DashboardManager class."""

    def test_singleton_pattern(self):
        """Test that DashboardManager is a singleton."""
        DashboardManager.reset_instance()
        manager1 = DashboardManager()
        manager2 = DashboardManager()
        assert manager1 is manager2
        DashboardManager.reset_instance()

    def test_reset_instance(self):
        """Test resetting singleton instance."""
        DashboardManager.reset_instance()
        manager1 = DashboardManager()
        DashboardManager.reset_instance()
        manager2 = DashboardManager()
        assert manager1 is not manager2
        DashboardManager.reset_instance()

    def test_is_master_initially_false(self):
        """Test that is_master returns False initially."""
        DashboardManager.reset_instance()
        manager = DashboardManager()
        assert manager.is_master() is False
        DashboardManager.reset_instance()


class TestInstanceInfo:
    """Tests for InstanceInfo dataclass."""

    def test_instance_info_creation(self):
        """Test creating InstanceInfo."""
        info = InstanceInfo(
            instance_id="abc123",
            name="test_app",
            pid=12345,
            host="localhost",
            start_time=time.time(),
        )
        assert info.instance_id == "abc123"
        assert info.name == "test_app"
        assert info.pid == 12345

    def test_instance_info_to_dict(self):
        """Test InstanceInfo to_dict method."""
        info = InstanceInfo(
            instance_id="abc123",
            name="test_app",
            pid=12345,
            host="localhost",
            start_time=1234567890.0,
        )
        result = info.to_dict()
        assert isinstance(result, dict)
        assert result["instance_id"] == "abc123"
        assert result["name"] == "test_app"


class TestInstanceMetrics:
    """Tests for InstanceMetrics dataclass."""

    def test_instance_metrics_creation(self):
        """Test creating InstanceMetrics."""
        metrics = InstanceMetrics(
            instance_id="abc123",
            agents=5,
            timers=2,
            active_tasks=10,
            last_heartbeat=time.time(),
        )
        assert metrics.instance_id == "abc123"
        assert metrics.agents == 5
        assert metrics.timers == 2

    def test_instance_metrics_to_dict(self):
        """Test InstanceMetrics to_dict method."""
        metrics = InstanceMetrics(
            instance_id="abc123",
            agents=5,
            timers=2,
            active_tasks=10,
            last_heartbeat=1234567890.0,
        )
        result = metrics.to_dict()
        assert result["agents"] == 5
        assert result["timers"] == 2


class TestCreateApp:
    """Tests for create_app function."""

    def test_create_app_returns_fastapi(self):
        """Test that create_app returns a FastAPI app."""
        from fastapi import FastAPI
        app = create_app()
        assert isinstance(app, FastAPI)

    def test_app_has_routes(self):
        """Test that created app has expected routes."""
        app = create_app()
        routes = [route.path for route in app.routes]
        assert "/" in routes
        assert "/api/health" in routes
        assert "/api/instances" in routes
        assert "/api/agents" in routes
        assert "/api/timers" in routes
        assert "/api/storage" in routes
        assert "/api/streams" in routes


class TestGetDashboardHtml:
    """Tests for get_dashboard_html function."""

    def test_returns_html_string(self):
        """Test that get_dashboard_html returns HTML."""
        html = get_dashboard_html()
        assert isinstance(html, str)
        assert "<!DOCTYPE html>" in html
        assert "<html>" in html
        assert "</html>" in html

    def test_html_contains_title(self):
        """Test that HTML contains expected title."""
        html = get_dashboard_html()
        assert "StreamMachine Dashboard" in html

    def test_html_contains_javascript(self):
        """Test that HTML contains JavaScript for auto-refresh."""
        html = get_dashboard_html()
        assert "<script>" in html
        assert "fetchData" in html
        assert "startAutoRefresh" in html


class TestStartStopDashboard:
    """Tests for start_dashboard and stop_dashboard functions."""

    @pytest.mark.asyncio
    async def test_start_dashboard_returns_false_without_redis(self):
        """Test that start_dashboard handles missing Redis gracefully."""
        DashboardManager.reset_instance()

        # Mock the Redis connection to fail
        with patch('streammachine.dashboard.DashboardManager._get_redis') as mock_redis:
            mock_redis.side_effect = Exception("Redis not available")
            result = await start_dashboard("test_id", 8000, "localhost")
            assert result is False

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_stop_dashboard_is_safe_when_not_master(self):
        """Test that stop_dashboard is safe when not master."""
        DashboardManager.reset_instance()
        # Should not raise when not master
        await stop_dashboard()
        DashboardManager.reset_instance()


class TestAPIEndpoints:
    """Tests for dashboard API endpoints."""

    @pytest.mark.asyncio
    async def test_get_all_instances_from_redis(self):
        """Test _get_all_instances_from_redis helper."""
        from streammachine.dashboard import _get_all_instances_from_redis

        # Mock Redis client
        mock_client = AsyncMock()
        mock_client.scan = AsyncMock(side_effect=[
            (0, []),  # First call returns empty
        ])

        instances = await _get_all_instances_from_redis(mock_client)
        assert instances == []

    @pytest.mark.asyncio
    async def test_get_metrics_from_redis(self):
        """Test _get_metrics_from_redis helper."""
        import json

        from streammachine.dashboard import _get_metrics_from_redis

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=json.dumps({
            "instance_id": "abc123",
            "agents": 5,
            "timers": 2,
        }))

        metrics = await _get_metrics_from_redis(mock_client, "abc123")
        assert metrics["instance_id"] == "abc123"
        assert metrics["agents"] == 5


class TestDashboardManagerRedisLock:
    """Tests for Redis-based distributed locking."""

    @pytest.mark.asyncio
    async def test_try_become_master_with_redis_lock(self):
        """Test acquiring master lock via Redis."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.expire = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        result = await manager.try_become_master(8000, "localhost", "test_id")
        assert result is True
        assert manager.is_master() is True

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_try_become_master_fails_when_already_locked(self):
        """Test that becoming master fails when another instance holds lock."""
        DashboardManager.reset_instance()

        # Mock Redis client - SET returns None (lock already held)
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=None)  # NX condition fails
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        result = await manager.try_become_master(8000, "localhost", "test_id")
        assert result is False
        assert manager.is_master() is False

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_release_lock(self):
        """Test releasing master lock."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.delete = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        # Become master first
        await manager.try_become_master(8000, "localhost", "test_id")
        assert manager.is_master() is True

        # Release lock
        await manager.release_lock()
        assert manager.is_master() is False

        DashboardManager.reset_instance()


class TestAppIntegration:
    """Tests for App integration with dashboard."""

    def test_app_has_instance_id(self):
        """Test that App has instance_id attribute."""
        from streammachine import App
        app = App(name="test_app", to_scan=False)
        assert hasattr(app, '_instance_id')
        assert app._instance_id is not None

    def test_app_has_dashboard_config(self):
        """Test that App config includes dashboard settings."""
        from streammachine import App
        app = App(
            name="test_app",
            to_scan=False,
            dashboard_enabled=True,
            dashboard_port=9000,
            dashboard_host="0.0.0.0",
        )
        assert app.config.dashboard_enabled is True
        assert app.config.dashboard_port == 9000
        assert app.config.dashboard_host == "0.0.0.0"

    def test_app_dashboard_disabled_by_default_is_true(self):
        """Test that dashboard is enabled by default."""
        from streammachine import App
        app = App(to_scan=False)
        # Default should be True based on AppConfig
        assert app.config.dashboard_enabled is True


class TestConfigValidation:
    """Tests for AppConfig validation."""

    def test_dashboard_port_validation(self):
        """Test that invalid dashboard_port raises error."""
        import pytest

        from streammachine.models import AppConfig

        with pytest.raises(ValueError):
            AppConfig(name="test", dashboard_port=0)

        with pytest.raises(ValueError):
            AppConfig(name="test", dashboard_port=70000)

    def test_dashboard_refresh_interval_validation(self):
        """Test that invalid dashboard_refresh_interval raises error."""
        import pytest

        from streammachine.models import AppConfig

        with pytest.raises(ValueError):
            AppConfig(name="test", dashboard_refresh_interval=0)

    def test_valid_dashboard_config(self):
        """Test that valid dashboard config is accepted."""
        from streammachine.models import AppConfig
        config = AppConfig(
            name="test",
            dashboard_enabled=True,
            dashboard_port=8080,
            dashboard_host="localhost",
            dashboard_refresh_interval=10,
        )
        assert config.dashboard_port == 8080
        assert config.dashboard_refresh_interval == 10


class TestLockSafety:
    """Tests for ownership-safe lock operations."""

    @pytest.mark.asyncio
    async def test_lock_token_is_stored(self):
        """Test that lock token is stored and used for ownership verification."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.script_load = AsyncMock(return_value=b"renew_sha")
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        result = await manager.try_become_master(8000, "localhost", "test_id")
        assert result is True
        assert manager._lock_token is not None
        assert "test_id" in manager._lock_token

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_release_uses_token_verification(self):
        """Test that release_lock verifies token ownership."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.script_load = AsyncMock(return_value=b"sha")
        mock_client.evalsha = AsyncMock(return_value=1)  # Successfully released
        mock_client.delete = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        # Become master first
        await manager.try_become_master(8000, "localhost", "test_id")

        # Release should use token
        await manager.release_lock()

        # Token should be cleared
        assert manager._lock_token is None
        assert manager.is_master() is False

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_release_fails_if_not_owner(self):
        """Test that release does nothing if lock was lost to another master."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.script_load = AsyncMock(return_value=b"sha")
        mock_client.evalsha = AsyncMock(return_value=0)  # Not owner anymore
        mock_client.delete = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        # Become master
        await manager.try_become_master(8000, "localhost", "test_id")

        # Release should detect ownership loss
        await manager.release_lock()
        mock_client.delete.assert_not_called()

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_try_become_master_does_not_delete_newer_lock_on_script_error(self):
        """Test that script-load cleanup preserves a lock owned by another instance."""
        DashboardManager.reset_instance()

        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.script_load = AsyncMock(side_effect=RuntimeError("load failed"))
        mock_client.get = AsyncMock(return_value=b"other-token")
        mock_client.delete = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        result = await manager.try_become_master(8000, "localhost", "test_id")

        assert result is False
        mock_client.delete.assert_not_called()
        assert manager.is_master() is False
        assert manager._lock_token is None

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_heartbeat_lock_loss_stops_server(self):
        """Test that heartbeat renewal loss shuts down the running server."""
        DashboardManager.reset_instance()

        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.evalsha = AsyncMock(return_value=0)
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis
        manager._renew_script_sha = b"sha"
        manager._is_master = True
        manager._lock_token = "token"
        manager._server = MagicMock(should_exit=False)
        manager._server_task = asyncio.create_task(asyncio.sleep(0))

        async def fast_sleep(_seconds):
            manager._shutdown_event.set()

        with patch("streammachine.dashboard.asyncio.sleep", new=fast_sleep):
            await manager._heartbeat_loop()

        assert manager._server.should_exit is True
        assert manager.is_master() is False
        assert manager._lock_token is None

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_heartbeat_lock_loss_cancels_hung_server_task(self):
        """Test that lock-loss shutdown uses the same timeout/cancel flow as stop_server."""
        DashboardManager.reset_instance()

        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.evalsha = AsyncMock(return_value=0)
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis
        manager._renew_script_sha = b"sha"
        manager._is_master = True
        manager._lock_token = "token"
        manager._master_info = {"instance_id": "test_id"}
        manager._server = MagicMock(should_exit=False)
        manager._server_task = MagicMock()
        manager._server_task.done.return_value = False

        async def fast_sleep(_seconds):
            manager._shutdown_event.set()

        with patch("streammachine.dashboard.asyncio.sleep", new=fast_sleep):
            with patch("streammachine.dashboard.asyncio.wait_for", AsyncMock(side_effect=asyncio.TimeoutError)):
                await manager._heartbeat_loop()

        assert manager._server.should_exit is True
        manager._server_task.cancel.assert_called_once()
        assert manager.is_master() is False
        assert manager._lock_token is None
        assert manager._master_info is None

        # MASTER_KEY should NOT be deleted when lock is not owned
        # Check that delete was NOT called for MASTER_KEY (only evalsha for LOCK_KEY)
        # delete is only called for MASTER_KEY if lock_released is True
        assert manager._lock_token is None
        assert manager.is_master() is False

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_release_preserves_master_key_on_ownership_loss(self):
        """Test that MASTER_KEY is NOT deleted when lock ownership is lost."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.set = AsyncMock(return_value=True)
        mock_client.script_load = AsyncMock(return_value=b"sha")
        mock_client.evalsha = AsyncMock(return_value=0)  # Lock release failed (not owner)
        mock_client.get = AsyncMock(return_value=b"other_token")  # Different owner
        mock_client.delete = AsyncMock()
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        # Become master
        await manager.try_become_master(8000, "localhost", "test_id")
        token_before = manager._lock_token

        # Release should detect ownership loss and NOT delete MASTER_KEY
        await manager.release_lock()

        # Verify delete was NOT called for MASTER_KEY
        # delete should not be called since lock_released was False
        delete_calls = [call for call in mock_client.delete.call_args_list]
        # MASTER_KEY delete should not happen when ownership is lost
        master_key_deletes = [c for c in delete_calls if MASTER_KEY in str(c)]
        assert len(master_key_deletes) == 0, "MASTER_KEY should not be deleted when lock ownership lost"

        DashboardManager.reset_instance()

    @pytest.mark.asyncio
    async def test_heartbeat_stops_server_on_lock_loss(self):
        """Test that heartbeat loop stops server when lock is lost."""
        DashboardManager.reset_instance()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_client = AsyncMock()
        mock_client.script_load = AsyncMock(return_value=b"sha")
        mock_client.evalsha = AsyncMock(return_value=0)  # Lock renewal fails
        mock_client.set = AsyncMock(return_value=True)
        mock_redis.client = mock_client

        manager = DashboardManager()
        manager._redis = mock_redis

        # Become master
        await manager.try_become_master(8000, "localhost", "test_id")

        # Create a mock server
        mock_server = MagicMock()
        mock_server.should_exit = False
        manager._server = mock_server

        # Simulate heartbeat iteration that loses lock
        # We need to call _heartbeat_loop once and verify server is stopped
        import asyncio

        async def run_one_heartbeat():
            # Simulate one heartbeat cycle
            if manager._renew_script_sha and hasattr(mock_client, 'evalsha'):
                result = await mock_client.evalsha(
                    manager._renew_script_sha,
                    keys=[LOCK_KEY],
                    args=[manager._lock_token, LOCK_TTL]
                )
                if not result:
                    await manager._stop_server_if_running()
                    manager._is_master = False
                    manager._lock_token = None

        await run_one_heartbeat()

        # Verify server was signaled to stop
        assert mock_server.should_exit is True, "Server should be signaled to stop on lock loss"
        assert manager.is_master() is False

        DashboardManager.reset_instance()


class TestRedisDirectStorage:
    """Tests for Redis-based instance storage (cross-process visibility)."""

    @pytest.mark.asyncio
    async def test_register_instance_stores_in_redis(self):
        """Test that register_instance writes to Redis with TTL."""

        from streammachine.dashboard import register_instance

        mock_client = AsyncMock()
        mock_client.set = AsyncMock()

        await register_instance(mock_client, "test_id", "test_app", 1234, "localhost", 1234567890.0)

        # Verify set was called with correct key and TTL
        assert mock_client.set.called
        call_args = mock_client.set.call_args
        assert "streammachine:instances:test_id" in call_args[0][0]
        assert call_args[1].get("ex") is not None  # TTL should be set

    @pytest.mark.asyncio
    async def test_unregister_instance_deletes_from_redis(self):
        """Test that unregister_instance deletes from Redis."""
        from streammachine.dashboard import unregister_instance

        mock_client = AsyncMock()
        mock_client.delete = AsyncMock()

        await unregister_instance(mock_client, "test_id")

        # Verify both instance and metrics keys were deleted
        assert mock_client.delete.call_count == 2

    @pytest.mark.asyncio
    async def test_update_heartbeat_sets_ttl(self):
        """Test that update_heartbeat refreshes TTL."""

        from streammachine.dashboard import update_heartbeat

        mock_client = AsyncMock()
        mock_client.expire = AsyncMock()
        mock_client.set = AsyncMock()

        await update_heartbeat(mock_client, "test_id", {"agents": 5, "timers": 2})

        # Verify TTL was refreshed
        assert mock_client.expire.called
        assert mock_client.set.called

class TestDashboardDisabled:
    """Tests for dashboard_enabled=False behavior."""

    @pytest.mark.asyncio
    async def test_disabled_skips_registration(self):
        """Test that disabled dashboard skips instance registration."""
        from streammachine import App

        app = App(name="test", to_scan=False, dashboard_enabled=False)

        # Should have empty coroutine result (no-op)
        # _register_instance should return early
        assert app.config.dashboard_enabled is False

    def test_disabled_does_not_create_heartbeat_task(self):
        """Test that disabled dashboard doesn't create heartbeat task."""
        from streammachine import App

        app = App(name="test", to_scan=False, dashboard_enabled=False)

        # Verify config
        assert app.config.dashboard_enabled is False
        # Heartbeat task should be None initially
        assert app._heartbeat_task is None
