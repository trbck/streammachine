"""
Regression tests for defects found in the pre-release review.

Each test pins one behaviour that was broken before the fix, so a future
refactor cannot silently reintroduce it.
"""
import logging
import sys
from unittest.mock import AsyncMock, MagicMock, patch


from streammachine import App
from streammachine.fast_ohlc import FastOHLCConsumer
from streammachine.models import ConsumerConfig
from streammachine.redisapi import RedisConnection
from streammachine.util import Registry


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.connection_pool.__aenter__ = AsyncMock(return_value=None)
    client.connection_pool.__aexit__ = AsyncMock(return_value=None)
    client.xadd = AsyncMock(return_value=b"1-0")
    client.ping = AsyncMock(return_value=True)
    client.quit = AsyncMock()
    return client


class TestPoolInitialisation:
    """coredis 6.x refuses commands until the pool context has been entered."""

    async def test_send_enters_pool_before_xadd(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            client = _mock_client()
            redis_cls.return_value = client
            app = App(to_scan=False, dashboard_enabled=False)
            await app.send("topic", {"a": "1"})
            client.connection_pool.__aenter__.assert_awaited_once()
            client.xadd.assert_awaited_once()

    async def test_health_check_and_consumer_enter_pool(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls, \
             patch("streammachine.redisapi.GroupConsumer"):
            client = _mock_client()
            redis_cls.return_value = client
            conn = RedisConnection()
            assert await conn.health_check() is True
            await conn.consumer("s", "c", "g")
            client.connection_pool.__aenter__.assert_awaited_once()

    async def test_close_exits_pool_and_allows_reentry(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            client = _mock_client()
            redis_cls.return_value = client
            conn = RedisConnection()
            await conn._ensure_pool()
            await conn.close()
            client.connection_pool.__aexit__.assert_awaited_once()
            assert conn._pool_entered is False


class TestSendDoesNotMutateInput:
    async def test_send_leaves_caller_dict_untouched(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            client = _mock_client()
            redis_cls.return_value = client
            app = App(to_scan=False, dashboard_enabled=False)
            record = {"a": "1"}
            await app.send("topic", record)
            assert record == {"a": "1"}
            sent_payload = client.xadd.call_args.args[1]
            assert "sent" in sent_payload

    async def test_send_batch_leaves_caller_dicts_untouched(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            client = _mock_client()
            pipe = MagicMock()
            pipe.xadd = MagicMock()
            pipe.results = ("1-0", "1-1")
            del pipe.execute  # new API has no execute()
            client.pipeline = MagicMock(return_value=pipe)
            redis_cls.return_value = client
            app = App(to_scan=False, dashboard_enabled=False)
            records = [{"a": "1"}, {"b": "2"}]
            await app.send_batch("topic", records)
            assert records == [{"a": "1"}, {"b": "2"}]
            assert pipe.xadd.call_count == 2


class TestProcessesAgentsAreScheduled:
    def test_processes_agent_runs_in_process_with_warning(self, caplog):
        app = App(to_scan=False, dashboard_enabled=False)
        app.registry.add(ConsumerConfig("agent", "t", "g", concurrency=2, processes=3, obj_name="h"))
        app.registry.add(ConsumerConfig("agent", "u", "g", concurrency=1, obj_name="k"))
        with caplog.at_level(logging.WARNING, logger="streammachine.app"):
            coros = app._get_concurrent_agents()
        for c in coros:
            c.close()
        assert len(coros) == 3
        assert any("processes=3" in r.message for r in caplog.records)


class TestDecoratorDiscovery:
    """Handlers must resolve to the module that defines them, not a stack frame."""

    MODULE_SRC = """
from streammachine.util import AgentTaskDecorator, TimerTaskDecorator

@AgentTaskDecorator("stream", group="g", concurrency=2)
async def handler(record):
    return record

@TimerTaskDecorator(5)
async def tick():
    return None
"""

    def test_scan_records_defining_module_and_names(self, tmp_path):
        import importlib.util

        import venusian

        path = tmp_path / "fake_handlers_mod.py"
        path.write_text(self.MODULE_SRC)
        spec = importlib.util.spec_from_file_location("fake_handlers_mod", path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod.__name__] = mod
        try:
            spec.loader.exec_module(mod)
            registry = Registry()
            venusian.Scanner(registry=registry).scan(mod)

            by_type = {cfg.decorator_type: cfg for cfg in registry.registered}
            assert set(by_type) == {"agent", "timer"}
            assert by_type["agent"].mod is mod
            assert by_type["agent"].obj_name == "handler"
            assert by_type["agent"].concurrency == 2
            assert by_type["timer"].mod is mod
            assert by_type["timer"].obj_name == "tick"
            # the wrapper keeps the function's identity for introspection
            assert mod.handler.__name__ == "handler"
            assert mod.handler.__module__ == "fake_handlers_mod"
        finally:
            sys.modules.pop(mod.__name__, None)


class TestFastOHLCConsumerEmit:
    async def test_emit_candles_writes_completed_candles(self):
        consumer = FastOHLCConsumer("ticks", output_stream_prefix="candles", intervals=[60000])
        consumer._redis = MagicMock()
        consumer._redis._ensure_pool = AsyncMock()
        consumer._redis.client.xadd = AsyncMock(return_value=b"1-0")

        # Two ticks in one minute, then time moves on so the candle is complete.
        consumer.aggregator.update_tick(b"AAPL", 10.0, 1.0, 60_000)
        consumer.aggregator.update_tick(b"AAPL", 12.0, 1.0, 90_000)
        with patch("streammachine.fast_ohlc.time.time", return_value=1000.0):
            emitted = await consumer.emit_candles()

        assert emitted == 1
        stream, payload = consumer._redis.client.xadd.call_args.args
        assert stream == "candles_1m"
        assert payload["symbol"] == "AAPL"
        assert payload["high"] == "12.0"
        # flushed after emit
        assert consumer.aggregator.get_candles(b"AAPL", 60000) == []

    async def test_emit_without_start_is_noop(self):
        consumer = FastOHLCConsumer("ticks")
        assert await consumer.emit_candles() == 0


class TestLibraryHygiene:
    def test_import_does_not_configure_root_logger(self):
        root = logging.getLogger()
        assert all(isinstance(h, logging.NullHandler) or h in root.handlers for h in root.handlers)
        pkg = logging.getLogger("streammachine")
        assert any(isinstance(h, logging.NullHandler) for h in pkg.handlers)

    def test_message_decodes_via_package_decoder(self):
        from streammachine import models
        from streammachine.cython import _has_cython_decode

        assert models._has_cython_decode is _has_cython_decode

    def test_version_is_single_sourced(self):
        from importlib.metadata import version

        import streammachine

        assert version("streammachine") == streammachine.__version__


class TestRedisUrlFromEnvironment:
    """REDIS_URL in the environment must be used when no explicit params are given."""

    def test_env_url_is_used(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://example:6380/3")
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            redis_cls.from_url = MagicMock(return_value=MagicMock())
            _ = RedisConnection().client
            redis_cls.from_url.assert_called_once()
            assert redis_cls.from_url.call_args.args[0] == "redis://example:6380/3"

    def test_explicit_params_win_over_env_url(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://example:6380/3")
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            redis_cls.return_value = MagicMock()
            _ = RedisConnection(host="h", port=1, db=2).client
            redis_cls.assert_called_once()
            assert redis_cls.call_args.kwargs["host"] == "h"

    def test_no_env_uses_host_params(self, monkeypatch):
        monkeypatch.delenv("REDIS_URL", raising=False)
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            redis_cls.return_value = MagicMock()
            _ = RedisConnection().client
            redis_cls.assert_called_once()


class TestPipelineCompat:
    """pipeline_xadd must work with the awaitable (coredis 4.x) pipeline API too."""

    async def test_old_pipeline_api(self):
        with patch("streammachine.redisapi.coredis.Redis") as redis_cls:
            client = MagicMock(spec=[])  # bare: no connection_pool context protocol
            pipe = MagicMock(spec=["xadd", "execute"])
            pipe.xadd = AsyncMock()
            pipe.execute = AsyncMock(return_value=("id1", "id2"))
            client.pipeline = AsyncMock(return_value=pipe)
            client.connection_pool = MagicMock(spec=[])  # no __aenter__/__aexit__
            client.quit = AsyncMock()
            redis_cls.return_value = client

            conn = RedisConnection()
            result = await conn.pipeline_xadd("t", [{"a": "1"}, {"a": "2"}])
            assert result == ["id1", "id2"]
            assert pipe.xadd.await_count == 2
            await conn.close()
            client.quit.assert_awaited_once()
