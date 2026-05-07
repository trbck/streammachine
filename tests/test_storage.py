"""
Tests for streammachine.storage module.
"""
import asyncio
from unittest.mock import Mock

import pytest

from streammachine.storage import Storage


class TestStorage:
    """Tests for Storage singleton class."""

    def test_singleton_pattern(self, storage):
        """Test that Storage is a singleton."""
        s1 = Storage()
        s2 = Storage()
        assert s1 is s2

    def test_singleton_reset(self, storage):
        """Test that reset_instance creates a new instance."""
        s1 = Storage()
        Storage.reset_instance()
        s2 = Storage()
        assert s1 is not s2
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_write_and_read(self, storage):
        """Test writing and reading values."""
        await storage.write("test_key", {"data": "test_value"})
        result = await storage.read("test_key")
        assert result == {"data": "test_value"}

    @pytest.mark.asyncio
    async def test_read_nonexistent_key(self, storage):
        """Test reading a nonexistent key returns default."""
        result = await storage.read("nonexistent_key")
        assert result is None

        result = await storage.read("nonexistent_key", default="default_value")
        assert result == "default_value"

    @pytest.mark.asyncio
    async def test_delete(self, storage):
        """Test deleting a key."""
        await storage.write("key_to_delete", "value")
        deleted = await storage.delete("key_to_delete")
        assert deleted is True

        result = await storage.read("key_to_delete")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, storage):
        """Test deleting a nonexistent key returns False."""
        deleted = await storage.delete("nonexistent_key")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_exists(self, storage):
        """Test checking key existence."""
        await storage.write("existing_key", "value")
        assert await storage.exists("existing_key") is True
        assert await storage.exists("nonexistent_key") is False

    @pytest.mark.asyncio
    async def test_keys(self, storage):
        """Test listing all keys."""
        await storage.write("key1", "value1")
        await storage.write("key2", "value2")
        keys = await storage.keys()
        assert "key1" in keys
        assert "key2" in keys

    @pytest.mark.asyncio
    async def test_clear(self, storage):
        """Test clearing all keys."""
        await storage.write("key1", "value1")
        await storage.write("key2", "value2")
        await storage.clear()
        keys = await storage.keys()
        assert len(keys) == 0

    @pytest.mark.asyncio
    async def test_overwrite(self, storage):
        """Test overwriting an existing key."""
        await storage.write("key", "value1")
        await storage.write("key", "value2")
        result = await storage.read("key")
        assert result == "value2"

    @pytest.mark.asyncio
    async def test_complex_value(self, storage):
        """Test storing complex nested values."""
        complex_value = {
            "string": "text",
            "number": 42,
            "nested": {
                "list": [1, 2, 3],
                "dict": {"a": "b"},
            },
        }
        await storage.write("complex", complex_value)
        result = await storage.read("complex")
        assert result == complex_value


class TestStorageLazyInit:
    """Tests for Storage lazy manager initialization."""

    @pytest.mark.asyncio
    async def test_write_before_start_auto_inits(self):
        """Test that write() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        # Manager not started — write should lazily start it
        await s.write("auto_key", "auto_value")
        result = await s.read("auto_key")
        assert result == "auto_value"
        assert s._manager_started is True
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_read_before_start_auto_inits(self):
        """Test that read() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        result = await s.read("missing", default="fallback")
        assert result == "fallback"
        assert s._manager_started is True
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_keys_before_start_auto_inits(self):
        """Test that keys() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        result = await s.keys()
        assert result == []
        assert s._manager_started is True
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_exists_before_start_auto_inits(self):
        """Test that exists() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        result = await s.exists("nope")
        assert result is False
        assert s._manager_started is True
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_delete_before_start_auto_inits(self):
        """Test that delete() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        result = await s.delete("nope")
        assert result is False
        assert s._manager_started is True
        Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_clear_before_start_auto_inits(self):
        """Test that clear() auto-initializes manager if not started."""
        Storage.reset_instance()
        s = Storage()
        await s.clear()
        assert s._manager_started is True
        Storage.reset_instance()


class TestStorageCommandHandler:
    """Tests for Storage command handler shutdown behavior."""

    @pytest.mark.parametrize(
        ("side_effect", "expected_fragment"),
        [
            (BrokenPipeError("manager closed"), "manager closed"),
            (EOFError("queue closed"), "queue closed"),
        ],
    )
    def test_handle_commands_exits_on_shutdown_pipe_errors(self, side_effect, expected_fragment, caplog):
        """Test that expected manager shutdown pipe errors stop the loop cleanly."""
        Storage.reset_instance()
        storage = Storage()
        storage.command_queue = Mock()
        storage.command_queue.get.side_effect = side_effect

        with caplog.at_level("DEBUG"):
            storage.handle_commands()

        assert "Command handler exiting after manager shutdown" in caplog.text
        assert expected_fragment in caplog.text
        Storage.reset_instance()
