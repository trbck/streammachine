"""
Custom Storage Backend Example for StreamMachine

This example demonstrates implementing alternative storage backends:
- RedisObjectStorage for persistent storage
- SQLite storage implementation
- Custom storage interface

Run with: python custom_storage_backend.py
"""
import asyncio
import json
from typing import Any, Optional
from streammachine import App, Message


# =============================================================================
# Custom Storage Interface
# =============================================================================

class CustomStorage:
    """Interface for custom storage backends.

    Implement this interface to create custom storage backends.
    """

    async def read(self, key: str, default: Any = None) -> Any:
        """Read value from storage."""
        raise NotImplementedError

    async def write(self, key: str, value: Any) -> None:
        """Write value to storage."""
        raise NotImplementedError

    async def delete(self, key: str) -> bool:
        """Delete key from storage."""
        raise NotImplementedError

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        raise NotImplementedError

    async def keys(self) -> list:
        """List all keys."""
        raise NotImplementedError


# =============================================================================
# In-Memory Storage (for demonstration)
# =============================================================================

class MemoryStorage(CustomStorage):
    """Simple in-memory storage for testing/demonstration.

    Note: Not suitable for multiprocess or persistence.
    """

    def __init__(self):
        self._data: dict = {}

    async def read(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    async def write(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> bool:
        if key in self._data:
            del self._data[key]
            return True
        return False

    async def exists(self, key: str) -> bool:
        return key in self._data

    async def keys(self) -> list:
        return list(self._data.keys())


# =============================================================================
# File-Based Storage (for demonstration)
# =============================================================================

class FileStorage(CustomStorage):
    """File-based persistent storage.

    Stores each key-value pair as a JSON file.
    Suitable for single-process applications.
    """

    def __init__(self, base_dir: str = "./storage"):
        import os
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

    def _path(self, key: str) -> str:
        import os
        # Sanitize key for filesystem
        safe_key = "".join(c if c.isalnum() else "_" for c in key)
        return os.path.join(self.base_dir, f"{safe_key}.json")

    async def read(self, key: str, default: Any = None) -> Any:
        import aiofiles
        try:
            async with aiofiles.open(self._path(key), "r") as f:
                content = await f.read()
                return json.loads(content)
        except FileNotFoundError:
            return default

    async def write(self, key: str, value: Any) -> None:
        import aiofiles
        async with aiofiles.open(self._path(key), "w") as f:
            await f.write(json.dumps(value))

    async def delete(self, key: str) -> bool:
        import os
        path = self._path(key)
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return False

    async def exists(self, key: str) -> bool:
        import os
        return os.path.exists(self._path(key))

    async def keys(self) -> list:
        import os
        return [f[:-5] for f in os.listdir(self.base_dir) if f.endswith(".json")]


# =============================================================================
# Redis Object Storage (using library's built-in)
# =============================================================================

async def use_redis_storage():
    """Demonstrate using RedisObjectStorage from the library."""
    try:
        from streammachine import RedisObjectStorage

        storage = RedisObjectStorage(redis_host="localhost", redis_port=6379)

        # Store complex object
        await storage.store_with_pickle("my_key", {
            "data": "value",
            "nested": {"key": "value"},
            "list": [1, 2, 3],
        })

        # Retrieve object
        obj = await storage.retrieve_with_pickle("my_key")
        print(f"Retrieved: {obj}")

        # List keys
        keys = await storage.list_keys("my_*")
        print(f"Keys: {keys}")

        await storage.close()

    except ImportError:
        print("RedisObjectStorage not available. Install with: pip install redis")


# =============================================================================
# Usage Example
# =============================================================================

app = App(name="storage_example", to_scan=True)

# Use custom storage (in production, you'd inject this)
custom_storage = MemoryStorage()


@app.timer(1)
async def producer():
    """Produce messages."""
    await app.send("data_stream", {"key": f"value_{asyncio.get_event_loop().time():.0f}"})


@app.agent("data_stream", group="processors")
async def processor(record: Message):
    """Process messages using custom storage."""
    # Use custom storage instead of app.storage
    counter = await custom_storage.read("counter", default=0)
    await custom_storage.write("counter", counter + 1)

    # Store per-key data
    key = record.message.get("key", "unknown")
    await custom_storage.write(f"data_{key}", record.message)

    print(f"[Processor] Processed: {key}, Total: {counter + 1}")


@app.timer(5)
async def show_storage_state():
    """Display current storage state."""
    keys = await custom_storage.keys()
    counter = await custom_storage.read("counter", default=0)

    print(f"\n[Storage] Keys: {len(keys)}, Counter: {counter}")
    print(f"[Storage] Sample keys: {keys[:5]}\n")


if __name__ == "__main__":
    print("Starting custom storage backend example...")
    print("This example demonstrates:")
    print("  - Custom storage interface")
    print("  - In-memory storage implementation")
    print("  - File-based storage implementation")
    print("  - Redis object storage usage")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")