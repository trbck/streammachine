"""
StreamMachine Storage Module

This module provides a singleton Storage class for sharing state between
agents, timers, and across process boundaries.

Why multiprocessing.Manager?
    Python's multiprocessing.Manager creates a separate process that manages
    shared state. This is essential for cross-process communication when using
    agents with `processes=N`. Each worker process can read/write to Storage
    and see changes made by other processes.

    Alternative approaches and why they don't work:
    - threading.Lock: Only works within a single process
    - asyncio.Lock: Only works within a single event loop
    - multiprocessing.Value/Array: Requires predefined data types

    The Manager provides a dict() that can store arbitrary Python objects
    and a Queue() for inter-process communication.

Design Decisions:
    - Deferred manager startup: Manager is created on first use to avoid
      issues with macOS spawn multiprocessing (can't pickle at module level)
    - Per-key locking: Each key has its own asyncio.Lock, preventing
      write contention on different keys
    - Optional read locking: By default, reads don't lock (faster) but
      lock_reading=True enables read locking for strong consistency

Example:
    Within-process state sharing::

        storage = Storage()
        await storage.write("counter", 0)

        @app.agent("events")
        async def process(msg):
            count = await storage.read("counter", default=0)
            await storage.write("counter", count + 1)

    Cross-process state sharing::

        @app.agent("data", processes=4)
        async def worker(msg):
            # Each process can read/write to shared storage
            state = await storage.read("shared_state", default={})
            state[msg.key] = msg.value
            await storage.write("shared_state", state)
"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import threading
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class Storage:
    """
    Singleton async storage manager using multiprocessing.Manager for shared state.

    Uses asyncio.Lock for per-key write protection. Thread-safe singleton pattern
    ensures only one instance exists across the application.

    Thread/Process Safety:
        - Singleton pattern uses double-checked locking with threading.Lock
        - Per-key asyncio.Lock prevents concurrent writes to same key
        - multiprocessing.Manager provides cross-process state sharing

    Memory Management:
        - shared_dict stores all values (in Manager process memory)
        - _key_locks are local to each process (Manager doesn't share Locks)
        - Call clear() to free memory when done

    Example:
        storage = Storage()
        await storage.write("key", {"data": "value"})
        value = await storage.read("key")
    """

    _instance: Optional[Storage] = None
    _lock: threading.Lock = threading.Lock()
    _initialized: bool = False

    def __new__(cls) -> Storage:
        """
        Create or return the singleton instance.

        Uses double-checked locking for thread safety.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._init_storage()
                    cls._instance = instance
                    logger.debug("Storage singleton created")
        return cls._instance

    def _init_storage(self) -> None:
        """Initialize the storage attributes (without multiprocessing.Manager)."""
        if self._initialized:
            return

        self.manager = None
        self.shared_dict: Optional[Dict[str, Any]] = None
        self.command_queue = None
        # Dictionary of locks, one for each key (write lock only)
        self._key_locks: Dict[str, asyncio.Lock] = {}
        self._locks_lock = threading.Lock()  # Protect _key_locks access
        self.lock_reading = False  # By default, don't lock during reading
        self._manager_started = False
        self._command_handler_future = None
        self._initialized = True
        logger.debug("Storage initialized (manager deferred)")

    def _ensure_manager(self) -> None:
        """Start multiprocessing.Manager if not already started.

        This is deferred from __init__ to avoid issues with macOS spawn
        multiprocessing when App is created at module level.
        """
        if self._manager_started:
            return
        self.manager = multiprocessing.Manager()
        self.shared_dict = self.manager.dict()
        self.command_queue = self.manager.Queue()
        self._manager_started = True
        logger.debug("Storage manager started")

    def _get_lock(self, key: str) -> asyncio.Lock:
        """
        Get or create a lock for the given key.

        Args:
            key: The key to get a lock for

        Returns:
            asyncio.Lock for the key
        """
        with self._locks_lock:
            if key not in self._key_locks:
                self._key_locks[key] = asyncio.Lock()
            return self._key_locks[key]

    def handle_commands(self) -> None:
        """Listen and handle incoming commands (blocking, for background process).

        Requires manager to be started via start().
        """
        logger.debug("Command handler started")
        while True:
            try:
                command, args, kwargs = self.command_queue.get()
                if command == "terminate":
                    logger.debug("Command handler terminating")
                    break
            except (BrokenPipeError, EOFError) as e:
                logger.debug("Command handler exiting after manager shutdown: %s", e)
                break
            except Exception as e:
                logger.error(f"Error in command handler: {e}")

    async def start(self) -> None:
        """Start the manager and listen for commands asynchronously (in executor)."""
        self._ensure_manager()
        if self._command_handler_future is not None:
            return
        loop = asyncio.get_event_loop()
        self._command_handler_future = loop.run_in_executor(None, self.handle_commands)

    async def terminate(self) -> None:
        """Terminate the command handler process."""
        if not self._manager_started:
            return
        await asyncio.to_thread(self.command_queue.put, ("terminate", [], {}))
        if self._command_handler_future is not None:
            try:
                await self._command_handler_future
            finally:
                self._command_handler_future = None
        logger.debug("Storage termination signal sent")

    def stop(self) -> None:
        """
        Stop the storage manager and clean up resources.

        This shuts down the multiprocessing manager, which terminates
        the background process and releases all shared resources.
        """
        if not self._manager_started:
            return
        try:
            self.manager.shutdown()
            logger.debug("Storage manager shut down")
        except Exception as e:
            logger.warning(f"Error shutting down storage manager: {e}")
        finally:
            self._command_handler_future = None
            self._manager_started = False
            self.manager = None
            self.shared_dict = None
            self.command_queue = None

    async def write(self, key: str, value: Any) -> None:
        """
        Asynchronously write a key-value pair to the shared dictionary.

        Uses per-key locking to prevent concurrent writes to the same key.

        Args:
            key: The key to write
            value: The value to store
        """
        self._ensure_manager()
        lock = self._get_lock(key)
        async with lock:
            self.shared_dict[key] = value
        logger.debug(f"Wrote value to key '{key}'")

    async def read(self, key: str, default: Any = None) -> Any:
        """
        Asynchronously read a value from the shared dictionary.

        Args:
            key: The key to read
            default: Default value if key doesn't exist

        Returns:
            The stored value or default
        """
        self._ensure_manager()
        if self.lock_reading:
            lock = self._get_lock(key)
            async with lock:
                return self.shared_dict.get(key, default)
        return self.shared_dict.get(key, default)

    async def delete(self, key: str) -> bool:
        """
        Delete a key from the shared dictionary.

        Args:
            key: The key to delete

        Returns:
            True if key was deleted, False if key didn't exist
        """
        self._ensure_manager()
        lock = self._get_lock(key)
        async with lock:
            if key in self.shared_dict:
                del self.shared_dict[key]
                logger.debug(f"Deleted key '{key}'")
                return True
            return False

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in the shared dictionary.

        Args:
            key: The key to check

        Returns:
            True if key exists, False otherwise
        """
        self._ensure_manager()
        return key in self.shared_dict

    async def keys(self) -> list:
        """
        Get all keys in the shared dictionary.

        Returns:
            List of keys
        """
        self._ensure_manager()
        return list(self.shared_dict.keys())

    async def clear(self) -> None:
        """Clear all keys from the shared dictionary."""
        self._ensure_manager()
        self.shared_dict.clear()
        with self._locks_lock:
            self._key_locks.clear()
        logger.debug("Storage cleared")

    @classmethod
    def reset_instance(cls) -> None:
        """
        Reset the singleton instance.

        This is primarily useful for testing.
        Warning: This will lose all stored data.
        """
        with cls._lock:
            if cls._instance is not None:
                if cls._instance._manager_started:
                    try:
                        cls._instance.manager.shutdown()
                    except Exception as e:
                        logger.warning(f"Error shutting down manager: {e}")
                cls._instance = None
                cls._initialized = False
                logger.debug("Storage instance reset")


# --- Cythonization candidates ---
# If you have any CPU-bound data processing, mark here for Cythonization.
# Example:
# def heavy_processing(...):
#     ... # Move to .pyx and use nogil for true parallelism
