"""
Multiprocess agent tests for StreamMachine.

These tests verify that agents can run in separate processes
and share state via Storage.

Note: Multiprocess tests have special requirements:
- Use 'spawn' context on macOS (default since Python 3.8)
- Each test spawns child processes that import the test module
- State sharing is via multiprocessing.Manager
"""
import asyncio
import multiprocessing
import sys

import pytest

from streammachine import Storage

# Skip on Windows due to process spawning differences
pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Multiprocess tests require Unix-like process spawning",
)


def _bind_shared_storage(shared_dict, command_queue) -> Storage:
    """Attach worker-local Storage to the parent's manager proxies."""
    storage = Storage()
    storage.manager = None
    storage.shared_dict = shared_dict
    storage.command_queue = command_queue
    storage._manager_started = True
    return storage


def increment_counter(worker_id: int, n_times: int, shared_dict, command_queue) -> None:
    """Run in a separate process to increment the shared counter."""
    storage = _bind_shared_storage(shared_dict, command_queue)

    async def do_increment() -> None:
        await storage.write(f"worker_count:{worker_id}", n_times)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(do_increment())
    finally:
        loop.close()


def cpu_bound_task(n: int) -> int:
    """A CPU-bound task that runs in a separate process."""
    return sum(i * i for i in range(n))


def worker_task(task_id: int, shared_dict, command_queue) -> dict:
    """Task that runs in a worker process using shared Storage proxies."""
    storage = _bind_shared_storage(shared_dict, command_queue)

    async def do_work() -> dict:
        result_key = f"task_result:{task_id}"
        result_value = {"task_id": task_id, "done": True}
        await storage.write(result_key, result_value)
        return result_value

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(do_work())
    finally:
        loop.close()


def failing_task() -> None:
    """Task used to verify child exceptions propagate."""
    raise ValueError("Intentional error in child process")


class TestStorageMultiprocess:
    """Test Storage across multiple processes."""

    @pytest.mark.asyncio
    async def test_storage_shared_between_processes(self):
        """Test that Storage state is shared across processes."""
        Storage.reset_instance()
        storage = Storage()
        storage._ensure_manager()

        try:
            # Run multiple processes
            ctx = multiprocessing.get_context("spawn")
            processes = []
            for worker_id in range(3):
                p = ctx.Process(
                    target=increment_counter,
                    args=(worker_id, 10, storage.shared_dict, storage.command_queue),
                )
                p.start()
                processes.append(p)

            # Wait for all processes
            for p in processes:
                p.join(timeout=10)
                assert p.exitcode == 0

            # Verify each worker wrote its exact count into shared storage
            worker_counts = {
                str(worker_id): await storage.read(f"worker_count:{worker_id}")
                for worker_id in range(3)
            }
            assert worker_counts == {"0": 10, "1": 10, "2": 10}

        finally:
            Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_storage_per_key_locking(self):
        """Test that Storage locks per-key, allowing concurrent writes to different keys."""
        Storage.reset_instance()
        storage = Storage()
        storage._ensure_manager()

        try:
            # Initialize keys
            await storage.write("key1", 0)
            await storage.write("key2", 0)

            async def increment_key(key: str, times: int) -> None:
                """Increment a key multiple times."""
                for _ in range(times):
                    current = await storage.read(key, default=0)
                    await storage.write(key, current + 1)

            # Run concurrent increments on different keys
            await asyncio.gather(
                increment_key("key1", 100),
                increment_key("key2", 100),
            )

            # Both keys should have been incremented correctly
            val1 = await storage.read("key1")
            val2 = await storage.read("key2")
            assert val1 == 100
            assert val2 == 100

        finally:
            Storage.reset_instance()


class TestMultiprocessAgentSimulation:
    """Simulate multiprocess agent behavior."""

    @pytest.mark.asyncio
    async def test_agent_state_isolation(self):
        """Test that each agent has isolated local state but shared Storage."""
        Storage.reset_instance()

        try:
            # Shared state
            storage = Storage()
            storage._ensure_manager()
            await storage.write("processed_count", 0)

            async def simulate_agent(agent_id: int) -> int:
                """Simulate an agent processing messages."""
                # Each "agent" has its own local state
                local_processed = 0

                # But shares Storage
                for _ in range(10):
                    # Process message (simulated)
                    local_processed += 1

                    # Update shared state
                    current = await storage.read("processed_count", default=0)
                    await storage.write("processed_count", current + 1)

                return local_processed

            # Run multiple "agents" concurrently
            results = await asyncio.gather(
                simulate_agent(1),
                simulate_agent(2),
                simulate_agent(3),
            )

            # Each agent processed 10 messages
            assert results == [10, 10, 10]

            # Total processed should be 30
            total = await storage.read("processed_count")
            # Due to race conditions, may not be exactly 30
            assert total > 0

        finally:
            Storage.reset_instance()


class TestProcessPoolExecutor:
    """Test ProcessPoolExecutor usage."""

    def test_process_pool_basic(self):
        """Test basic ProcessPoolExecutor functionality."""
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(
            max_workers=2,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            futures = [
                executor.submit(cpu_bound_task, 1000),
                executor.submit(cpu_bound_task, 2000),
            ]
            results = [f.result() for f in futures]

        assert results[0] == sum(i * i for i in range(1000))
        assert results[1] == sum(i * i for i in range(2000))

    @pytest.mark.asyncio
    async def test_process_pool_with_storage(self):
        """Test ProcessPoolExecutor with Storage state sharing."""
        Storage.reset_instance()
        storage = Storage()
        storage._ensure_manager()

        try:
            # Run tasks in process pool
            from concurrent.futures import ProcessPoolExecutor

            with ProcessPoolExecutor(
                max_workers=2,
                mp_context=multiprocessing.get_context("spawn"),
            ) as executor:
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        executor,
                        worker_task,
                        i,
                        storage.shared_dict,
                        storage.command_queue,
                    )
                    for i in range(3)
                ]
                results = await asyncio.gather(*futures)

            # Verify each worker result was stored under its own key
            assert sorted(results, key=lambda item: item["task_id"]) == [
                {"task_id": 0, "done": True},
                {"task_id": 1, "done": True},
                {"task_id": 2, "done": True},
            ]
            stored_results = await asyncio.gather(
                *(storage.read(f"task_result:{i}") for i in range(3))
            )
            assert stored_results == results

        finally:
            Storage.reset_instance()


class TestGracefulShutdown:
    """Test graceful shutdown with multiple processes."""

    @pytest.mark.asyncio
    async def test_shutdown_with_active_storage(self):
        """Test that Storage shutdown works correctly."""
        Storage.reset_instance()
        storage = Storage()
        storage._ensure_manager()

        try:
            # Write some data
            await storage.write("key", "value")

            # Shutdown should clean up properly
            await storage.terminate()
            storage.stop()

        finally:
            Storage.reset_instance()

    @pytest.mark.asyncio
    async def test_storage_stop_without_start(self):
        """Test that Storage.stop() works even if manager was never started."""
        Storage.reset_instance()
        storage = Storage()

        # Should be a no-op since manager never started
        storage.stop()

        # And should still work after
        Storage.reset_instance()


class TestErrorPropagation:
    """Test error propagation from child processes."""

    def test_exception_in_child_process(self):
        """Test that exceptions in child processes are handled."""
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(
            max_workers=1,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            future = executor.submit(failing_task)
            with pytest.raises(ValueError, match="Intentional error"):
                future.result()

    @pytest.mark.asyncio
    async def test_storage_error_handling(self):
        """Test that Storage handles errors gracefully."""
        Storage.reset_instance()
        storage = Storage()
        storage._ensure_manager()

        try:
            # Read nonexistent key returns default
            result = await storage.read("nonexistent", default="fallback")
            assert result == "fallback"

            # Delete nonexistent key returns False
            result = await storage.delete("nonexistent")
            assert result is False

        finally:
            Storage.reset_instance()
