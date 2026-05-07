"""
StreamMachine Application Module

This module provides the core App class and StreamConsumer for building
async Redis Streams processing applications.

Key Design Decisions:
- Venusian for decorator discovery: Allows decorators to be scanned at module
  import time without requiring explicit registration. This enables a clean
  declarative API where @app.agent and @app.timer just work.
- uvloop for event loop: Provides 2-4x faster event loop performance vs asyncio
  default, critical for high-throughput stream processing.
- ProcessPoolExecutor for CPU-bound work: When `processes=N` is specified, the
  agent runs in separate processes to bypass Python's GIL for true parallelism.
- Graceful shutdown: Signal handlers (SIGINT/SIGTERM) trigger cleanup sequence
  that waits for in-flight messages before terminating.

Architecture Overview:
    ┌─────────────────────────────────────────────────────────┐
    │                        App                               │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
    │  │  Registry   │  │ Event Loop  │  │ Process/Thread   │  │
    │  │ (decorators)│  │  (uvloop)   │  │    Pools        │  │
    │  └─────────────┘  └─────────────┘  └─────────────────┘  │
    │         │                │                    │         │
    │         ▼                ▼                    ▼         │
    │  ┌─────────────────────────────────────────────────┐   │
    │  │              StreamConsumer                    │   │
    │  │   - XREADGROUP from consumer group            │   │
    │  │   - Message parsing (Message object)          │   │
    │  │   - Handler invocation                        │   │
    │  │   - Auto-ack (configurable)                   │   │
    │  └─────────────────────────────────────────────────┘   │
    │                          │                             │
    │                          ▼                             │
    │              ┌─────────────────────┐                   │
    │              │   Redis Streams      │                   │
    │              │   (via coredis)      │                   │
    │              └─────────────────────┘                   │
    └─────────────────────────────────────────────────────────┘
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
import time
import uvloop
import inspect
import venusian
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable, List, Optional, Set

from .util import Registry, AgentTaskDecorator, TimerTaskDecorator
from .models import (
    AppConfig,
    ConsumerConfig,
    TimerConfig,
    StreamTopic,
    Message,
    DEFAULT_CONSUMER_GROUP,
)
from .redisapi import RedisConnection
from . import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s [%(name)s]: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Set uvloop as the default event loop policy
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class App:
    """
    Python Stream Processing with Redis Streams.

    Main application class that manages the event loop, task discovery,
    and lifecycle of stream consumers and timers.

    Design Pattern:
        This follows a "registry and run" pattern where:
        1. Decorators (@app.agent, @app.timer) register tasks in a registry
        2. Venusian scans modules at import time to discover registered tasks
        3. App.start() creates the event loop and runs all discovered tasks

    Consumer Groups:
        Each agent creates a Redis consumer group. Multiple instances of
        the same app can run in parallel, and Redis will distribute messages
        among consumers in the same group. This enables horizontal scaling.

    Multiprocessing:
        For CPU-bound agents, use processes=N to spawn N worker processes.
        Each process has its own event loop and Redis connection. Use Storage
        for cross-process state sharing via multiprocessing.Manager.

    Thread Safety:
        - Storage uses multiprocessing.Manager for cross-process state
        - Each StreamConsumer has its own RedisConnection
        - asyncio.Lock in Storage prevents concurrent writes to same key

    Dashboard:
        When dashboard_enabled=True (default), the first App instance to start
        becomes the dashboard master and serves a web UI on the configured port.
        Subsequent instances register themselves and skip dashboard startup.
        The dashboard aggregates metrics from all registered instances.

    Example:
        Simple agent and timer::

            app = App(name="my_app")

            @app.timer(1)  # Run every 1 second
            async def producer():
                await app.send("ticks", {"count": 1})

            @app.agent("ticks", group="processors")
            async def process_tick(record: Message):
                print(f"Received: {record.message}")

            if __name__ == "__main__":
                app.start()

        Multiprocess agent for CPU-bound work::

            @app.agent("data", processes=4)  # 4 worker processes
            async def heavy_processing(record: Message):
                result = cpu_intensive_work(record.message)
                await app.send("results", result)

    Args:
        name: Application name for logging and identification
        to_scan: Whether to scan for decorated tasks (default True)
        max_processes: Maximum number of process pool workers
        max_threads: Maximum number of thread pool workers
        dashboard_enabled: Whether to enable the monitoring dashboard (default True)
        dashboard_port: Port for the dashboard server (default 8000)
        dashboard_host: Host for the dashboard server (default "localhost")
        dashboard_refresh_interval: Heartbeat interval in seconds (default 5)
    """

    def __init__(
        self,
        name: str = __name__,
        to_scan: bool = True,
        max_processes: int = 5,
        max_threads: int = 5,
        dashboard_enabled: bool = True,
        dashboard_port: int = 8000,
        dashboard_host: str = "localhost",
        dashboard_refresh_interval: int = 5,
    ):
        """
        Initialize the StreamMachine application.

        Args:
            name: Application name for logging
            to_scan: Whether to scan for decorated tasks
            max_processes: Maximum number of process pool workers
            max_threads: Maximum number of thread pool workers
            dashboard_enabled: Whether to enable the monitoring dashboard
            dashboard_port: Port for the dashboard server
            dashboard_host: Host for the dashboard server
            dashboard_refresh_interval: Heartbeat interval in seconds
        """
        self.config = AppConfig(
            name=name,
            to_scan=to_scan,
            max_processes=max_processes,
            max_threads=max_threads,
            dashboard_enabled=dashboard_enabled,
            dashboard_port=dashboard_port,
            dashboard_host=dashboard_host,
            dashboard_refresh_interval=dashboard_refresh_interval,
        )
        self.process_pool = ProcessPoolExecutor(max_workers=max_processes)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_threads)
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._shutdown_event = asyncio.Event()
        self._running_tasks: Set[asyncio.Task] = set()
        self.registry = Registry()
        self.agent = AgentTaskDecorator
        self.timer = TimerTaskDecorator
        self.rc = RedisConnection()
        self.storage = storage.Storage()
        self._is_shutting_down = False

        # Instance tracking for dashboard
        self._instance_id: str = str(uuid.uuid4())[:8]
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._start_time: float = 0.0
        self._dashboard_started: bool = False

    def _discover(self) -> None:
        """
        Discover decorated agents and timers using Venusian scanner.

        Venusian provides deferred decorator scanning, which means decorators
        don't execute at import time. Instead, Venusian attaches metadata to
        the decorated function, and this method scans the calling module to
        find all decorated functions.

        Why Venusian vs manual registration?
            - Cleaner API: Users just add @app.agent decorators
            - No circular imports: Decorators can reference app before it's built
            - Lazy discovery: Only scans when start() is called

        The scanner looks at the call stack to find the main module, then
        recursively scans all imported modules for Venusian attachments.
        """
        if self.config.to_scan:
            frm = inspect.stack()[len(inspect.stack()) - 1]
            mod = inspect.getmodule(frm[0])
            if mod is not None:
                scanner = venusian.Scanner(registry=self.registry)
                scanner.scan(mod)
                logger.info(
                    f"Discovered {len(self.registry.registered)} tasks: "
                    f"{sum(1 for t in self.registry.registered if t.decorator_type == 'agent')} agents, "
                    f"{sum(1 for t in self.registry.registered if t.decorator_type == 'timer')} timers"
                )

    def _get_concurrent_agents(self) -> List[Callable[[], Any]]:
        """
        Get list of agent coroutines to run within the main process.

        Returns agents that don't use multiprocessing (processes=None).
        These agents run as asyncio tasks in the main process's event loop.

        For each agent, we create N coroutine instances where N is the
        concurrency parameter, allowing parallel message processing within
        the same process.
        """
        return [
            agent_container(item)
            for item in self.registry.registered
            if item.decorator_type == "agent" and item.processes is None
            for _ in range(item.concurrency)
        ]

    def _get_timers(self) -> List[Callable[[], Any]]:
        """Get list of timer coroutines to run."""
        return [
            timer_container(item, self._shutdown_event)
            for item in self.registry.registered
            if item.decorator_type == "timer"
        ]

    def _get_multiprocesses_concurrent_agents(self) -> List[Any]:
        """
        Get list of agents configured for multiprocess execution.

        These agents use processes=N to spawn separate processes for
        CPU-bound work. Each process gets its own event loop and
        Redis connection, bypassing the GIL for true parallelism.

        Note: Communication between processes is via Storage (which uses
        multiprocessing.Manager) or Redis streams.

        Returns:
            List of ConsumerConfig items with processes != None
        """
        return [
            item
            for item in self.registry.registered
            if item.decorator_type == "agent" and item.processes is not None
            for _ in range(item.concurrency)
        ]

    def _setup_signal_handlers(self) -> None:
        """
        Set up signal handlers for graceful shutdown.

        Registers handlers for SIGINT (Ctrl+C) and SIGTERM (kill command).
        On signal reception:
        1. Sets _is_shutting_down flag to prevent double-shutdown
        2. Calls shutdown() to cancel all tasks
        3. Waits for tasks to complete with timeout
        4. Closes Redis connections and stops event loop

        Note: On Windows, add_signal_handler raises NotImplementedError.
        In that case, shutdown must be triggered manually or via Ctrl+C.
        """
        if self.loop is None:
            return

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self.loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._handle_signal(s))
                )
                logger.debug(f"Registered handler for signal {sig.name}")
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                logger.warning(f"Cannot register signal handler for {sig.name} on this platform")

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle shutdown signals gracefully."""
        if self._is_shutting_down:
            logger.info("Already shutting down, ignoring signal")
            return

        self._is_shutting_down = True
        logger.info(f"Received signal {sig.name}, initiating graceful shutdown...")
        await self.shutdown()

    async def _register_instance(self) -> None:
        """
        Register this App instance in Redis for dashboard aggregation.

        Uses Redis directly (not Storage) so independently started processes
        can see each other's instances.
        """
        if not self.config.dashboard_enabled:
            return

        try:
            from .dashboard import register_instance
            await self.rc._ensure_pool()
            await register_instance(
                self.rc.client,
                self._instance_id,
                self.config.name,
                os.getpid(),
                socket.gethostname(),
                self._start_time
            )
        except Exception as e:
            logger.warning(f"Failed to register instance for dashboard: {e}")

    async def _unregister_instance(self) -> None:
        """Unregister this App instance from Redis."""
        if not self.config.dashboard_enabled:
            return

        try:
            from .dashboard import unregister_instance
            await unregister_instance(self.rc.client, self._instance_id)
        except Exception as e:
            logger.warning(f"Failed to unregister instance from dashboard: {e}")

    async def _heartbeat_loop(self) -> None:
        """
        Periodically update heartbeat in Redis.

        Updates instance metrics every dashboard_refresh_interval seconds
        to indicate the instance is still alive and processing.
        Only runs when dashboard is enabled.
        """
        if not self.config.dashboard_enabled:
            return

        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.dashboard_refresh_interval
                )
                # If we get here, shutdown was requested
                break
            except asyncio.TimeoutError:
                # Timeout means it's time to update heartbeat
                pass

            try:
                metrics = await self._get_metrics()
                from .dashboard import update_heartbeat
                await self.rc._ensure_pool()
                await update_heartbeat(self.rc.client, self._instance_id, metrics)
                logger.debug(f"Updated heartbeat for instance {self._instance_id}")
            except Exception as e:
                logger.error(f"Error updating heartbeat: {e}", exc_info=True)

    async def _get_metrics(self) -> dict:
        """
        Return current metrics for this instance.

        Gathers information about agents, timers, and active tasks.
        """
        agents = [
            item for item in self.registry.registered
            if item.decorator_type == "agent"
        ]
        timers = [
            item for item in self.registry.registered
            if item.decorator_type == "timer"
        ]

        # Build detailed info for dashboard
        agents_detail = []
        for agent in agents:
            agent_info = {
                "topic": agent.topic if isinstance(agent.topic, str) else list(agent.topic),
                "group": agent.group,
                "concurrency": agent.concurrency,
                "processes": agent.processes,
            }
            agents_detail.append(agent_info)

        timers_detail = []
        for timer in timers:
            timer_info = {
                "name": timer.obj_name,
                "interval": timer.t,
            }
            timers_detail.append(timer_info)

        # Get stream info from agents
        streams = set()
        for agent in agents:
            if isinstance(agent.topic, str):
                streams.add(agent.topic)
            elif isinstance(agent.topic, list):
                streams.update(agent.topic)

        return {
            "instance_id": self._instance_id,
            "agents": len(agents),
            "timers": len(timers),
            "active_tasks": len(self._running_tasks),
            "last_heartbeat": time.time(),
            "agents_detail": agents_detail,
            "timers_detail": timers_detail,
            "streams": list(streams),
        }

    async def _start_dashboard(self) -> None:
        """
        Start the dashboard if enabled and not already running.

        Uses DashboardManager singleton for cross-process coordination.
        """
        if not self.config.dashboard_enabled:
            return

        try:
            from .dashboard import start_dashboard
            self._dashboard_started = await start_dashboard(
                app_instance_id=self._instance_id,
                port=self.config.dashboard_port,
                host=self.config.dashboard_host,
            )
            if self._dashboard_started:
                logger.info(
                    f"Dashboard started on http://{self.config.dashboard_host}:{self.config.dashboard_port}"
                )
        except ImportError:
            logger.warning(
                "Dashboard enabled but FastAPI not installed. "
                "Install with: pip install fastapi uvicorn"
            )
        except Exception as e:
            logger.error(f"Failed to start dashboard: {e}", exc_info=True)

    async def _stop_dashboard(self) -> None:
        """Stop the dashboard if this instance is master."""
        if not self._dashboard_started:
            return

        try:
            from .dashboard import stop_dashboard
            await stop_dashboard()
            logger.info("Dashboard stopped")
        except Exception as e:
            logger.warning(f"Error stopping dashboard: {e}")

    def start(self) -> None:
        """
        Start the application event loop.

        This is the main entry point that discovers tasks, sets up signal handlers,
        and runs the event loop until shutdown is requested.

        If dashboard_enabled is True (default), the first App instance to start
        will host the dashboard. Subsequent instances will register themselves
        and skip dashboard startup.
        """
        self._start_time = time.time()
        self._discover()
        agents = self._get_concurrent_agents()
        timers = self._get_timers()

        asyncio.set_event_loop(uvloop.new_event_loop())
        self.loop = asyncio.get_event_loop()

        # Set up signal handlers
        self._setup_signal_handlers()

        # Create and schedule all tasks
        all_coros = [maintenance_task(self._shutdown_event)]
        all_coros.extend(timers)
        all_coros.extend(agents)
        all_coros.append(self.storage.start())

        # Register instance and start heartbeat for dashboard (only if enabled)
        if self.config.dashboard_enabled:
            all_coros.append(self._register_instance())
            self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
            self._running_tasks.add(self._heartbeat_task)
            self._heartbeat_task.add_done_callback(self._running_tasks.discard)

            # Start dashboard if this instance becomes master
            all_coros.append(self._start_dashboard())

        for coro in all_coros:
            task = asyncio.ensure_future(coro)
            self._running_tasks.add(task)
            task.add_done_callback(self._running_tasks.discard)

        logger.info(
            f"Starting {len(agents)} agents and {len(timers)} timers "
            f"(total tasks: {len(self._running_tasks)})"
        )

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        """Clean up resources after event loop stops."""
        logger.info("Cleaning up resources...")

        # Shutdown storage manager (multiprocessing.Manager creates a separate process)
        try:
            self.storage.stop()
        except Exception as e:
            logger.warning(f"Error stopping storage: {e}")

        # Shutdown executors
        try:
            self.process_pool.shutdown(wait=False)
            self.thread_pool.shutdown(wait=False)
        except Exception as e:
            logger.warning(f"Error shutting down executors: {e}")

        # Close event loop
        if self.loop and not self.loop.is_closed():
            self.loop.close()

        logger.info("Cleanup complete")

    async def send(self, topic: str, record: dict) -> Any:
        """
        Send a single record to a Redis stream.

        Args:
            topic: Stream name
            record: Record data as a dictionary

        Returns:
            Message ID from Redis
        """
        t = time.time()
        record["sent"] = t
        return await self.rc.client.xadd(topic, record)

    async def send_batch(self, topic: str, records: List[dict]) -> List:
        """
        Batch send multiple records to a Redis stream.

        Args:
            topic: Stream name
            records: List of record dictionaries

        Returns:
            List of message IDs from Redis
        """
        t = time.time()
        for record in records:
            record["sent"] = t
        return await self.rc.pipeline_xadd(topic, records)

    async def shutdown(self) -> None:
        """
        Gracefully shutdown all running tasks.

        This method implements a graceful shutdown sequence:
        1. Set shutdown event to signal timers to stop
        2. Stop dashboard if this instance is master
        3. Unregister instance from dashboard
        4. Cancel all pending asyncio tasks
        5. Wait up to 10 seconds for tasks to complete
        6. Close Redis connection pool
        7. Terminate storage manager process
        8. Stop the event loop

        The 10-second timeout ensures the app doesn't hang indefinitely
        if a task is stuck. Tasks that don't complete in time are logged
        as warnings.

        Important: This method should only be called once. The
        _is_shutting_down flag prevents double-shutdown scenarios.
        """
        self._shutdown_event.set()
        logger.info("Shutting down...")

        # Stop dashboard if this instance is master
        await self._stop_dashboard()

        # Unregister instance from dashboard
        try:
            await self._unregister_instance()
        except Exception as e:
            logger.warning(f"Error unregistering instance: {e}")

        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Get all tasks except current
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

        if tasks:
            logger.info(f"Cancelling {len(tasks)} outstanding tasks...")
            for task in tasks:
                task.cancel()

            # Wait for all tasks to complete (with timeout)
            done, pending = await asyncio.wait(
                tasks,
                timeout=10.0,
                return_when=asyncio.ALL_COMPLETED
            )

            if pending:
                logger.warning(f"{len(pending)} tasks did not complete in time")

        # Close Redis connection
        try:
            await self.rc.close()
            logger.debug("Redis connection closed")
        except Exception as e:
            logger.warning(f"Error closing Redis connection: {e}")

        # Terminate storage command handler
        try:
            await self.storage.terminate()
            logger.debug("Storage termination signal sent")
        except Exception as e:
            logger.warning(f"Error terminating storage: {e}")

        # Stop the event loop
        if self.loop:
            self.loop.stop()

        logger.info("Shutdown complete")

    async def health_check(self) -> dict:
        """
        Perform a health check of the application.

        Returns:
            Dictionary with health status information
        """
        redis_healthy = await self.rc.health_check()

        return {
            "status": "healthy" if redis_healthy else "degraded",
            "redis": "connected" if redis_healthy else "disconnected",
            "active_tasks": len(self._running_tasks),
            "registered_agents": sum(
                1 for t in self.registry.registered if t.decorator_type == "agent"
            ),
            "registered_timers": sum(
                1 for t in self.registry.registered if t.decorator_type == "timer"
            ),
        }


async def agent_container(item: ConsumerConfig) -> Any:
    """
    Create and run an agent consumer.

    Args:
        item: Consumer configuration

    Returns:
        Result of the agent (typically runs forever)
    """
    agent = StreamConsumer(item)
    return await agent()


async def timer_container(item: TimerConfig, shutdown_event: asyncio.Event) -> None:
    """
    Run a timer task periodically until shutdown is requested.

    Args:
        item: Timer configuration
        shutdown_event: Event to signal shutdown
    """
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=item.t)
            # If we get here, shutdown was requested
            break
        except asyncio.TimeoutError:
            # Timeout means it's time to run the timer
            pass

        try:
            await getattr(item.mod, item.obj_name)()
        except Exception as e:
            logger.error(f"Timer {item.obj_name} failed: {e}", exc_info=True)


class StreamConsumer:
    """
    Redis Stream consumer that processes messages from one or more streams.

    Each consumer creates its own Redis connection and consumer group membership,
    processing messages and passing them to the configured handler function.

    Consumer Group Behavior:
        Redis Streams consumer groups enable horizontal scaling. Multiple consumers
        in the same group share the message load - each message is delivered to
        exactly one consumer in the group.

        Key concepts:
        - Group: Named group of consumers that share a stream
        - Consumer: Individual consumer instance (identified by UUID)
        - Pending Entries List (PEL): Messages claimed but not acknowledged
        - Backlog: Messages in stream before consumer joined

    Message Flow:
        1. Consumer calls XREADGROUP to fetch new messages (blocks if empty)
        2. Message enters PEL (Pending Entries List)
        3. Handler processes the message
        4. If auto_acknowledge=True, XACK is called automatically
        5. If processing fails, message remains in PEL for retry

    Error Handling:
        The consumer loop catches exceptions and continues processing.
        Errors are logged with exc_info=True for debugging. The consumer
        only exits on CancelledError (shutdown) or catastrophic failures.

    Args:
        config: ConsumerConfig with topic, group, handler function, etc.
    """

    def __init__(self, config: ConsumerConfig):
        """
        Initialize the stream consumer.

        Args:
            config: Consumer configuration including topic, group, and handler
        """
        self.config = config
        if isinstance(self.config.topic, str):
            self.config.topic = [self.config.topic]
        self._rc: Optional[RedisConnection] = None

    async def __call__(self) -> None:
        """
        Run the consumer loop, processing messages indefinitely.

        This is the main consumer loop that:
        1. Creates a unique consumer ID (UUID)
        2. Creates/Joins the consumer group on the stream(s)
        3. Enters an infinite loop reading messages via XREADGROUP
        4. Processes each message through the handler function
        5. Handles graceful shutdown on CancelledError

        The GroupConsumer from coredis provides an async iterator that
        yields (stream_name, entry) tuples. When the block timeout expires
        without new messages, the iterator exhausts - we loop back and
        try again.

        Important: Each consumer gets its own RedisConnection instance.
        This is necessary because coredis connections are not thread-safe
        and each async task needs its own connection for concurrent ops.
        """
        consumer_id = str(uuid.uuid4())
        self._rc = RedisConnection()
        group = self.config.group or DEFAULT_CONSUMER_GROUP

        logger.info(f"Starting consumer {consumer_id} for {self.config.topic} in group {group}")

        try:
            cons = await self._rc.consumer(
                self.config.topic,
                consumer_id,
                group
            )

            # The GroupConsumer async iterator raises StopAsyncIteration
            # when xreadgroup returns no messages after the block timeout.
            # We wrap in a while-True to keep polling for new messages.
            while True:
                async for stream, entry in cons:
                    try:
                        await self._process_message(stream, entry, consumer_id)
                    except Exception as e:
                        logger.error(
                            f"Error processing message from {stream}: {e}",
                            exc_info=True
                        )
                        # Continue processing next messages
                    await asyncio.sleep(0)  # Yield to event loop
                # Iterator exhausted (timeout with no messages) — retry
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            logger.info(f"Consumer {consumer_id} cancelled")
        except Exception as e:
            logger.error(f"Consumer {consumer_id} error: {e}", exc_info=True)
        finally:
            if self._rc:
                await self._rc.close()

    async def _process_message(self, stream: bytes, entry: Any, consumer_id: str) -> None:
        """
        Process a single message from the stream.

        This method wraps the user's handler function with:
        1. Message parsing (bytes to Message object)
        2. Timestamp extraction for latency tracking
        3. Handler invocation with proper error handling

        The Message object provides:
        - topic: Stream name the message came from
        - key: Stream entry ID (e.g., "1638360000000-0")
        - sent: Timestamp when message was produced (if present)
        - received: Timestamp when message was consumed
        - data: Raw field-values dict
        - message: Decoded field-values as strings (property)

        Args:
            stream: Stream name as bytes (e.g., b"my_stream")
            entry: Redis stream entry with identifier and field_values
            consumer_id: Unique identifier for this consumer instance
        """
        m = Message(
            topic=stream.decode("UTF-8"),
            key=entry.identifier.decode("UTF-8"),
            received=time.time(),
            consumer_id=consumer_id,
            data=entry.field_values
        )

        # Extract sent timestamp if present
        if b'sent' in entry.field_values:
            m.sent = float(entry.field_values[b'sent'])
            # Don't delete from field_values to avoid mutation issues
            # del entry.field_values[b'sent']

        # Call the handler
        handler = getattr(self.config.mod, self.config.obj_name)
        await handler(m)


async def maintenance_task(shutdown_event: asyncio.Event) -> None:
    """
    Run periodic maintenance tasks.

    Args:
        shutdown_event: Event to signal shutdown
    """
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            # Shutdown was requested
            break
        except asyncio.TimeoutError:
            # Time for maintenance
            pass

        # Add maintenance logic here
        # e.g., cleanup old messages, update metrics, health checks
        logger.debug("Running maintenance task")


# Public API
__all__ = [
    "App",
    "StreamConsumer",
    "Message",
    "AppConfig",
    "ConsumerConfig",
    "TimerConfig",
    "StreamTopic",
    "agent_container",
    "timer_container",
    "maintenance_task",
]


if __name__ == "__main__":
    # This module is not meant to be run directly.
    # Use example.py or create your own entry point.
    print("Run 'python example.py' to start the application.")