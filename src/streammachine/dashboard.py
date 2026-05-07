"""
StreamMachine Dashboard Module

FastAPI dashboard for monitoring StreamMachine tasks across multiple
running App instances. Uses Redis-based distributed locking for singleton
dashboard pattern.

Why Redis for state storage?
    - Storage uses multiprocessing.Manager which is per-process
    - Independently started App processes cannot share Storage
    - Redis is already required and provides true cross-process visibility

Why ownership-safe locks?
    - Simple SETNX + EXPIRE allows stale masters after TTL expiry
    - Compare-and-set with Lua ensures only the lock owner can release/renew
    - Prevents split-brain when a dead master's TTL expires

Architecture:
    - DashboardManager: Singleton pattern with Redis distributed lock
    - Each App instance registers itself in Redis with heartbeat
    - Dashboard aggregates metrics from all registered instances via Redis
    - First started file becomes dashboard master
    - Subsequent files detect existing dashboard and skip startup

Lock Safety:
    - Lock value includes unique token (instance_id:pid:timestamp)
    - Renewal compares token before extending TTL
    - Release compares token before deleting
    - Uses Lua scripts for atomic compare-and-set operations
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# FastAPI/uvicorn imports (optional dependency)
try:
    import uvicorn
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    _HAS_FASTAPI = True
except ImportError:
    FastAPI = None
    HTMLResponse = None
    uvicorn = None
    _HAS_FASTAPI = False

# coredis imports for Redis operations
try:
    import coredis
    from coredis import PureToken
    _HAS_COREDIS = True
except ImportError:
    coredis = None
    PureToken = None
    _HAS_COREDIS = False


# Redis key prefixes for dashboard
INSTANCES_KEY_PREFIX = "streammachine:instances:"
METRICS_KEY_PREFIX = "streammachine:metrics:"
MASTER_KEY = "streammachine:dashboard:master"
LOCK_KEY = "streammachine:dashboard:lock"

# Lock TTL in seconds
LOCK_TTL = 30
HEARTBEAT_INTERVAL = 10
INSTANCE_TTL = 60  # Instance data expires after 60s without heartbeat

# Lua scripts for safe lock operations
# These ensure atomicity - only the lock owner can renew or release
RENEW_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("EXPIRE", KEYS[1], ARGV[2])
    return 1
else
    return 0
end
"""

RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("DEL", KEYS[1])
    return 1
else
    return 0
end
"""


@dataclass
class InstanceInfo:
    """Information about a registered App instance."""
    instance_id: str
    name: str
    pid: int
    host: str
    start_time: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class InstanceMetrics:
    """Metrics for a registered App instance."""
    instance_id: str
    agents: int
    timers: int
    active_tasks: int
    last_heartbeat: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class DashboardManager:
    """
    Singleton manager for dashboard lifecycle using Redis distributed lock.

    Uses Redis SETNX (SET if Not eXists) for atomic lock acquisition, ensuring
    only one dashboard instance runs across multiple processes/machines.

    Lock Safety:
        - Lock value includes unique token that identifies the owner
        - Renewal uses Lua script to compare token before extending TTL
        - Release uses Lua script to compare token before deleting
        - This prevents stale masters from interfering with new masters

    Thread Safety:
        - Uses threading.Lock for in-process thread safety
        - Uses Redis SETNX for cross-process/machine coordination

    Attributes:
        _instance: Singleton instance
        _lock: Thread lock for singleton pattern
        _lock_token: Unique token for this master (used for safe release)
        _server: uvicorn.Server instance (kept for graceful shutdown)
        _heartbeat_task: Task for renewing lock and heartbeat
    """

    _instance: Optional[DashboardManager] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls) -> DashboardManager:
        """Create or return singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self):
        """Initialize the dashboard manager."""
        if self._initialized:
            return

        self._is_master = False
        self._master_info: Optional[Dict[str, Any]] = None
        self._server: Optional[uvicorn.Server] = None
        self._server_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        self._app_instance_id: Optional[str] = None
        self._lock_token: Optional[str] = None  # Unique token for ownership
        self._redis = None
        self._port: int = 8000
        self._host: str = "localhost"
        self._renew_script_sha: Optional[str] = None
        self._release_script_sha: Optional[str] = None
        self._initialized = True
        logger.debug("DashboardManager initialized")

    async def _get_redis(self):
        """Get Redis connection lazily."""
        if self._redis is None:
            from .redisapi import RedisConnection
            self._redis = RedisConnection()
            await self._redis._ensure_pool()
        return self._redis

    async def _load_scripts(self, client) -> None:
        """Load Lua scripts into Redis for fast execution."""
        if self._renew_script_sha is not None:
            return

        # Load renew script
        self._renew_script_sha = await client.script_load(RENEW_LOCK_SCRIPT)
        self._release_script_sha = await client.script_load(RELEASE_LOCK_SCRIPT)
        logger.debug("Loaded lock safety scripts into Redis")

    @staticmethod
    def _lock_value_matches(current: Any, token: str) -> bool:
        """Return True when the stored Redis lock value matches our token."""
        if current is None:
            return False
        if isinstance(current, bytes):
            current = current.decode()
        return current == token

    async def _shutdown_server_task(self, timeout: float = 5.0) -> None:
        """Signal the dashboard server to exit and await completion."""
        if self._server is not None:
            self._server.should_exit = True

        if self._server_task is not None and not self._server_task.done():
            try:
                await asyncio.wait_for(self._server_task, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Server shutdown timed out, cancelling")
                self._server_task.cancel()

    async def try_become_master(self, port: int, host: str, app_instance_id: str) -> bool:
        """
        Atomically acquire dashboard master lock via Redis SETNX.

        Args:
            port: Port for the dashboard server
            host: Host for the dashboard server
            app_instance_id: ID of the App instance trying to become master

        Returns:
            True if this instance became master, False if another instance
            already holds the lock
        """
        try:
            redis = await self._get_redis()
            client = redis.client

            # Generate unique lock token for ownership verification
            self._lock_token = f"{app_instance_id}:{os.getpid()}:{time.time()}"

            # Try to acquire lock with TTL
            if _HAS_COREDIS:
                acquired = await client.set(
                    LOCK_KEY,
                    self._lock_token,
                    condition=PureToken.NX,
                    ex=LOCK_TTL
                )
            else:
                acquired = await client.set(
                    LOCK_KEY,
                    self._lock_token,
                    nx=True,
                    ex=LOCK_TTL
                )

            if acquired:
                try:
                    # Load Lua scripts for safe lock operations
                    await self._load_scripts(client)
                except Exception:
                    current = await client.get(LOCK_KEY)
                    if self._lock_value_matches(current, self._lock_token):
                        await client.delete(LOCK_KEY)
                    self._lock_token = None
                    raise

                self._is_master = True
                self._app_instance_id = app_instance_id
                self._port = port
                self._host = host
                self._master_info = {
                    "instance_id": app_instance_id,
                    "port": port,
                    "host": host,
                    "started_at": time.time(),
                    "pid": os.getpid(),
                    "lock_token": self._lock_token
                }

                # Store master info in Redis for other instances to query
                await client.set(MASTER_KEY, json.dumps(self._master_info))

                logger.info(f"DashboardManager became master on {host}:{port}")
                return True
            else:
                self._lock_token = None
                logger.info("Dashboard already running, skipping startup")
                return False

        except Exception as e:
            logger.error(f"Failed to acquire dashboard lock: {e}")
            self._lock_token = None
            return False

    async def release_lock(self) -> None:
        """Release the master lock safely using compare-and-delete."""
        if not self._is_master or not self._lock_token:
            return

        try:
            redis = await self._get_redis()
            client = redis.client

            # Use Lua script for safe release - only delete if we own it
            released = False
            if self._release_script_sha and _HAS_COREDIS:
                result = await client.evalsha(
                    self._release_script_sha,
                    keys=[LOCK_KEY],
                    args=[self._lock_token]
                )
                if result:
                    released = True
                    logger.info("Released dashboard lock (owned)")
                else:
                    logger.warning("Could not release lock - not owner or already released")
            else:
                # Fallback: check then delete (not atomic but better than nothing)
                current = await client.get(LOCK_KEY)
                if self._lock_value_matches(current, self._lock_token):
                    await client.delete(LOCK_KEY)
                    released = True
                    logger.info("Released dashboard lock")
                else:
                    logger.warning("Lock ownership lost or changed")

            # Only delete MASTER_KEY if we still own the lock (prevents erasing new master's metadata)
            if released:
                await client.delete(MASTER_KEY)
            else:
                logger.warning("Not deleting MASTER_KEY - lock was not owned by this instance")

        except Exception as e:
            logger.warning(f"Error releasing dashboard lock: {e}")
        finally:
            self._is_master = False
            self._master_info = None
            self._lock_token = None

    async def _stop_server_if_running(self) -> None:
        """Stop the uvicorn server if running. Called when lock is lost."""
        if self._server is not None:
            logger.info("Stopping dashboard server due to lock loss")
            self._server.should_exit = True

            if self._server_task and not self._server_task.done():
                try:
                    await asyncio.wait_for(self._server_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("Server shutdown timed out during lock loss")
                    self._server_task.cancel()

    async def _heartbeat_loop(self) -> None:
        """Renew lock TTL and update heartbeat periodically."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)

                if self._is_master and self._lock_token:
                    redis = await self._get_redis()
                    client = redis.client

                    # Use Lua script for safe renewal - only extend if we own it
                    if self._renew_script_sha and _HAS_COREDIS:
                        result = await client.evalsha(
                            self._renew_script_sha,
                            keys=[LOCK_KEY],
                            args=[self._lock_token, LOCK_TTL]
                        )
                        if not result:
                            logger.warning("Lock ownership lost during renewal")
                            try:
                                await self._shutdown_server_task()
                            finally:
                                self._is_master = False
                                self._lock_token = None
                                self._master_info = None
                            break
                    else:
                        # Fallback: check then expire (not atomic)
                        current = await client.get(LOCK_KEY)
                        if self._lock_value_matches(current, self._lock_token):
                            await client.expire(LOCK_KEY, LOCK_TTL)
                        else:
                            logger.warning("Lock ownership lost")
                            try:
                                await self._shutdown_server_task()
                            finally:
                                self._is_master = False
                                self._lock_token = None
                                self._master_info = None
                            break

                    logger.debug("Dashboard heartbeat renewed")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dashboard heartbeat: {e}")

    def is_master(self) -> bool:
        """Check if this instance is the dashboard master."""
        return self._is_master

    def get_master_info(self) -> Optional[Dict[str, Any]]:
        """Get info about the current master."""
        return self._master_info

    async def start_server(self) -> None:
        """Start FastAPI server in background."""
        if not self._is_master:
            return

        if not _HAS_FASTAPI:
            logger.warning("FastAPI not installed, dashboard disabled")
            return

        # Create FastAPI app
        app = create_app()

        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Start uvicorn server - keep reference for proper shutdown
        config = uvicorn.Config(
            app,
            host=self._host,
            port=self._port,
            log_level="warning",
            access_log=False
        )
        self._server = uvicorn.Server(config)

        self._server_task = asyncio.create_task(self._server.serve())
        logger.info(f"Dashboard started on http://{self._host}:{self._port}")

    async def stop_server(self) -> None:
        """Graceful shutdown and cleanup."""
        self._shutdown_event.set()

        # Signal uvicorn to shutdown gracefully
        if self._server is not None:
            logger.debug("Signaled uvicorn server to shutdown")

        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Wait for server task to finish (with timeout)
        await self._shutdown_server_task()

        # Release lock after server is stopped
        await self.release_lock()

        logger.info("Dashboard stopped")

    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton instance (for testing)."""
        with cls._lock:
            cls._instance = None


def create_app() -> FastAPI:
    """Create FastAPI application with dashboard routes."""
    from contextlib import asynccontextmanager
    from .redisapi import RedisConnection

    # Shared Redis connection for the app lifespan
    _shared_redis: Optional[RedisConnection] = None

    @asynccontextmanager
    async def lifespan(app):
        """Manage Redis connection lifespan."""
        nonlocal _shared_redis
        _shared_redis = RedisConnection()
        await _shared_redis._ensure_pool()
        try:
            yield
        finally:
            if _shared_redis:
                await _shared_redis.close()
                _shared_redis = None

    app = FastAPI(
        title="StreamMachine Dashboard",
        description="Monitor StreamMachine tasks across multiple instances",
        version="0.1.0",
        lifespan=lifespan
    )

    def get_shared_client():
        """Get the shared Redis client from lifespan state."""
        if _shared_redis is None:
            raise RuntimeError("Redis connection not initialized - app not started")
        return _shared_redis.client

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_ui():
        """HTML dashboard UI."""
        return get_dashboard_html()

    @app.get("/api/health")
    async def health():
        """Aggregated health from all instances."""
        return await get_aggregated_health(get_shared_client())

    @app.get("/api/instances")
    async def instances():
        """List all registered App instances."""
        return await get_all_instances(get_shared_client())

    @app.get("/api/instance/{instance_id}")
    async def instance_detail(instance_id: str):
        """Details for specific instance."""
        return await get_instance_detail(get_shared_client(), instance_id)

    @app.get("/api/agents")
    async def agents():
        """All registered agents across instances."""
        return await get_all_agents(get_shared_client())

    @app.get("/api/timers")
    async def timers():
        """All registered timers across instances."""
        return await get_all_timers(get_shared_client())

    @app.get("/api/storage")
    async def storage():
        """Storage contents viewer."""
        return await get_storage_contents(get_shared_client())

    @app.get("/api/streams")
    async def streams():
        """Redis stream information."""
        return await get_stream_info(get_shared_client())

    return app


# API endpoint implementations - All use Redis directly for cross-process visibility

async def _get_all_instances_from_redis(client) -> List[Dict[str, Any]]:
    """Get all registered instances from Redis using SCAN."""
    instances = []

    # Use SCAN to find all instance keys
    cursor = 0
    while True:
        if _HAS_COREDIS:
            result = await client.scan(cursor, match=f"{INSTANCES_KEY_PREFIX}*", count=100)
        else:
            result = await client.scan(cursor, match=f"{INSTANCES_KEY_PREFIX}*", count=100)

        cursor = result[0] if isinstance(result, tuple) else result.cursor
        keys = result[1] if isinstance(result, tuple) else result.keys

        for key in keys:
            try:
                data = await client.get(key)
                if data:
                    # Decode if bytes
                    if isinstance(data, bytes):
                        data = data.decode('utf-8')
                    # Parse JSON
                    instances.append(json.loads(data))
            except Exception as e:
                logger.warning(f"Error reading instance {key}: {e}")

        # Check if scan is complete
        if _HAS_COREDIS:
            if cursor == 0:
                break
        else:
            if not cursor or cursor == b'0':
                break

    return instances


async def _get_metrics_from_redis(client, instance_id: str) -> Optional[Dict[str, Any]]:
    """Get metrics for an instance from Redis."""
    metrics_key = f"{METRICS_KEY_PREFIX}{instance_id}"
    data = await client.get(metrics_key)
    if data:
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        return json.loads(data)
    return None


async def get_aggregated_health(client) -> Dict[str, Any]:
    """Get aggregated health from all registered instances."""
    instances = await _get_all_instances_from_redis(client)

    total_agents = 0
    total_timers = 0
    total_tasks = 0
    healthy_count = 0
    now = time.time()

    instance_health = []

    for inst in instances:
        metrics = await _get_metrics_from_redis(client, inst.get("instance_id", ""))

        # Check if heartbeat is recent (within 30 seconds)
        is_healthy = (
            metrics and
            (now - metrics.get("last_heartbeat", 0)) < 30
        )

        if is_healthy:
            healthy_count += 1

        total_agents += metrics.get("agents", 0) if metrics else 0
        total_timers += metrics.get("timers", 0) if metrics else 0
        total_tasks += metrics.get("active_tasks", 0) if metrics else 0

        instance_health.append({
            "instance_id": inst.get("instance_id", ""),
            "name": inst.get("name", "unknown"),
            "healthy": is_healthy,
            "metrics": metrics
        })

    return {
        "status": "healthy" if healthy_count == len(instances) else "degraded",
        "total_instances": len(instances),
        "healthy_instances": healthy_count,
        "total_agents": total_agents,
        "total_timers": total_timers,
        "total_active_tasks": total_tasks,
        "instances": instance_health
    }


async def get_all_instances(client) -> List[Dict[str, Any]]:
    """List all registered App instances."""
    instances = await _get_all_instances_from_redis(client)

    result = []
    now = time.time()

    for inst in instances:
        metrics = await _get_metrics_from_redis(client, inst.get("instance_id", ""))
        is_active = (
            metrics and
            (now - metrics.get("last_heartbeat", 0)) < 30
        )

        result.append({
            **inst,
            "metrics": metrics,
            "active": is_active
        })

    return result


async def get_instance_detail(client, instance_id: str) -> Dict[str, Any]:
    """Get details for a specific instance."""
    instance_key = f"{INSTANCES_KEY_PREFIX}{instance_id}"

    data = await client.get(instance_key)
    if not data:
        return {"error": "Instance not found", "instance_id": instance_id}

    if isinstance(data, bytes):
        data = data.decode('utf-8')
    instance_data = json.loads(data)

    metrics = await _get_metrics_from_redis(client, instance_id)

    return {
        "instance": instance_data,
        "metrics": metrics
    }


async def get_all_agents(client) -> List[Dict[str, Any]]:
    """Get all registered agents across instances."""
    instances = await _get_all_instances_from_redis(client)

    agents = []
    for inst in instances:
        metrics = await _get_metrics_from_redis(client, inst.get("instance_id", ""))
        if metrics and "agents_detail" in metrics:
            for agent in metrics.get("agents_detail", []):
                agents.append({
                    **agent,
                    "instance_id": inst.get("instance_id", ""),
                    "instance_name": inst.get("name", "unknown")
                })

    return agents


async def get_all_timers(client) -> List[Dict[str, Any]]:
    """Get all registered timers across instances."""
    instances = await _get_all_instances_from_redis(client)

    timers = []
    for inst in instances:
        metrics = await _get_metrics_from_redis(client, inst.get("instance_id", ""))
        if metrics and "timers_detail" in metrics:
            for timer in metrics.get("timers_detail", []):
                timers.append({
                    **timer,
                    "instance_id": inst.get("instance_id", ""),
                    "instance_name": inst.get("name", "unknown")
                })

    return timers


async def get_storage_contents(client) -> Dict[str, Any]:
    """Get storage contents (keys only for safety)."""
    # Use SCAN to find all streammachine keys
    keys = []
    cursor = 0
    while True:
        if _HAS_COREDIS:
            result = await client.scan(cursor, match="streammachine:*", count=100)
        else:
            result = await client.scan(cursor, match="streammachine:*", count=100)

        cursor = result[0] if isinstance(result, tuple) else result.cursor
        scan_keys = result[1] if isinstance(result, tuple) else result.keys
        keys.extend(scan_keys)

        if _HAS_COREDIS:
            if cursor == 0:
                break
        else:
            if not cursor or cursor == b'0':
                break

    # Decode keys and fetch values
    result = {}
    for key in keys[:100]:  # Limit to 100 keys
        try:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            data = await client.get(key)
            if data:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                result[key_str] = data
        except Exception as e:
            result[key_str if 'key_str' in dir() else key] = f"Error reading: {e}"

    return {
        "total_keys": len(keys),
        "contents": result
    }


async def get_stream_info(client) -> List[Dict[str, Any]]:
    """Get Redis stream information."""
    # Get stream info from registered instances
    instances = await _get_all_instances_from_redis(client)
    streams = []

    for inst in instances:
        metrics = await _get_metrics_from_redis(client, inst.get("instance_id", ""))
        if metrics and "streams" in metrics:
            for stream in metrics.get("streams", []):
                streams.append({
                    "name": stream,
                    "instance_id": inst.get("instance_id", ""),
                    "instance_name": inst.get("name", "unknown")
                })

    return streams


def get_dashboard_html() -> str:
    """Generate HTML dashboard UI."""
    return """
<!DOCTYPE html>
<html>
<head>
    <title>StreamMachine Dashboard</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }
        h1 { color: #333; margin-bottom: 20px; }
        h2 { color: #555; margin-top: 30px; }
        .container { max-width: 1200px; margin: 0 auto; }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .stat-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
        }
        .stat {
            text-align: center;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }
        .stat-label {
            font-size: 0.85em;
            color: #666;
            margin-top: 5px;
        }
        .status-healthy { color: #28a745; }
        .status-degraded { color: #ffc107; }
        .status-error { color: #dc3545; }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid #eee;
        }
        th { background: #f8f9fa; font-weight: 600; }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 500;
        }
        .badge-active { background: #d4edda; color: #155724; }
        .badge-inactive { background: #f8d7da; color: #721c24; }
        .refresh-info {
            color: #666;
            font-size: 0.85em;
            margin-bottom: 15px;
        }
        #last-refresh { font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>StreamMachine Dashboard</h1>
        <div class="refresh-info">Last refresh: <span id="last-refresh">-</span></div>

        <div class="card">
            <h2 style="margin-top:0">Overview</h2>
            <div class="stat-grid">
                <div class="stat">
                    <div class="stat-value" id="total-instances">-</div>
                    <div class="stat-label">Instances</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="total-agents">-</div>
                    <div class="stat-label">Agents</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="total-timers">-</div>
                    <div class="stat-label">Timers</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="total-tasks">-</div>
                    <div class="stat-label">Active Tasks</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2 style="margin-top:0">Instances</h2>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Instance ID</th>
                        <th>PID</th>
                        <th>Status</th>
                        <th>Agents</th>
                        <th>Timers</th>
                        <th>Last Heartbeat</th>
                    </tr>
                </thead>
                <tbody id="instances-table">
                    <tr><td colspan="7">Loading...</td></tr>
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2 style="margin-top:0">Agents</h2>
            <table>
                <thead>
                    <tr>
                        <th>Topic</th>
                        <th>Group</th>
                        <th>Instance</th>
                        <th>Concurrency</th>
                    </tr>
                </thead>
                <tbody id="agents-table">
                    <tr><td colspan="4">Loading...</td></tr>
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2 style="margin-top:0">Timers</h2>
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Interval</th>
                        <th>Instance</th>
                    </tr>
                </thead>
                <tbody id="timers-table">
                    <tr><td colspan="3">Loading...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let refreshInterval;

        async function fetchData() {
            try {
                const [healthRes, instancesRes, agentsRes, timersRes] = await Promise.all([
                    fetch('/api/health'),
                    fetch('/api/instances'),
                    fetch('/api/agents'),
                    fetch('/api/timers')
                ]);

                const health = await healthRes.json();
                const instances = await instancesRes.json();
                const agents = await agentsRes.json();
                const timers = await timersRes.json();

                // Update overview stats
                document.getElementById('total-instances').textContent = health.total_instances;
                document.getElementById('total-agents').textContent = health.total_agents;
                document.getElementById('total-timers').textContent = health.total_timers;
                document.getElementById('total-tasks').textContent = health.total_active_tasks;

                // Update instances table
                const instancesHtml = instances.map(i => {
                    const status = i.active ? 'active' : 'inactive';
                    const statusClass = i.active ? 'badge-active' : 'badge-inactive';
                    const heartbeatAge = i.metrics ?
                        Math.round((Date.now() / 1000) - i.metrics.last_heartbeat) + 's ago' :
                        'N/A';
                    return `<tr>
                        <td>${i.name}</td>
                        <td><code>${i.instance_id}</code></td>
                        <td>${i.pid}</td>
                        <td><span class="badge ${statusClass}">${status}</span></td>
                        <td>${i.metrics?.agents || 0}</td>
                        <td>${i.metrics?.timers || 0}</td>
                        <td>${heartbeatAge}</td>
                    </tr>`;
                }).join('');
                document.getElementById('instances-table').innerHTML = instancesHtml || '<tr><td colspan="7">No instances</td></tr>';

                // Update agents table
                const agentsHtml = agents.map(a => `<tr>
                    <td><code>${a.topic || '-'}</code></td>
                    <td>${a.group || '-'}</td>
                    <td>${a.instance_name}</td>
                    <td>${a.concurrency || 1}</td>
                </tr>`).join('');
                document.getElementById('agents-table').innerHTML = agentsHtml || '<tr><td colspan="4">No agents</td></tr>';

                // Update timers table
                const timersHtml = timers.map(t => `<tr>
                    <td>${t.name || '-'}</td>
                    <td>${t.interval || '-'}s</td>
                    <td>${t.instance_name}</td>
                </tr>`).join('');
                document.getElementById('timers-table').innerHTML = timersHtml || '<tr><td colspan="3">No timers</td></tr>';

                // Update last refresh
                document.getElementById('last-refresh').textContent = new Date().toLocaleTimeString();
            } catch (error) {
                console.error('Error fetching data:', error);
            }
        }

        function startAutoRefresh(intervalSec) {
            if (refreshInterval) clearInterval(refreshInterval);
            refreshInterval = setInterval(fetchData, intervalSec * 1000);
        }

        // Initial fetch and start auto-refresh
        fetchData();
        // Default 5 second refresh, can be configured
        startAutoRefresh(5);
    </script>
</body>
</html>
"""


async def start_dashboard(app_instance_id: str, port: int = 8000, host: str = "localhost") -> bool:
    """
    Start dashboard if not already running.

    Args:
        app_instance_id: Unique ID of the App instance
        port: Port for the dashboard server
        host: Host for the dashboard server

    Returns:
        True if this instance started the dashboard, False if already running
    """
    if not _HAS_FASTAPI:
        logger.warning("FastAPI not installed, dashboard disabled. Install with: pip install fastapi uvicorn")
        return False

    manager = DashboardManager()

    # Try to become master
    became_master = await manager.try_become_master(port, host, app_instance_id)

    if became_master:
        await manager.start_server()
        return True

    return False


async def stop_dashboard() -> None:
    """Stop dashboard if this instance is master."""
    if not _HAS_FASTAPI:
        return

    manager = DashboardManager()
    await manager.stop_server()


# Helper functions for instance registration (used by App)

async def register_instance(redis_client, instance_id: str, name: str, pid: int, host: str, start_time: float) -> None:
    """
    Register an App instance in Redis.

    Args:
        redis_client: Redis client (from RedisConnection)
        instance_id: Unique instance identifier
        name: Application name
        pid: Process ID
        host: Hostname
        start_time: Start timestamp
    """
    instance_key = f"{INSTANCES_KEY_PREFIX}{instance_id}"
    instance_data = {
        "instance_id": instance_id,
        "name": name,
        "pid": pid,
        "host": host,
        "start_time": start_time,
    }
    # Set with expiry so stale instances are cleaned up
    await redis_client.set(instance_key, json.dumps(instance_data), ex=INSTANCE_TTL * 2)
    logger.debug(f"Registered instance {instance_id} in Redis")


async def unregister_instance(redis_client, instance_id: str) -> None:
    """Unregister an App instance from Redis."""
    instance_key = f"{INSTANCES_KEY_PREFIX}{instance_id}"
    metrics_key = f"{METRICS_KEY_PREFIX}{instance_id}"
    await redis_client.delete(instance_key)
    await redis_client.delete(metrics_key)
    logger.debug(f"Unregistered instance {instance_id} from Redis")


async def update_heartbeat(redis_client, instance_id: str, metrics: Dict[str, Any]) -> None:
    """
    Update heartbeat and metrics for an instance.

    Args:
        redis_client: Redis client
        instance_id: Unique instance identifier
        metrics: Metrics dictionary (agents, timers, etc.)
    """
    metrics_key = f"{METRICS_KEY_PREFIX}{instance_id}"
    metrics["last_heartbeat"] = time.time()

    # Refresh instance key expiry as well
    instance_key = f"{INSTANCES_KEY_PREFIX}{instance_id}"
    await redis_client.expire(instance_key, INSTANCE_TTL * 2)

    # Set metrics with expiry
    await redis_client.set(metrics_key, json.dumps(metrics), ex=INSTANCE_TTL * 2)


# Public API
__all__ = [
    "DashboardManager",
    "InstanceInfo",
    "InstanceMetrics",
    "start_dashboard",
    "stop_dashboard",
    "create_app",
    "get_dashboard_html",
    "register_instance",
    "unregister_instance",
    "update_heartbeat",
]
