"""
Connection Pooling Example for StreamMachine

This example demonstrates Redis connection pool configuration:
- Pool sizing for different workloads
- Connection reuse patterns
- Monitoring connection pool stats

Run with: python connection_pooling.py
"""
import asyncio
from streammachine import App, Message, RedisConnection

app = App(name="pooling_example", to_scan=True)


# =============================================================================
# Connection Pool Configuration
# =============================================================================

# The RedisConnection class manages connection pooling automatically.
# Configuration is via environment variables or constructor params:
#
# Environment variables:
#   REDIS_URL=redis://localhost:6379/0
#   REDIS_MAX_CONNECTIONS=10 (default)
#
# Or programmatically:
#   rc = RedisConnection(
#       host="localhost",
#       port=6379,
#       db=0,
#       max_connections=50,  # Pool size
#   )

# Example pool sizing recommendations:
# - Single agent, low throughput: 10 connections
# - Multiple agents, moderate throughput: 25-50 connections
# - High throughput, many agents: 100+ connections
# - Rule of thumb: max_connections >= concurrency * agents * 2


# =============================================================================
# High-Throughput Configuration Example
# =============================================================================

# For high-throughput applications, create a dedicated connection pool:
high_throughput_pool = RedisConnection(
    url="redis://localhost:6379/0",
    max_connections=100,
)


# =============================================================================
# Producer with Connection Reuse
# =============================================================================

@app.timer(0.5)
async def high_speed_producer():
    """Produce messages at high speed."""
    # app.send() uses the app's shared RedisConnection internally
    # This is efficient because connections are reused from the pool
    await app.send("high_speed_stream", {
        "timestamp": asyncio.get_event_loop().time(),
        "data": "fast_message",
    })


# =============================================================================
# Consumer with Connection Monitoring
# =============================================================================

@app.agent("high_speed_stream", group="fast_consumers", concurrency=5)
async def fast_consumer(record: Message):
    """Consume messages with connection reuse."""
    # Each consumer uses the app's RedisConnection
    # The pool manages connection allocation automatically

    # Simulate processing
    await asyncio.sleep(0.001)

    # The connection pool will reuse connections across all 5 concurrency tasks
    timestamp = record.message.get("timestamp", "0")
    print(f"[FastConsumer] Processed: {timestamp}")


# =============================================================================
# Manual Connection Management
# =============================================================================

async def manual_connection_example():
    """Example of manual connection management."""
    # Create a dedicated connection for special operations
    dedicated_conn = RedisConnection(max_connections=5)

    async with dedicated_conn:
        # Connection pool is automatically managed within context
        await dedicated_conn.client.set("key", "value")
        value = await dedicated_conn.client.get("key")
        print(f"[Manual] Got value: {value}")

        # Connection is returned to pool after operation
        # Pool is closed when exiting context manager


# =============================================================================
# Connection Pool Stats Monitoring
# =============================================================================

@app.timer(10)
async def monitor_pool_stats():
    """Monitor connection pool statistics."""
    # Get pool info from the app's Redis connection
    try:
        await app.rc._ensure_pool()

        # Note: coredis doesn't expose all pool stats like redis-py
        # But you can check pool size via:
        pool = app.rc.client.connection_pool

        print("\n[Pool Stats]")
        print(f"  Pool class: {pool.__class__.__name__}")
        print(f"  Max connections: {app.rc._max_connections}")

        # Health check
        healthy = await app.rc.health_check()
        print(f"  Health: {'OK' if healthy else 'FAIL'}\n")

    except Exception as e:
        print(f"[Pool Stats] Error: {e}")


# =============================================================================
# Batch Operations (Pipeline)
# =============================================================================

@app.timer(5)
async def batch_producer():
    """Demonstrate batch operations for efficiency."""
    # Batch operations are more efficient than individual sends
    # because they use Redis pipeline

    messages = [
        {"id": f"batch_{i}", "timestamp": asyncio.get_event_loop().time()}
        for i in range(10)
    ]

    # Use pipeline_xadd for batch sending
    await app.rc._ensure_pool()
    ids = await app.rc.pipeline_xadd("batch_stream", messages)

    print(f"[BatchProducer] Sent {len(ids)} messages in batch")


@app.agent("batch_stream", group="batch_consumers")
async def batch_consumer(record: Message):
    """Consume batch messages."""
    print(f"[BatchConsumer] Received: {record.message}")


# =============================================================================
# Best Practices
# =============================================================================

# 1. Use app.send() for most operations - it uses a shared connection pool
# 2. Use app.send_batch() or pipeline_xadd() for bulk operations
# 3. Set max_connections based on your concurrency needs
# 4. Monitor pool stats during high load
# 5. Use async context managers for dedicated connections

# =============================================================================
# Pool Sizing Calculator
# =============================================================================

def calculate_pool_size(num_agents: int, concurrency: int, operations_per_agent: int = 2) -> int:
    """Calculate recommended pool size.

    Args:
        num_agents: Number of agents in your application
        concurrency: Concurrency setting for each agent
        operations_per_agent: Average concurrent operations per agent task

    Returns:
        Recommended max_connections value
    """
    base_size = num_agents * concurrency * operations_per_agent

    # Add buffer for timers and other operations
    buffer = 10

    # Round up to nearest 5
    recommended = ((base_size + buffer + 4) // 5) * 5

    return max(recommended, 10)  # Minimum 10


if __name__ == "__main__":
    print("Starting connection pooling example...")
    print("This example demonstrates:")
    print("  - Connection pool configuration")
    print("  - Connection reuse patterns")
    print("  - Batch operations")
    print("  - Pool monitoring")
    print(f"\nRecommended pool size for this app: {calculate_pool_size(3, 5)}")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")