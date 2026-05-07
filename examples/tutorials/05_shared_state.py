"""
Tutorial 5: Shared State Between Agents

This tutorial covers:
- Using Storage for cross-agent state
- Read-modify-write patterns
- State synchronization

Run with: python 05_shared_state.py
"""
import asyncio
import time
from streammachine import App, Message

app = App(name="tutorial_05", to_scan=True)


# =============================================================================
# Shared State with Storage
# =============================================================================

# Storage provides shared state across all agents and timers.
# It uses multiprocessing.Manager for cross-process state sharing.
#
# Key operations:
# - await app.storage.write(key, value) - Store a value
# - await app.storage.read(key, default) - Read a value
# - await app.storage.delete(key) - Delete a value
# - await app.storage.exists(key) - Check if key exists
# - await app.storage.keys() - List all keys

# =============================================================================
# Counter Pattern
# =============================================================================

@app.timer(1)
async def increment_counter():
    """Increment a shared counter."""
    # Read current value
    count = await app.storage.read("counter", default=0)

    # Modify
    count += 1

    # Write back
    await app.storage.write("counter", count)

    # Note: This read-modify-write is NOT atomic!
    # For atomic operations, use Redis directly or implement locking.
    # See the state_management pattern for atomic patterns.

    if count % 5 == 0:
        print(f"[Counter] Count: {count}")


# =============================================================================
# State Aggregation
# =============================================================================

@app.timer(2)
async def produce_events():
    """Produce events to aggregate."""
    import random

    event_type = random.choice(["click", "view", "purchase"])
    user_id = f"user_{random.randint(1, 10)}"

    await app.send("events", {
        "type": event_type,
        "user_id": user_id,
        "timestamp": time.time(),
    })


@app.agent("events", group="aggregators")
async def aggregate_events(record: Message):
    """Aggregate events by type."""
    msg = record.message
    event_type = msg.get("type", "unknown")

    # Get current counts
    counts = await app.storage.read("event_counts", default={})

    # Update
    counts[event_type] = counts.get(event_type, 0) + 1

    # Write back
    await app.storage.write("event_counts", counts)


# =============================================================================
# Per-User State
# =============================================================================

@app.agent("events", group="user_trackers")
async def track_user_events(record: Message):
    """Track events per user."""
    msg = record.message
    user_id = msg.get("user_id", "unknown")

    # Get user's event history
    key = f"user_events_{user_id}"
    events = await app.storage.read(key, default=[])

    # Add new event
    events.append({
        "type": msg.get("type"),
        "timestamp": msg.get("timestamp"),
    })

    # Keep only last 100 events
    events = events[-100:]

    # Write back
    await app.storage.write(key, events)


# =============================================================================
# State Query Pattern
# =============================================================================

@app.timer(10)
async def query_state():
    """Query shared state."""
    # Get counter
    counter = await app.storage.read("counter", default=0)

    # Get event counts
    counts = await app.storage.read("event_counts", default={})

    # List all keys
    keys = await app.storage.keys()

    print("\n" + "=" * 50)
    print("State Query")
    print("=" * 50)
    print(f"Counter: {counter}")
    print(f"Event counts: {counts}")
    print(f"Total keys: {len(keys)}")
    print(f"User tracking keys: {[k for k in keys if k.startswith('user_events_')]}")
    print("=" * 50 + "\n")


# =============================================================================
# Cleanup Pattern
# =============================================================================

@app.timer(30)
async def cleanup_old_state():
    """Periodically clean up old state."""
    # Get all keys
    keys = await app.storage.keys()

    # Example: Clean up old user event histories
    for key in keys:
        if key.startswith("user_events_"):
            # In production, check timestamps and clean up old entries
            pass

    print(f"[Cleanup] Checked {len(keys)} keys")


# =============================================================================
# Atomic Update Pattern (Conceptual)
# =============================================================================

async def atomic_increment(key: str, amount: int = 1) -> int:
    """Atomically increment a counter using Storage locks.

    Note: This is conceptual - in production, use Redis INCR
    or implement proper distributed locking.
    """
    # Storage doesn't have built-in atomic operations,
    # but you can implement them using locks

    # Get the per-key lock
    lock = app.storage._get_lock(key)

    async with lock:
        # Read-modify-write within lock
        value = await app.storage.read(key, default=0)
        value += amount
        await app.storage.write(key, value)
        return value


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 5: Shared State Between Agents")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Using Storage for cross-agent state")
    print("  - Counter pattern")
    print("  - State aggregation")
    print("  - Per-user state tracking")
    print("  - State querying")
    print("\nStorage API:")
    print("  await app.storage.write(key, value)")
    print("  await app.storage.read(key, default)")
    print("  await app.storage.delete(key)")
    print("  await app.storage.exists(key)")
    print("  await app.storage.keys()")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")