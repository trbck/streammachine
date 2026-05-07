"""
State Management Patterns for StreamMachine

This example demonstrates using Storage for shared state:
- Cross-agent state sharing
- State persistence patterns
- Consistency considerations
- Broadcasting state changes

Run with: python state_management.py
"""
import asyncio
from streammachine import App, Message, Storage

app = App(name="state_example", to_scan=True)
storage = Storage()


# Timer: Initialize shared state
@app.timer(1)
async def initialize_state():
    """Initialize state on startup."""
    # Only initialize if not already set
    initialized = await storage.read("initialized", default=False)
    if not initialized:
        await storage.write("initialized", True)
        await storage.write("counter", 0)
        await storage.write("metrics", {
            "total_processed": 0,
            "errors": 0,
            "start_time": asyncio.get_event_loop().time(),
        })
        print("[Init] State initialized")
    return


# Timer: Periodic state update
@app.timer(5)
async def update_config():
    """Periodically update configuration state."""
    # Pattern 1: Read-modify-write pattern
    config = await storage.read("config", default={})
    config["last_update"] = asyncio.get_event_loop().time()
    await storage.write("config", config)
    print(f"[Config] Updated: {config}")


# Agent: Consumer that maintains shared state
@app.agent("input_stream", group="stateful_workers")
async def stateful_processor(record: Message):
    """Process messages and update shared state."""
    # Pattern 2: Atomic counter increment
    counter = await storage.read("counter", default=0)
    await storage.write("counter", counter + 1)

    # Pattern 3: Update metrics atomically
    metrics = await storage.read("metrics", default={
        "total_processed": 0,
        "errors": 0,
    })
    metrics["total_processed"] += 1
    await storage.write("metrics", metrics)

    # Get running count
    current_count = await storage.read("counter")
    print(f"[Processor] Processed message {current_count}: {record.message}")


# Agent: Multiple consumers sharing state
@app.agent("shared_stream", group="shared_state_consumers")
async def shared_state_consumer(record: Message):
    """Consumer that shares state with other agents."""
    # Pattern 4: Deduplication using shared state
    seen_key = f"seen_{record.key}"

    if await storage.exists(seen_key):
        print(f"[Consumer] Skipping duplicate: {record.key[:16]}")
        return

    # Mark as seen
    await storage.write(seen_key, True)

    # Process
    print(f"[Consumer] Processing unique: {record.message}")

    # Pattern 5: Aggregate state
    aggregates = await storage.read("aggregates", default={})
    msg_type = record.message.get("type", "unknown")
    aggregates[msg_type] = aggregates.get(msg_type, 0) + 1
    await storage.write("aggregates", aggregates)


# Timer: Broadcast state changes
@app.timer(3)
async def broadcast_state():
    """Broadcast current state to a stream."""
    counter = await storage.read("counter", default=0)
    metrics = await storage.read("metrics", default={})
    aggregates = await storage.read("aggregates", default={})

    await app.send("state_broadcast", {
        "counter": str(counter),
        "metrics": str(metrics),
        "aggregates": str(aggregates),
    })
    print("[Broadcast] State sent to broadcast stream")


# Agent: Monitor state changes
@app.agent("state_broadcast", group="state_monitors")
async def state_monitor(record: Message):
    """Monitor state changes."""
    print(f"[Monitor] State update: {record.message}")


# Timer: Periodic state report
@app.timer(10)
async def state_report():
    """Report current state."""
    counter = await storage.read("counter", default=0)
    metrics = await storage.read("metrics", default={})
    aggregates = await storage.read("aggregates", default={})

    print("\n=== State Report ===")
    print(f"Counter: {counter}")
    print(f"Metrics: {metrics}")
    print(f"Aggregates: {aggregates}")
    print("===================\n")


# Producer for testing
@app.timer(2)
async def test_producer():
    """Produce test messages."""
    await app.send("input_stream", {"data": "test"})
    await app.send("shared_stream", {"type": "test", "data": "shared"})


if __name__ == "__main__":
    print("Starting state management example...")
    print("This example demonstrates:")
    print("  - Cross-agent state sharing")
    print("  - Read-modify-write patterns")
    print("  - Deduplication using state")
    print("  - State aggregation")
    print("  - Broadcasting state changes")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")