"""
Tutorial 2: Timers and Agents

This tutorial covers:
- Creating timer tasks (producers)
- Creating agent tasks (consumers)
- Combining timers and agents in a pipeline

Run with: python 02_timers_and_agents.py
"""
import asyncio
from streammachine import App, Message

app = App(name="tutorial_02", to_scan=True)


# =============================================================================
# Timers (Producers)
# =============================================================================

# Timers run periodically at a specified interval (in seconds).
# They're typically used as producers to send messages to streams.
# You can have multiple timers in an app.

@app.timer(1)  # Run every 1 second
async def fast_producer():
    """Produce messages rapidly."""
    await app.send("fast_stream", {
        "source": "fast_producer",
        "timestamp": asyncio.get_event_loop().time(),
    })


@app.timer(5)  # Run every 5 seconds
async def slow_producer():
    """Produce messages less frequently."""
    await app.send("slow_stream", {
        "source": "slow_producer",
        "timestamp": asyncio.get_event_loop().time(),
        "data": "important_event",
    })


# Timers can also be used for maintenance tasks
@app.timer(10)
async def maintenance_timer():
    """Periodic maintenance task."""
    # Clear old data, update metrics, etc.
    print("[Maintenance] Running periodic tasks...")

    # Get health status
    health = await app.health_check()
    print(f"[Maintenance] Health: {health['status']}")


# =============================================================================
# Agents (Consumers)
# =============================================================================

# Agents consume messages from streams.
# They run continuously, processing messages as they arrive.
# You can have multiple agents, each consuming from different streams.

@app.agent("fast_stream", group="fast_handlers")
async def fast_handler(record: Message):
    """Handle fast stream messages."""
    print(f"[FastHandler] {record.message.get('source')}: {record.message.get('timestamp'):.2f}")


@app.agent("slow_stream", group="slow_handlers")
async def slow_handler(record: Message):
    """Handle slow stream messages."""
    print(f"[SlowHandler] {record.message.get('source')}: {record.message.get('data')}")


# =============================================================================
# Concurrency
# =============================================================================

# Agents can process multiple messages concurrently using the 'concurrency' parameter.
# This creates multiple instances of the agent running in parallel.

@app.agent("fast_stream", group="concurrent_handlers", concurrency=3)
async def concurrent_handler(record: Message):
    """Handle messages with 3 concurrent workers."""
    # Simulate some processing time
    await asyncio.sleep(0.1)
    print(f"[ConcurrentHandler {record.key[:8]}] Processing...")

    # Forward to processed stream
    await app.send("processed", {
        "original_key": record.key,
        "processed_at": asyncio.get_event_loop().time(),
    })


# =============================================================================
# Processing Pipeline
# =============================================================================

# Agents can chain together to form a processing pipeline:
# Producer → Stream A → Agent A → Stream B → Agent B → ...

@app.agent("processed", group="final_handlers")
async def final_handler(record: Message):
    """Final stage of the pipeline."""
    print(f"[FinalHandler] Completed: {record.message.get('original_key')}")


# =============================================================================
# Timer + Agent Pattern
# =============================================================================

# A common pattern is:
# 1. Timer produces messages periodically
# 2. Agent processes those messages
# 3. Optional: Agent forwards to another stream

@app.timer(3)
async def data_generator():
    """Generate data for processing."""
    import random

    await app.send("raw_data", {
        "id": f"data_{asyncio.get_event_loop().time():.0f}",
        "value": random.randint(1, 100),
        "source": "generator",
    })


@app.agent("raw_data", group="processors")
async def data_processor(record: Message):
    """Process raw data."""
    msg = record.message

    # Transform the data
    processed = {
        "original_id": msg.get("id"),
        "value_doubled": int(msg.get("value", 0)) * 2,
        "processed_at": asyncio.get_event_loop().time(),
    }

    # Forward to next stream
    await app.send("processed_data", processed)


@app.agent("processed_data", group="output")
async def output_handler(record: Message):
    """Handle processed data."""
    print(f"[Output] {record.message}")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 2: Timers and Agents")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Multiple timers with different intervals")
    print("  - Multiple agents consuming different streams")
    print("  - Concurrency in agents")
    print("  - Processing pipelines")
    print("\nArchitecture:")
    print("  Timers:")
    print("    - fast_producer (1s) → fast_stream")
    print("    - slow_producer (5s) → slow_stream")
    print("    - data_generator (3s) → raw_data")
    print("  Agents:")
    print("    - fast_handler ← fast_stream")
    print("    - slow_handler ← slow_stream")
    print("    - concurrent_handler (x3) ← fast_stream")
    print("    - data_processor ← raw_data → processed_data")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")