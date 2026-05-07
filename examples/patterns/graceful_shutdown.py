"""
Graceful Shutdown Patterns for StreamMachine

This example demonstrates proper shutdown handling:
- Signal handling (SIGINT, SIGTERM)
- Cleanup of resources
- Timeout patterns
- In-flight message handling

Run with: python graceful_shutdown.py
"""
import asyncio
import signal
from streammachine import App, Message

app = App(name="shutdown_example", to_scan=True)

# Track in-flight messages
inflight_messages = set()


@app.timer(2)
async def producer():
    """Produce messages periodically."""
    await app.send("work_queue", {"data": "work_item"})
    print("[Producer] Sent work item")


@app.agent("work_queue", group="workers")
async def worker(record: Message):
    """Process messages with cleanup on shutdown."""
    msg_id = record.key
    inflight_messages.add(msg_id)

    try:
        # Simulate work that takes time
        print(f"[Worker] Processing {msg_id[:16]}...")
        await asyncio.sleep(1)  # Simulate processing time

        # Mark as done
        print(f"[Worker] Completed {msg_id[:16]}")

    except asyncio.CancelledError:
        # Pattern 1: Handle cancellation gracefully
        print(f"[Worker] Cancelled while processing {msg_id[:16]}")

        # Pattern 2: Save checkpoint/state for recovery
        await app.storage.write(f"checkpoint_{msg_id}", {
            "status": "interrupted",
            "data": record.message,
        })

        # Re-raise to propagate cancellation
        raise

    finally:
        # Pattern 3: Always clean up
        inflight_messages.discard(msg_id)


async def cleanup_before_shutdown():
    """Perform cleanup before shutdown completes."""
    print("\n[Cleanup] Starting graceful shutdown...")

    # Wait for in-flight messages
    timeout = 10.0
    start = asyncio.get_event_loop().time()

    while inflight_messages and (asyncio.get_event_loop().time() - start) < timeout:
        print(f"[Cleanup] Waiting for {len(inflight_messages)} in-flight messages...")
        await asyncio.sleep(0.5)

    if inflight_messages:
        print(f"[Cleanup] Timeout! {len(inflight_messages)} messages still in-flight")
        # In production, save these to persistent storage for recovery
    else:
        print("[Cleanup] All messages processed")

    # Pattern 4: Close external connections
    # await external_db.close()
    # await api_client.close()

    print("[Cleanup] Cleanup complete")


# Override default shutdown for custom cleanup
original_shutdown = app.shutdown


async def custom_shutdown():
    """Custom shutdown with cleanup."""
    # Run cleanup
    await cleanup_before_shutdown()

    # Call original shutdown
    await original_shutdown()


app.shutdown = custom_shutdown


# Timer: Periodic checkpoint
@app.timer(5)
async def checkpoint():
    """Save state periodically for recovery."""
    await app.storage.write("last_checkpoint", {
        "inflight_count": len(inflight_messages),
        "inflight_ids": list(inflight_messages),
    })
    print(f"[Checkpoint] Saved state: {len(inflight_messages)} in-flight")


if __name__ == "__main__":
    print("Starting graceful shutdown example...")
    print("This example demonstrates:")
    print("  - Handling SIGINT/SIGTERM")
    print("  - Cleaning up in-flight messages")
    print("  - Timeout patterns")
    print("  - State checkpointing")
    print("\nPress Ctrl+C to trigger graceful shutdown\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Keyboard interrupt received")