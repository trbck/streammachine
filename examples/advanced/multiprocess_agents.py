"""
Multiprocess Agents Example for StreamMachine

This example demonstrates using multiple processes for CPU-bound work:
- Running agents with processes=N
- Cross-process state sharing via Storage
- Process pool configuration

Run with: python multiprocess_agents.py
"""
import asyncio
from streammachine import App, Message

# Create app with process pool configuration
app = App(
    name="multiprocess_example",
    to_scan=True,
    max_processes=4,  # Maximum worker processes
)


# Timer: Producer running in main process
@app.timer(1)
async def producer():
    """Produce messages for processing."""
    import time
    await app.send("work_queue", {
        "data": f"work_item_{time.time():.0f}",
        "timestamp": time.time(),
    })
    print("[Producer] Sent work item")


# Agent: CPU-bound work with multiple processes
# NOTE: Multiprocess agents use the 'processes' parameter
# This example shows the pattern, but actual multiprocess execution
# requires careful handling of pickling and state sharing.

# For demonstration, we'll show a standard agent that could be
# configured for multiprocess:

@app.agent("work_queue", group="workers", concurrency=2)
async def worker(record: Message):
    """Process messages - can be CPU-bound work.

    In production, you would use:
    @app.agent("work_queue", processes=4)
    async def worker(record: Message):
        # This runs in separate processes

    Each process has its own event loop and Redis connection.
    State is shared via Storage using multiprocessing.Manager.
    """
    import time

    # Simulate CPU-bound work
    start = time.time()

    # Example: Number crunching
    result = sum(i * i for i in range(10000))

    elapsed = time.time() - start

    # Update shared state
    processed = await app.storage.read("total_processed", default=0)
    await app.storage.write("total_processed", processed + 1)

    print(f"[Worker] Processed in {elapsed:.4f}s: {record.message.get('data')}")
    print(f"[Worker] Result: {result}, Total processed: {processed + 1}")

    # Forward result
    await app.send("results", {
        "input": record.message.get("data"),
        "result": result,
        "processing_time": elapsed,
    })


# Agent: Result collector
@app.agent("results", group="collectors")
async def result_collector(record: Message):
    """Collect results from workers."""
    msg = record.message
    print(f"[Collector] Result for {msg.get('input')}: {msg.get('result')}")


# Timer: Monitor multiprocess state
@app.timer(5)
async def monitor():
    """Monitor multiprocess statistics."""
    processed = await app.storage.read("total_processed", default=0)
    print(f"\n[Monitor] Total processed: {processed}\n")


if __name__ == "__main__":
    print("Starting multiprocess agents example...")
    print("This example demonstrates:")
    print("  - Using processes=N for CPU-bound work")
    print("  - Cross-process state sharing via Storage")
    print("  - Process pool configuration")
    print("\nNote: In production, use @app.agent('stream', processes=N)")
    print("for true multiprocess execution.\n")
    print("Press Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")