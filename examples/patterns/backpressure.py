"""
Backpressure Patterns for StreamMachine

This example demonstrates handling slow consumers and backpressure:
- Monitoring queue depth
- Throttling fast producers
- Circuit breaker patterns
- Graceful degradation

Run with: python backpressure.py
"""
import asyncio
import time
from streammachine import App, Message, Storage

app = App(name="backpressure_example", to_scan=True)

# Shared state for monitoring
queue_metrics = Storage()


async def get_queue_depth(stream_name: str) -> int:
    """Get approximate queue depth using XLEN.

    In production, you might use XINFO for more details.
    """
    try:
        await app.rc._ensure_pool()
        length = await app.rc.client.xlen(stream_name)
        return length
    except Exception:
        return 0


async def check_backpressure(threshold: int = 1000) -> bool:
    """Check if we should apply backpressure.

    Returns True if queue is above threshold (slow consumer detected).
    """
    depth = await get_queue_depth("input_stream")
    return depth > threshold


# Timer: Fast producer with backpressure awareness
@app.timer(1)
async def smart_producer():
    """Produce messages, but slow down if consumer can't keep up."""
    # Check queue depth before producing
    depth = await get_queue_depth("input_stream")

    # Pattern 1: Stop producing when queue is full
    if depth > 1000:
        print(f"[Producer] Backpressure detected! Queue depth: {depth}")
        await queue_metrics.write("backpressure_events",
            await queue_metrics.read("backpressure_events", default=0) + 1)
        return

    # Pattern 2: Reduce production rate based on depth
    if depth > 500:
        print(f"[Producer] Reducing rate. Queue depth: {depth}")
        # Skip this cycle to let consumer catch up
        return

    # Normal production
    await app.send("input_stream", {
        "timestamp": time.time(),
        "data": f"message_{time.time():.2f}",
    })
    print(f"[Producer] Sent message. Queue depth: {depth}")


# Timer: Throttled producer with rate limiting
@app.timer(0.5)
async def throttled_producer():
    """Produce with built-in rate limiting."""
    # Get last send time
    last_send = await queue_metrics.read("last_send_time", default=0)
    min_interval = 0.1  # Minimum 100ms between sends
    current_time = time.time()

    # Pattern 3: Enforce minimum interval between sends
    if current_time - last_send < min_interval:
        return

    await app.send("throttled_stream", {"data": "throttled"})
    await queue_metrics.write("last_send_time", current_time)
    print("[Throttled] Sent message")


# Agent: Slow consumer that simulates processing time
@app.agent("input_stream", group="slow_consumers")
async def slow_consumer(record: Message):
    """A consumer that processes messages slowly."""
    # Simulate variable processing time
    import random
    processing_time = random.uniform(0.1, 0.5)

    # Pattern 4: Measure and log processing time
    start = time.time()
    await asyncio.sleep(processing_time)
    elapsed = time.time() - start

    print(f"[Consumer] Processed in {elapsed:.3f}s: {record.message.get('data', '')[:20]}")

    # Track throughput
    processed = await queue_metrics.read("processed_count", default=0)
    await queue_metrics.write("processed_count", processed + 1)


# Agent: Consumer with circuit breaker
@app.agent("throttled_stream", group="circuit_breaker_consumers")
async def circuit_breaker_consumer(record: Message):
    """Consumer with circuit breaker pattern."""
    # Get failure count
    failure_count = await queue_metrics.read("failure_count", default=0)
    circuit_open = await queue_metrics.read("circuit_open", default=False)

    # Pattern 5: Circuit breaker
    if circuit_open:
        # Circuit is open - fail fast
        print("[CircuitBreaker] Circuit open, skipping message")
        return

    try:
        # Process message
        await asyncio.sleep(0.01)  # Simulate work
        print(f"[CircuitBreaker] Processed: {record.message}")

        # Reset failure count on success
        await queue_metrics.write("failure_count", 0)

    except Exception as e:
        failure_count += 1
        await queue_metrics.write("failure_count", failure_count)

        # Open circuit if threshold exceeded
        if failure_count >= 5:
            print(f"[CircuitBreaker] Opening circuit after {failure_count} failures")
            await queue_metrics.write("circuit_open", True)

            # Schedule circuit close
            asyncio.create_task(reset_circuit_after_delay(30))


async def reset_circuit_after_delay(delay: float):
    """Reset circuit breaker after delay."""
    await asyncio.sleep(delay)
    await queue_metrics.write("circuit_open", False)
    await queue_metrics.write("failure_count", 0)
    print("[CircuitBreaker] Circuit closed, resuming processing")


# Timer: Monitor and report metrics
@app.timer(5)
async def monitor_metrics():
    """Monitor and report queue metrics."""
    depth = await get_queue_depth("input_stream")
    processed = await queue_metrics.read("processed_count", default=0)
    backpressure = await queue_metrics.read("backpressure_events", default=0)

    print(f"\n[Metrics] Queue depth: {depth}")
    print(f"[Metrics] Processed: {processed}")
    print(f"[Metrics] Backpressure events: {backpressure}")

    # Calculate throughput
    last_processed = await queue_metrics.read("last_processed_count", default=0)
    throughput = (processed - last_processed) / 5  # Messages per second
    await queue_metrics.write("last_processed_count", processed)
    print(f"[Metrics] Throughput: {throughput:.1f} msg/s\n")


if __name__ == "__main__":
    print("Starting backpressure example...")
    print("This example demonstrates:")
    print("  - Monitoring queue depth")
    print("  - Throttling fast producers")
    print("  - Circuit breaker patterns")
    print("  - Graceful degradation")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")