"""
Error Handling Patterns for StreamMachine

This example demonstrates robust error handling patterns:
- Try/except in message handlers
- Retry logic with exponential backoff
- Dead letter queues for failed messages
- Graceful error recovery

Run with: python error_handling.py
"""
import json
import asyncio
from typing import Optional
from streammachine import App, Message

app = App(name="error_handling_example", to_scan=True)

# Track retry counts (in production, use Redis or persistent storage)
retry_counts: dict = {}


async def send_to_dead_letter_queue(msg: Message, error: Exception) -> None:
    """Send failed message to dead letter queue for later analysis."""
    await app.send("dead_letter_queue", {
        "original_topic": msg.topic,
        "original_key": msg.key,
        "original_data": json.dumps(dict(msg.data) if msg.data else {}),
        "error": str(error),
        "retry_count": retry_counts.get(msg.key, 0),
    })


async def handle_with_retry(
    msg: Message,
    handler,
    max_retries: int = 3,
    backoff_ms: int = 100,
) -> bool:
    """Handle message with retry logic and exponential backoff.

    Args:
        msg: The message to process
        handler: Async handler function
        max_retries: Maximum retry attempts
        backoff_ms: Initial backoff in milliseconds

    Returns:
        True if message was processed successfully, False otherwise
    """
    key = msg.key
    retry_count = retry_counts.get(key, 0)

    for attempt in range(max_retries):
        try:
            await handler(msg)
            # Success - clear retry count
            if key in retry_counts:
                del retry_counts[key]
            return True
        except Exception as e:
            retry_counts[key] = retry_count + attempt + 1

            if attempt < max_retries - 1:
                # Exponential backoff
                wait_ms = backoff_ms * (2 ** attempt)
                print(f"[Retry {attempt + 1}/{max_retries}] Waiting {wait_ms}ms for {key}")
                await asyncio.sleep(wait_ms / 1000)
            else:
                # Final attempt failed - send to DLQ
                print(f"[Error] All retries exhausted for {key}: {e}")
                await send_to_dead_letter_queue(msg, e)
                return False

    return False


# Timer: Produce messages that might fail processing
@app.timer(2)
async def produce_messages():
    """Produce test messages, some designed to fail."""
    import random
    message_type = random.choice(["valid", "invalid", "slow"])

    await app.send("input_stream", {
        "type": message_type,
        "data": f"message_{asyncio.get_event_loop().time():.2f}",
    })
    print(f"[Producer] Sent {message_type} message")


# Agent: Process messages with error handling
@app.agent("input_stream", group="error_handlers")
async def process_with_error_handling(record: Message):
    """Process messages with comprehensive error handling."""
    msg_type = record.message.get("type", "valid")
    data = record.message.get("data", "")

    # Pattern 1: Validate input
    if not data:
        raise ValueError("Empty data field")

    # Pattern 2: Handle specific failure modes
    if msg_type == "invalid":
        raise ValueError("Simulated processing error")

    if msg_type == "slow":
        # Simulate slow processing that might timeout
        await asyncio.sleep(5)

    # Pattern 3: Wrap handler logic in try/except
    try:
        # Simulate processing
        result = f"processed_{data}"
        await app.send("output_stream", {
            "result": result,
            "original_type": msg_type,
        })
        print(f"[Handler] Successfully processed: {msg_type}")

    except Exception as e:
        # Pattern 4: Log and decide
        print(f"[Handler] Error processing {record.key}: {e}")

        # Pattern 5: Retry or send to DLQ
        success = await handle_with_retry(
            record,
            lambda m: app.send("output_stream", {"result": "retried", "error": str(e)}),
        )

        if not success:
            # Already sent to DLQ by handle_with_retry
            pass


# Agent: Monitor dead letter queue
@app.agent("dead_letter_queue", group="dlq_monitors")
async def monitor_dead_letter_queue(record: Message):
    """Process messages from dead letter queue for analysis."""
    msg = record.message
    print(f"[DLQ Monitor] Failed message from topic '{msg.get('original_topic')}':")
    print(f"  Error: {msg.get('error')}")
    print(f"  Retry count: {msg.get('retry_count')}")

    # In production, you might:
    # - Alert operations team
    # - Store in database for analysis
    # - Attempt recovery
    # - Forward to external monitoring system


# Timer: Health check
@app.timer(10)
async def health_check():
    """Periodic health check."""
    health = await app.health_check()
    print(f"[Health] Status: {health}")


if __name__ == "__main__":
    print("Starting error handling example...")
    print("This example demonstrates:")
    print("  - Try/except patterns in handlers")
    print("  - Retry logic with exponential backoff")
    print("  - Dead letter queue for failed messages")
    print("  - Graceful error recovery")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")