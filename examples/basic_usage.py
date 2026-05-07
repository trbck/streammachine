"""
Basic StreamMachine Usage Example

This example demonstrates the core features of StreamMachine:
- Creating an App instance
- Registering agents (message consumers)
- Registering timers (periodic tasks)
- Using shared storage
- Sending and receiving messages

Run with: python basic_usage.py

Note: Redis stream values must be flat scalars (str, bytes, int, float).
      Use json.dumps() to serialize nested structures.
"""
import json

from streammachine import App, Message

# Create the application at module level so Venusian scanning can find decorators
app = App(name="basic_example", to_scan=True)


# Timer: Sends a message every 2 seconds
@app.timer(2)
async def produce_messages():
    """Produce test messages to a stream."""
    await app.send("input_stream", {"value": "42", "source": "timer"})
    print("[Timer] Sent message to input_stream")


# Agent: Consumes messages from input_stream
@app.agent("input_stream", concurrency=1, group="processors")
async def process_input(record: Message):
    """Process incoming messages and store results."""
    print(f"[Agent] Received: {record.message}")

    # Store processed data in shared storage
    await app.storage.write("last_message", record.message)

    # Forward to another stream — Redis xadd values must be flat scalars,
    # so serialize the original message dict as JSON.
    await app.send("output_stream", {
        "original": json.dumps(record.message),
        "processed": "true",
    })


# Agent: Consumes processed messages
@app.agent("output_stream", concurrency=1, group="output_handlers")
async def handle_output(record: Message):
    """Handle processed messages."""
    msg = record.message
    print(f"[Output] Final message: {msg}")

    # Deserialize the original payload
    original = json.loads(msg.get("original", "{}"))
    print(f"[Output] Original payload: {original}")

    # Read from shared storage
    last_input = await app.storage.read("last_message")
    print(f"[Output] Last input stored: {last_input}")

    # Calculate latency if timestamps are available
    if record.sent and record.received:
        latency_ms = (record.received - record.sent) * 1000
        print(f"[Output] Latency: {latency_ms:.2f} ms")


if __name__ == "__main__":
    print("Starting StreamMachine basic example...")
    print("Press Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
