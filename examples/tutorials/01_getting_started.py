"""
Tutorial 1: Getting Started with StreamMachine

This tutorial covers the basics:
- Creating an App
- Sending messages to streams
- Consuming messages with agents

Run with: python 01_getting_started.py
"""
from streammachine import App, Message

# =============================================================================
# Creating an App
# =============================================================================

# The App is the main entry point for StreamMachine.
# It manages:
# - Event loop (uvloop for better performance)
# - Redis connections
# - Task discovery (finding @app.agent and @app.timer decorators)
# - Graceful shutdown

app = App(name="tutorial_01", to_scan=True)

# to_scan=True means the app will scan this module for decorated functions.
# Set to_scan=False if you want to manually register tasks.


# =============================================================================
# Sending Messages
# =============================================================================

# Messages are sent to Redis Streams using app.send()
# This is typically done in timers (producers) or other agents.

# Timer that runs every 2 seconds
@app.timer(2)
async def send_hello():
    """Send a message every 2 seconds."""
    # app.send(stream_name, data_dict) sends to a Redis stream
    await app.send("greetings", {
        "message": "Hello, StreamMachine!",
        "count": 1,
    })
    print("[Producer] Sent greeting message")


# =============================================================================
# Consuming Messages
# =============================================================================

# Messages are consumed by agents (decorators on async functions)
# Each agent:
# - Creates a consumer group on the stream
# - Processes messages from that group
# - Can run with multiple consumers for horizontal scaling

@app.agent("greetings", group="greeters")
async def greet_handler(record: Message):
    """Process greeting messages."""
    # The Message object contains:
    # - topic: Stream name the message came from
    # - key: Message ID (stream entry ID)
    # - sent: Timestamp when message was sent (if present)
    # - received: Timestamp when message was received
    # - data: Raw field-values from Redis
    # - message: Decoded dict of field-values

    print(f"[Consumer] Received: {record.message}")
    print(f"  Topic: {record.topic}")
    print(f"  Key: {record.key}")

    # You can access latency
    if record.sent and record.received:
        latency_ms = (record.received - record.sent) * 1000
        print(f"  Latency: {latency_ms:.2f}ms")


# =============================================================================
# Running the App
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 1: Getting Started with StreamMachine")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Creating an App instance")
    print("  - Sending messages with app.send()")
    print("  - Consuming messages with @app.agent()")
    print("\nArchitecture:")
    print("  Timer (send_hello) → 'greetings' stream → Agent (greet_handler)")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")