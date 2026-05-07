"""
Tutorial 3: Stream Groups and Horizontal Scaling

This tutorial covers:
- Consumer groups for horizontal scaling
- Multiple consumers in the same group
- Message distribution across consumers

Run with: python 03_stream_groups.py
"""
import asyncio
import time
from streammachine import App, Message

app = App(name="tutorial_03", to_scan=True)


# =============================================================================
# Consumer Groups
# =============================================================================

# Redis Streams consumer groups enable horizontal scaling.
# Multiple consumers in the same group share the message load:
# - Each message is delivered to exactly one consumer in the group
# - If a consumer fails, messages are picked up by other consumers
# - New consumers automatically start from the last delivered message

# Producer: Generate work items
@app.timer(1)
async def work_producer():
    """Produce work items for processing."""
    for i in range(5):
        await app.send("work_items", {
            "id": f"work_{time.time():.0f}_{i}",
            "data": f"item_{i}",
            "priority": i % 3,
        })


# =============================================================================
# Multiple Consumers in Same Group
# =============================================================================

# When you run multiple instances of the same app with the same group name,
# messages are distributed among them.
#
# Instance 1: python 03_stream_groups.py
# Instance 2: python 03_stream_groups.py
#
# Both instances will share the messages from "work_items" stream.

@app.agent("work_items", group="workers")
async def worker_1(record: Message):
    """Worker instance 1 - processes work items."""
    msg = record.message

    # Simulate work
    await asyncio.sleep(0.1)

    print(f"[Worker1] Processed: {msg.get('id')} (priority: {msg.get('priority')})")


# =============================================================================
# Different Groups = Each Gets a Copy
# =============================================================================

# If you want multiple consumers to each receive EVERY message,
# use different group names:

@app.agent("work_items", group="analytics")
async def analytics_consumer(record: Message):
    """Analytics group - receives all messages."""
    print(f"[Analytics] Tracking: {record.message.get('id')}")

    # Send to analytics stream
    await app.send("analytics_stream", {
        "event": "work_processed",
        "id": record.message.get("id"),
    })


@app.agent("work_items", group="audit")
async def audit_consumer(record: Message):
    """Audit group - receives all messages for logging."""
    print(f"[Audit] Logging: {record.message.get('id')}")

    # Send to audit stream
    await app.send("audit_stream", {
        "action": "process",
        "id": record.message.get("id"),
        "timestamp": time.time(),
    })


# =============================================================================
# Concurrency Within Single Instance
# =============================================================================

# Within a single instance, use 'concurrency' to process messages in parallel.
# This creates multiple coroutines for the same agent.

@app.agent("analytics_stream", group="analytics_processors", concurrency=3)
async def analytics_processor(record: Message):
    """Process analytics with 3 concurrent handlers."""
    await asyncio.sleep(0.05)  # Simulate processing
    print(f"[AnalyticsProcessor] {record.message.get('event')}")


# =============================================================================
# Demonstrating Group Behavior
# =============================================================================

@app.timer(10)
async def status_report():
    """Report on group behavior."""
    print("\n" + "=" * 50)
    print("Consumer Group Behavior")
    print("=" * 50)
    print("\nGroups:")
    print("  - 'workers': Processes work items (horizontal scaling)")
    print("  - 'analytics': Receives copy of each message")
    print("  - 'audit': Receives copy of each message")
    print("\nTo test horizontal scaling:")
    print("  1. Run this script in multiple terminals")
    print("  2. Messages will be distributed among 'workers' groups")
    print("  3. 'analytics' and 'audit' groups each get all messages")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 3: Stream Groups and Horizontal Scaling")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Consumer groups for load distribution")
    print("  - Multiple consumers in the same group")
    print("  - Different groups each receiving all messages")
    print("  - Concurrency within a single instance")
    print("\nArchitecture:")
    print("  work_items stream")
    print("    → 'workers' group (horizontal scaling)")
    print("    → 'analytics' group (all messages)")
    print("    → 'audit' group (all messages)")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")