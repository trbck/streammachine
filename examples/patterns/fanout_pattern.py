"""
Fanout Pattern for StreamMachine

This example demonstrates one producer with multiple consumer groups:
- Multiple consumer groups reading from same stream
- Independent processing pipelines
- Different consumer configurations

Run with: python fanout_pattern.py
"""
import asyncio
from streammachine import App, Message

app = App(name="fanout_example", to_scan=True)


# Timer: Single producer sending to one stream
@app.timer(1)
async def producer():
    """Produce messages to a single stream.

    Multiple consumer groups will each receive a copy.
    """
    await app.send("events", {
        "type": "user_action",
        "user_id": "user_123",
        "action": "click",
        "timestamp": asyncio.get_event_loop().time(),
    })
    print("[Producer] Sent event")


# Agent: First consumer group - Analytics
@app.agent("events", group="analytics_pipeline")
async def analytics_consumer(record: Message):
    """Analytics pipeline processes every event for metrics."""
    msg = record.message
    print(f"[Analytics] Processing for analytics: {msg.get('type')}")

    # Simulate analytics processing
    await asyncio.sleep(0.1)

    # Send to analytics output
    await app.send("analytics_output", {
        "event_type": msg.get("type"),
        "user_id": msg.get("user_id"),
        "processed_by": "analytics",
    })


# Agent: Second consumer group - Audit Log
@app.agent("events", group="audit_pipeline")
async def audit_consumer(record: Message):
    """Audit pipeline logs every event for compliance."""
    msg = record.message
    print(f"[Audit] Logging for audit: {msg.get('type')}")

    # Simulate audit logging
    await asyncio.sleep(0.05)

    # Send to audit output
    await app.send("audit_output", {
        "event_type": msg.get("type"),
        "user_id": msg.get("user_id"),
        "timestamp": msg.get("timestamp"),
        "processed_by": "audit",
    })


# Agent: Third consumer group - Real-time Alerts
@app.agent("events", group="alert_pipeline")
async def alert_consumer(record: Message):
    """Alert pipeline triggers real-time notifications."""
    msg = record.message

    # Only process certain events
    if msg.get("action") == "click":
        print(f"[Alert] Click detected from user: {msg.get('user_id')}")

        # Send alert
        await app.send("alerts", {
            "user_id": msg.get("user_id"),
            "action": msg.get("action"),
            "processed_by": "alerts",
        })


# Agent: Fourth consumer group - Data Transformation
@app.agent("events", group="transform_pipeline")
async def transform_consumer(record: Message):
    """Transform pipeline enriches events."""
    msg = record.message

    # Enrich the event with additional data
    enriched = {
        **msg,
        "enriched": "true",
        "processing_time": asyncio.get_event_loop().time(),
    }

    print(f"[Transform] Enriched event: {msg.get('type')}")

    await app.send("enriched_events", enriched)


# Agent: Monitor all output streams
@app.agent("analytics_output", group="monitor")
async def monitor_analytics(record: Message):
    """Monitor analytics output."""
    print(f"[Monitor] Analytics: {record.message}")


@app.agent("audit_output", group="monitor")
async def monitor_audit(record: Message):
    """Monitor audit output."""
    print(f"[Monitor] Audit: {record.message}")


@app.agent("alerts", group="monitor")
async def monitor_alerts(record: Message):
    """Monitor alerts output."""
    print(f"[Monitor] Alert: {record.message}")


@app.agent("enriched_events", group="monitor")
async def monitor_enriched(record: Message):
    """Monitor enriched events."""
    print(f"[Monitor] Enriched: {record.message}")


if __name__ == "__main__":
    print("Starting fanout pattern example...")
    print("This example demonstrates:")
    print("  - One producer sending to a single stream")
    print("  - Four consumer groups receiving copies of each message")
    print("  - Independent processing pipelines")
    print("\nArchitecture:")
    print("  Producer → events stream")
    print("    → analytics_pipeline group")
    print("    → audit_pipeline group")
    print("    → alert_pipeline group")
    print("    → transform_pipeline group")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")