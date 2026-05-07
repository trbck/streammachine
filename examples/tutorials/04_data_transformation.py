"""
Tutorial 4: Data Transformation Pipeline

This tutorial covers:
- Reading from one stream
- Transforming data
- Writing to another stream

Run with: python 04_data_transformation.py
"""
import asyncio
import time
from streammachine import App, Message

app = App(name="tutorial_04", to_scan=True)


# =============================================================================
# Stage 1: Raw Data Producer
# =============================================================================

@app.timer(2)
async def raw_data_producer():
    """Produce raw data for transformation."""
    import random

    await app.send("raw_events", {
        "event_id": f"evt_{time.time():.0f}",
        "event_type": random.choice(["click", "view", "purchase"]),
        "user_id": f"user_{random.randint(1, 100)}",
        "value": random.randint(1, 1000),
        "timestamp": time.time(),
    })
    print("[Producer] Sent raw event")


# =============================================================================
# Stage 2: Validation
# =============================================================================

@app.agent("raw_events", group="validators")
async def validate_events(record: Message):
    """Validate and filter raw events."""
    msg = record.message

    # Validation logic
    if not msg.get("event_id"):
        print("[Validator] Invalid event: missing event_id")
        await app.send("invalid_events", {
            "reason": "missing_event_id",
            "original": str(msg),
        })
        return

    if not msg.get("user_id"):
        print("[Validator] Invalid event: missing user_id")
        await app.send("invalid_events", {
            "reason": "missing_user_id",
            "original": str(msg),
        })
        return

    # Valid event - forward to valid stream
    validated = {
        **msg,
        "validated": True,
        "validation_time": time.time(),
    }

    await app.send("valid_events", validated)
    print(f"[Validator] Valid: {msg.get('event_id')}")


# =============================================================================
# Stage 3: Enrichment
# =============================================================================

@app.agent("valid_events", group="enrichers")
async def enrich_events(record: Message):
    """Enrich events with additional data."""
    msg = record.message

    # Simulate enrichment (in production, lookup from database/API)
    enriched = {
        **msg,
        # Add derived fields
        "value_category": "high" if int(msg.get("value", 0)) > 500 else "low",
        # Add computed fields
        "day_of_week": time.strftime("%A"),
        # Add enrichment timestamp
        "enriched_at": time.time(),
    }

    await app.send("enriched_events", enriched)
    print(f"[Enricher] Enriched: {msg.get('event_id')}")


# =============================================================================
# Stage 4: Aggregation (Optional)
# =============================================================================

# State for aggregation
event_counts = {}

@app.agent("enriched_events", group="aggregators")
async def aggregate_events(record: Message):
    """Aggregate events by type."""
    msg = record.message
    event_type = msg.get("event_type", "unknown")

    # Update counts
    event_counts[event_type] = event_counts.get(event_type, 0) + 1

    # Also store in shared Storage for cross-agent access
    await app.storage.write("event_counts", event_counts)

    # Send aggregated data periodically (conceptual - in production use a timer)
    await app.send("aggregated_events", {
        "event_type": event_type,
        "count": event_counts[event_type],
        "last_event_id": msg.get("event_id"),
    })


# =============================================================================
# Stage 5: Final Output
# =============================================================================

@app.agent("aggregated_events", group="output")
async def final_output(record: Message):
    """Final output - could send to external system."""
    print(f"\n[Output] Aggregated: {record.message}\n")


# =============================================================================
# Error Stream Handler
# =============================================================================

@app.agent("invalid_events", group="error_handlers")
async def handle_invalid(record: Message):
    """Handle invalid events."""
    print(f"[Error] Invalid event: {record.message}")


# =============================================================================
# Status Monitor
# =============================================================================

@app.timer(10)
async def pipeline_status():
    """Report pipeline status."""
    counts = await app.storage.read("event_counts", default={})

    print("\n" + "=" * 50)
    print("Pipeline Status")
    print("=" * 50)
    print(f"Event counts: {counts}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 4: Data Transformation Pipeline")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Multi-stage transformation pipeline")
    print("  - Validation and filtering")
    print("  - Data enrichment")
    print("  - Aggregation")
    print("  - Error handling")
    print("\nPipeline stages:")
    print("  1. raw_events (producer)")
    print("  2. valid_events (validation)")
    print("  3. enriched_events (enrichment)")
    print("  4. aggregated_events (aggregation)")
    print("  5. final output")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")