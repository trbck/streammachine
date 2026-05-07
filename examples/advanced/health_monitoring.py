"""
Health Monitoring Example for StreamMachine

This example demonstrates health monitoring patterns:
- Health check endpoints
- Metrics exposition
- Graceful degradation
- Monitoring integration

Run with: python health_monitoring.py
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from streammachine import App, Message

app = App(name="health_example", to_scan=True)


# =============================================================================
# Metrics Collection
# =============================================================================

@dataclass
class Metrics:
    """Simple metrics collection."""
    messages_processed: int = 0
    messages_failed: int = 0
    processing_time_ms: float = 0.0
    last_message_time: Optional[float] = None
    start_time: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "avg_processing_time_ms": (
                self.processing_time_ms / self.messages_processed
                if self.messages_processed > 0 else 0
            ),
            "uptime_seconds": time.time() - self.start_time,
            "last_message_time": self.last_message_time,
        }


# Shared metrics
metrics = Metrics()


# =============================================================================
# Health Check
# =============================================================================

@app.timer(10)
async def health_check():
    """Periodic health check."""
    health = await app.health_check()

    # Add custom metrics
    health["metrics"] = metrics.to_dict()

    # Determine health status
    error_rate = (
        metrics.messages_failed / metrics.messages_processed
        if metrics.messages_processed > 0 else 0
    )

    if error_rate > 0.1:
        health["status"] = "degraded"
        health["error_rate"] = error_rate
    elif metrics.messages_processed == 0:
        health["status"] = "starting"
    else:
        health["status"] = "healthy"

    print(f"\n[Health] {health['status'].upper()}")
    print(f"  Processed: {metrics.messages_processed}")
    print(f"  Failed: {metrics.messages_failed}")
    print(f"  Uptime: {health['metrics']['uptime_seconds']:.0f}s\n")

    # Store for external monitoring
    await app.storage.write("health_status", health)


# =============================================================================
# Message Processing with Metrics
# =============================================================================

@app.timer(2)
async def producer():
    """Produce messages with some failures."""
    import random

    # Occasionally produce "bad" messages to test error handling
    if random.random() < 0.1:
        await app.send("work_stream", {"type": "bad", "data": "fail"})
    else:
        await app.send("work_stream", {"type": "good", "data": f"work_{time.time():.0f}"})


@app.agent("work_stream", group="workers")
async def worker(record: Message):
    """Process messages and collect metrics."""
    start_time = time.time()

    try:
        msg_type = record.message.get("type", "good")

        if msg_type == "bad":
            raise ValueError("Simulated processing error")

        # Simulate processing
        await asyncio.sleep(0.01)

        # Track success
        metrics.messages_processed += 1
        metrics.last_message_time = time.time()

        # Track processing time
        elapsed_ms = (time.time() - start_time) * 1000
        metrics.processing_time_ms += elapsed_ms

        # Send to output
        await app.send("output_stream", {
            "input": record.message,
            "processing_time_ms": elapsed_ms,
            "status": "success",
        })

    except Exception as e:
        # Track failure
        metrics.messages_failed += 1
        metrics.messages_processed += 1

        # Send to error stream
        await app.send("error_stream", {
            "input": record.message,
            "error": str(e),
            "status": "failed",
        })


# =============================================================================
# Error Tracking
# =============================================================================

@app.agent("error_stream", group="error_trackers")
async def error_tracker(record: Message):
    """Track and aggregate errors."""
    error = record.message

    # Get current error counts
    errors = await app.storage.read("errors", default={})
    error_type = error.get("error", "unknown")
    errors[error_type] = errors.get(error_type, 0) + 1
    await app.storage.write("errors", errors)

    print(f"[ErrorTracker] Error: {error_type}, Count: {errors[error_type]}")


# =============================================================================
# Metrics Exposure (for Prometheus, etc.)
# =============================================================================

async def get_metrics_text() -> str:
    """Generate Prometheus-style metrics text."""
    m = metrics

    lines = [
        "# HELP messages_processed Total messages processed",
        "# TYPE messages_processed counter",
        f"messages_processed {m.messages_processed}",
        "",
        "# HELP messages_failed Total messages failed",
        "# TYPE messages_failed counter",
        f"messages_failed {m.messages_failed}",
        "",
        "# HELP processing_time_ms Total processing time in ms",
        "# TYPE processing_time_ms counter",
        f"processing_time_ms {m.processing_time_ms}",
        "",
        "# HELP uptime_seconds Application uptime in seconds",
        "# TYPE uptime_seconds gauge",
        f"uptime_seconds {time.time() - m.start_time:.0f}",
    ]

    return "\n".join(lines)


@app.timer(30)
async def expose_metrics():
    """Expose metrics for external monitoring."""
    metrics_text = await get_metrics_text()
    print("\n[Metrics] Current metrics:")
    print("---")
    print(metrics_text)
    print("---\n")

    # In production, you would:
    # 1. Serve metrics on /metrics endpoint (using aiohttp, fastapi, etc.)
    # 2. Push to Prometheus Pushgateway
    # 3. Send to StatsD, DataDog, etc.


# =============================================================================
# Alerting
# =============================================================================

@app.timer(30)
async def check_alerts():
    """Check for alerting conditions."""
    # Get health status
    health = await app.storage.read("health_status", default={})

    # Check error rate
    error_rate = (
        metrics.messages_failed / metrics.messages_processed
        if metrics.messages_processed > 0 else 0
    )

    # Alert if error rate too high
    if error_rate > 0.2:
        await app.send("alerts", {
            "type": "high_error_rate",
            "error_rate": error_rate,
            "processed": metrics.messages_processed,
            "failed": metrics.messages_failed,
        })


@app.agent("alerts", group="alert_handlers")
async def handle_alert(record: Message):
    """Handle alerts."""
    alert = record.message
    print(f"\n[ALERT] {alert.get('type')}: {alert}\n")

    # In production, you would:
    # 1. Send to PagerDuty, Slack, etc.
    # 2. Log to error tracking system
    # 3. Trigger auto-remediation


if __name__ == "__main__":
    print("Starting health monitoring example...")
    print("This example demonstrates:")
    print("  - Health check patterns")
    print("  - Metrics collection")
    print("  - Error tracking")
    print("  - Alerting conditions")
    print("\nPress Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")