"""
Tutorial 6: Production Ready StreamMachine

This tutorial covers:
- Health checks
- Graceful shutdown
- Logging
- Error handling

Run with: python 06_production_ready.py
"""
import asyncio
import logging
import time
from streammachine import App, Message

# =============================================================================
# Logging Configuration
# =============================================================================

# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("production_app")


# =============================================================================
# Production App Configuration
# =============================================================================

app = App(
    name="production_app",
    to_scan=True,
    max_processes=4,  # Process pool for CPU-bound work
    max_threads=4,    # Thread pool for blocking I/O
)


# =============================================================================
# Health Check
# =============================================================================

# Use app.health_check() to verify Redis connectivity and app status

@app.timer(30)
async def health_monitor():
    """Periodic health check."""
    health = await app.health_check()

    if health["status"] != "healthy":
        logger.warning(f"Health check degraded: {health}")
        # In production: Alert operations team
    else:
        logger.info(f"Health check OK: {health}")


# =============================================================================
# Error Handling in Agents
# =============================================================================

# Track errors for monitoring
error_counts = {}


@app.timer(2)
async def producer():
    """Produce messages, some that may fail."""
    import random

    # Occasionally produce problematic messages
    if random.random() < 0.1:
        await app.send("work_stream", {"type": "bad", "data": None})
    else:
        await app.send("work_stream", {
            "type": "good",
            "data": f"work_{time.time():.0f}",
        })


@app.agent("work_stream", group="workers")
async def worker(record: Message):
    """Process messages with error handling."""
    msg = record.message

    try:
        # Validate message
        if msg.get("type") == "bad":
            raise ValueError("Invalid message type")

        # Process message
        logger.info(f"Processing: {msg.get('data')}")

        # Send to output
        await app.send("output_stream", {
            "input": msg.get("data"),
            "processed_at": time.time(),
        })

        # Track success
        error_counts["success"] = error_counts.get("success", 0) + 1

    except ValueError as e:
        # Handle expected errors
        logger.warning(f"Validation error: {e}")

        # Send to dead letter queue
        await app.send("dead_letter", {
            "original": msg,
            "error": str(e),
            "timestamp": time.time(),
        })

        error_counts["validation_error"] = error_counts.get("validation_error", 0) + 1

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error: {e}", exc_info=True)

        # Send to error queue
        await app.send("errors", {
            "original": msg,
            "error": str(e),
            "traceback": str(e.__dict__),
            "timestamp": time.time(),
        })

        error_counts["unexpected_error"] = error_counts.get("unexpected_error", 0) + 1


# =============================================================================
# Dead Letter Queue Handler
# =============================================================================

@app.agent("dead_letter", group="dlq_handlers")
async def handle_dead_letter(record: Message):
    """Handle messages that failed validation."""
    logger.warning(f"Dead letter: {record.message}")
    # In production: Alert operations, store for analysis


@app.agent("errors", group="error_handlers")
async def handle_errors(record: Message):
    """Handle unexpected errors."""
    logger.error(f"Error queue: {record.message}")
    # In production: Alert operations, store for analysis


# =============================================================================
# Graceful Shutdown
# =============================================================================

# StreamMachine handles SIGINT and SIGTERM automatically.
# You can also trigger shutdown programmatically:

async def graceful_shutdown():
    """Custom shutdown logic."""
    logger.info("Starting graceful shutdown...")

    # Wait for in-flight messages (tracked manually)
    in_flight = await app.storage.read("in_flight_count", default=0)
    if in_flight > 0:
        logger.info(f"Waiting for {in_flight} in-flight messages...")
        # In production: Wait with timeout

    # Store final state
    await app.storage.write("shutdown_time", time.time())
    await app.storage.write("error_counts", error_counts)

    logger.info("Shutdown complete")


# =============================================================================
# Metrics Collection
# =============================================================================

@app.timer(10)
async def collect_metrics():
    """Collect and report metrics."""
    # Get counts
    total_processed = error_counts.get("success", 0)
    total_errors = sum(v for k, v in error_counts.items() if k != "success")

    # Calculate error rate
    total = total_processed + total_errors
    error_rate = total_errors / total if total > 0 else 0

    # Store metrics
    await app.storage.write("metrics", {
        "total_processed": total_processed,
        "total_errors": total_errors,
        "error_rate": error_rate,
        "timestamp": time.time(),
    })

    # Log metrics
    logger.info(f"Metrics: processed={total_processed}, errors={total_errors}, rate={error_rate:.2%}")


# =============================================================================
# Cleanup on Startup/Shutdown
# =============================================================================

@app.timer(1)
async def startup_init():
    """One-time startup initialization."""
    # Only run once
    initialized = await app.storage.read("initialized", default=False)
    if initialized:
        return

    logger.info("Performing startup initialization...")

    # Initialize state
    await app.storage.write("initialized", True)
    await app.storage.write("start_time", time.time())
    await app.storage.write("in_flight_count", 0)

    logger.info("Startup complete")


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Tutorial 6: Production Ready StreamMachine")
    print("=" * 60)
    print("\nThis tutorial demonstrates:")
    print("  - Logging configuration")
    print("  - Health checks")
    print("  - Error handling patterns")
    print("  - Dead letter queues")
    print("  - Metrics collection")
    print("  - Graceful shutdown")
    print("\nProduction checklist:")
    print("  ✓ Configure logging")
    print("  ✓ Add health check endpoint")
    print("  ✓ Handle errors gracefully")
    print("  ✓ Use dead letter queues")
    print("  ✓ Collect metrics")
    print("  ✓ Plan for graceful shutdown")
    print("\nPress Ctrl+C to stop\n")
    print("=" * 60 + "\n")

    try:
        app.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        # Custom cleanup runs here
    finally:
        logger.info("Application stopped")