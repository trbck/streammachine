"""
Dashboard Demo - High-Throughput Stream Processing with Monitoring

This example demonstrates the StreamMachine dashboard for monitoring
multiple producers and consumers across different processes.

Architecture:
    - Producer processes: Send messages at high rate to streams
    - Consumer processes: Process messages from streams with consumer groups
    - Dashboard: Aggregates metrics from all processes at http://localhost:8000

Running the Demo:

    # Terminal 1: Start the first process (becomes dashboard master)
    python dashboard_demo.py producer_a

    # Terminal 2: Start a second process (detects existing dashboard)
    python dashboard_demo.py producer_b

    # Terminal 3: Start consumers (multiple instances share the load)
    python dashboard_demo.py consumer

    # Open browser: http://localhost:8000
    # You'll see all instances, their agents/timers, and metrics

Dashboard Features:
    - Instance tracking with heartbeat monitoring
    - Agent and timer counts per instance
    - Active task monitoring
    - Auto-refresh UI (5 seconds)

Notes:
    - First process to start becomes the dashboard master
    - Subsequent processes register themselves and skip dashboard startup
    - Redis-based distributed lock ensures only one dashboard runs
    - Dashboard is optional - requires FastAPI/uvicorn installation
"""
import asyncio
import json
import random
import time
import os
import sys
from datetime import datetime

from streammachine import App, Message


def create_producer_app(name: str, stream: str, rate_hz: float = 10):
    """
    Create a producer app that sends messages at a specified rate.

    Args:
        name: Application name for identification
        stream: Stream to send messages to
        rate_hz: Messages per second (default 10)
    """
    app = App(
        name=name,
        dashboard_enabled=True,  # Enable dashboard for monitoring
        dashboard_port=8000,
        dashboard_host="0.0.0.0",  # Allow external access
        dashboard_refresh_interval=5,
    )

    # Track metrics in storage
    message_count = {"total": 0}

    @app.timer(1 / rate_hz)  # Timer interval based on rate
    async def produce_messages():
        """Produce messages at high rate."""
        message_count["total"] += 1

        # Create message with various data types
        data = {
            "producer": name,
            "seq": message_count["total"],
            "timestamp": datetime.utcnow().isoformat(),
            "value": random.randint(1, 100),
            "type": random.choice(["order", "trade", "quote", "signal"]),
        }

        await app.send(stream, data)

        # Log every 100 messages
        if message_count["total"] % 100 == 0:
            print(f"[{name}] Sent {message_count['total']} messages to {stream}")

    @app.timer(5)  # Stats every 5 seconds
    async def log_stats():
        """Log periodic statistics."""
        rate = message_count["total"] / max(1, time.time() - app._start_time)
        print(f"[{name}] Rate: {rate:.1f} msg/s, Total: {message_count['total']}")

    return app


def create_consumer_app(name: str, streams: list, group: str = "workers"):
    """
    Create a consumer app that processes messages from multiple streams.

    Args:
        name: Application name for identification
        streams: List of streams to consume from
        group: Consumer group name (shared across instances)
    """
    app = App(
        name=name,
        dashboard_enabled=True,
        dashboard_port=8000,
        dashboard_host="0.0.0.0",
    )

    processed_count = {"total": 0}

    @app.agent(streams, group=group, concurrency=2)
    async def process_messages(record: Message):
        """Process incoming messages."""
        processed_count["total"] += 1

        # Simulate some processing work
        await asyncio.sleep(0.001)  # 1ms processing time

        # Log periodically
        if processed_count["total"] % 50 == 0:
            msg = record.message
            print(f"[{name}] Processed #{processed_count['total']}: "
                  f"{msg.get('producer', '?')} -> {msg.get('type', '?')} "
                  f"(value={msg.get('value', '?')})")

        # Store processed count in shared storage for dashboard
        await app.storage.write(f"consumer:{name}:count", processed_count["total"])

    @app.timer(10)
    async def log_consumer_stats():
        """Log consumer statistics."""
        print(f"[{name}] Total processed: {processed_count['total']}")

    return app


def create_mixed_app(name: str):
    """
    Create an app that both produces and consumes.

    This demonstrates how a single process can have multiple agents
    and timers, all visible in the dashboard.
    """
    app = App(
        name=name,
        dashboard_enabled=True,
        dashboard_port=8000,
        dashboard_host="0.0.0.0",
    )

    # Producer timer
    count = {"produced": 0, "consumed": 0}

    @app.timer(0.5)  # 2 Hz producer
    async def produce():
        """Produce messages to multiple streams."""
        count["produced"] += 1
        await app.send("stream_a", {"source": name, "seq": count["produced"]})
        await app.send("stream_b", {"source": name, "seq": count["produced"]})

        if count["produced"] % 20 == 0:
            print(f"[{name}] Produced {count['produced']} messages")

    # Consumer agents for different streams
    @app.agent("stream_a", group="group_a", concurrency=1)
    async def consume_a(record: Message):
        """Consume from stream A."""
        count["consumed"] += 1
        if count["consumed"] % 20 == 0:
            print(f"[{name}] Consumed {count['consumed']} from stream_a")

    @app.agent("stream_b", group="group_b", concurrency=1)
    async def consume_b(record: Message):
        """Consume from stream B."""
        # Just consume, no action needed
        pass

    return app


def print_usage():
    """Print usage information."""
    print("""
Dashboard Demo - High-Throughput Stream Processing with Monitoring

Usage: python dashboard_demo.py <mode> [options]

Modes:
    producer_a      Start producer A (sends to 'orders' stream at 10 Hz)
    producer_b      Start producer B (sends to 'orders' stream at 5 Hz)
    consumer        Start consumer (processes 'orders' stream)
    mixed           Start mixed producer/consumer app
    all             Start all components in one process (for demo)

Options:
    --port PORT     Dashboard port (default: 8000)
    --no-dashboard  Disable dashboard

Examples:
    # Start producer A with dashboard
    python dashboard_demo.py producer_a

    # Start consumer with custom port
    python dashboard_demo.py consumer --port 8001

    # Start mixed app without dashboard
    python dashboard_demo.py mixed --no-dashboard

Dashboard:
    Open http://localhost:8000 in your browser to see:
    - All registered App instances
    - Number of agents and timers per instance
    - Active tasks
    - Heartbeat status

The first process started becomes the dashboard master.
Subsequent processes register themselves and skip dashboard startup.
""")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    mode = sys.argv[1].lower()
    port = 8000
    dashboard = True

    # Parse options
    for i, arg in enumerate(sys.argv[2:], 2):
        if arg == "--port" and i + 1 < len(sys.argv):
            port = int(sys.argv[i + 1])
        elif arg == "--no-dashboard":
            dashboard = False

    print(f"\n{'='*60}")
    print(f"StreamMachine Dashboard Demo")
    print(f"{'='*60}")
    print(f"Mode: {mode}")
    print(f"Dashboard: {'enabled' if dashboard else 'disabled'}")
    print(f"Port: {port}")
    print(f"{'='*60}\n")

    if mode == "producer_a":
        app = create_producer_app("producer_a", "orders", rate_hz=10)
        print("Starting Producer A (10 Hz to 'orders' stream)")
        print(f"Dashboard: http://localhost:{port}")

    elif mode == "producer_b":
        app = create_producer_app("producer_b", "orders", rate_hz=5)
        print("Starting Producer B (5 Hz to 'orders' stream)")
        print(f"Dashboard: http://localhost:{port}")

    elif mode == "consumer":
        app = create_consumer_app(
            f"consumer_{os.getpid()}",  # Unique name per process
            ["orders"],
            group="order_processors"
        )
        print("Starting Consumer (processing 'orders' stream)")
        print(f"Dashboard: http://localhost:{port}")

    elif mode == "mixed":
        app = create_mixed_app(f"mixed_{os.getpid()}")
        print("Starting Mixed Producer/Consumer App")
        print(f"Dashboard: http://localhost:{port}")

    elif mode == "all":
        # Run all components in one process for quick demo
        app = App(
            name="demo_all",
            dashboard_enabled=True,
            dashboard_port=port,
            dashboard_host="0.0.0.0",
        )

        count = {"produced": 0, "consumed_a": 0, "consumed_b": 0}

        @app.timer(0.1)  # 10 Hz producer
        async def fast_producer():
            count["produced"] += 1
            await app.send("high_volume", {
                "seq": count["produced"],
                "ts": datetime.utcnow().isoformat(),
            })

        @app.agent("high_volume", group="consumers", concurrency=2)
        async def consumer_a(record: Message):
            count["consumed_a"] += 1
            if count["consumed_a"] % 100 == 0:
                print(f"Consumer A: {count['consumed_a']} messages")

        @app.agent("high_volume", group="consumers", concurrency=2)
        async def consumer_b(record: Message):
            count["consumed_b"] += 1
            if count["consumed_b"] % 100 == 0:
                print(f"Consumer B: {count['consumed_b']} messages")

        @app.timer(5)
        async def stats():
            rate = count["produced"] / max(1, time.time() - app._start_time)
            print(f"\n--- Stats ---")
            print(f"Produced: {count['produced']} ({rate:.1f} msg/s)")
            print(f"Consumed A: {count['consumed_a']}")
            print(f"Consumed B: {count['consumed_b']}")
            print(f"-------------\n")

        print("Starting all-in-one demo")
        print("Producer: 10 Hz to 'high_volume' stream")
        print("Consumers: 2 agents sharing the load")
        print(f"Dashboard: http://localhost:{port}")

    else:
        print(f"Unknown mode: {mode}")
        print_usage()
        sys.exit(1)

    # Override dashboard settings if specified
    if not dashboard:
        app.config.dashboard_enabled = False
    if port != 8000:
        app.config.dashboard_port = port

    print("\nPress Ctrl+C to stop\n")

    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main()