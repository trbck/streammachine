"""
Health Check Example

This example demonstrates health check and graceful shutdown features.

Run with: python health_check_example.py
"""
import asyncio
from streammachine import App, Message

app = App(name="health_check_example")


@app.timer(1)
async def producer():
    """Produce messages."""
    await app.send("health_test", {"timestamp": asyncio.get_event_loop().time()})


@app.agent("health_test", concurrency=1, group="test_group")
async def consumer(record: Message):
    """Consume messages."""
    print(f"[Consumer] Message received: {record.message}")


@app.timer(5)
async def health_report():
    """Report health status periodically."""
    health = await app.health_check()
    print(f"\n=== Health Check ===")
    print(f"Status: {health['status']}")
    print(f"Redis: {health['redis']}")
    print(f"Active tasks: {health['active_tasks']}")
    print(f"Registered agents: {health['registered_agents']}")
    print(f"Registered timers: {health['registered_timers']}")
    print(f"===================\n")


if __name__ == "__main__":
    print("Starting health check example...")
    print("The app will report health status every 5 seconds.")
    print("Press Ctrl+C for graceful shutdown\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nInitiating graceful shutdown...")
