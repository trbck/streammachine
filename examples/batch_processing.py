"""
Batch Processing Example

This example shows how to send batches of messages efficiently
using the send_batch method.

Run with: python batch_processing.py
"""
from streammachine import App, Message

app = App(name="batch_example")

BATCH_SIZE = 100


@app.timer(2)
async def batch_producer():
    """Produce a batch of messages."""
    records = [
        {"id": i, "data": f"batch_item_{i}"}
        for i in range(BATCH_SIZE)
    ]
    ids = await app.send_batch("batch_stream", records)
    print(f"[Producer] Sent batch of {len(ids)} messages")


@app.agent("batch_stream", concurrency=1, group="batch_processors")
async def batch_consumer(record: Message):
    """Process batch messages one at a time."""
    print(f"[Consumer] Processing: {record.message}")


if __name__ == "__main__":
    print("Starting batch processing example...")
    print(f"Sending {BATCH_SIZE} messages every 2 seconds")
    print("Press Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
