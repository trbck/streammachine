"""
Multiple Consumers Example

This example shows how to run multiple consumers for the same stream
with different consumer groups for parallel processing.

Run with: python multiple_consumers.py
"""
from streammachine import App, Message

app = App(name="multiple_consumers")


# Timer to produce messages
@app.timer(1)
async def producer():
    for i in range(5):
        await app.send("work_queue", {"task_id": i, "data": f"task_{i}"})


# First consumer group - processes tasks
@app.agent("work_queue", concurrency=2, group="processors")
async def processor(record: Message):
    print(f"[Processor-Group] Processing: {record.message}")


# Second consumer group - logs all messages
@app.agent("work_queue", concurrency=1, group="loggers")
async def logger(record: Message):
    print(f"[Logger-Group] Logging: {record.message}")


if __name__ == "__main__":
    print("Starting multiple consumers example...")
    print("Press Ctrl+C to stop\n")
    try:
        app.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
