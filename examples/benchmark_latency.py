import asyncio
import time
import uuid
from streammachine import App, Message

NUM_MESSAGES = 10
TOPIC = f"test_bench_{uuid.uuid4()}"

latencies = []
received_count = 0
last_id = "0"

def make_record(i):
    return {"test": str(i)}

async def sender(app: App):
    for i in range(NUM_MESSAGES):
        await app.send(TOPIC, make_record(i))
        if (i + 1) % 100 == 0 or i == NUM_MESSAGES - 1:
            print(f"Sent {i+1}/{NUM_MESSAGES}")
        await asyncio.sleep(0)  # Yield to event loop

async def receiver(app: App):
    global received_count, last_id
    while received_count <= NUM_MESSAGES:
        #print(f"DEBUG: Calling xread with last_id={last_id}")
        msgs = await app.rc.client.xread({TOPIC: last_id}, count=10, block=1000)
        #print (msgs)
        if not msgs:
            continue
        for item in msgs:
            if not (isinstance(item, (list, tuple)) and len(item) == 2):
                continue  # Silently skip unexpected items
            stream, entries = item
            if not isinstance(entries, list):
                continue  # Silently skip non-list entries
            for entry in entries:
                if not (isinstance(entry, (list, tuple)) and len(entry) == 2):
                    continue  # Silently skip unexpected entry structures
                entry_id, data = entry
                #print(f"DEBUG: Received entry_id={entry_id}, data={data}")
                sent = float(data.get(b"sent", 0))
                received = time.time()
                latency = (received - sent) * 1000
                latencies.append(latency)
                received_count += 1
                print(f"DEBUG: last_id before update: {last_id}")
                # Convert entry_id to string for next xread call
                last_id = entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)
                print(f"DEBUG: last_id after update: {last_id}")
                if received_count % 100 == 0 or received_count == NUM_MESSAGES:
                    print(f"Received {received_count}/{NUM_MESSAGES}")

async def main():
    app = App()
    await asyncio.gather(sender(app), receiver(app))
    if latencies:
        print(f"Sent/Received {len(latencies)} messages.")
        print(f"Average latency: {sum(latencies)/len(latencies):.2f} ms")
        print(f"Min latency: {min(latencies):.2f} ms")
        print(f"Max latency: {max(latencies):.2f} ms")
    else:
        print("No messages received.")

if __name__ == "__main__":
    asyncio.run(main()) 