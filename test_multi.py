import asyncio
import uvloop
import time
from aioprocessing import AioPipe, AioProcess

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def subprocess_code():
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()

    async def f():
        while True:
            print("worker process func f")
            await asyncio.sleep(2)

    #asyncio.ensure_future(f())
    #asyncio.ensure_future(secondWorker("loop2 worker2"))

    tasks = [f() for _ in range(1000)]
    asyncio.gather(*tasks)
    loop.run_forever()


async def firstWorker(value):
    while True:
        await asyncio.sleep(1)
        print(value)


async def secondWorker(value):
    while True:
        await asyncio.sleep(0.1)
        print(value)
       

async def launch_worker():
    process = AioProcess(target=subprocess_code, args=())
    process.start()
    await asyncio.sleep(0)

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(firstWorker("loop1 worker1"))
    asyncio.ensure_future(secondWorker("loop1 worker2"))
    asyncio.ensure_future(launch_worker())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()