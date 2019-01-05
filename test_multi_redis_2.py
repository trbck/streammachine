from __future__ import print_function
import time
import random
import asyncio
import aioredis
import uvloop
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

REDIS_CONNECTION_STRING = ('localhost', 6380)
RECORDS = 10000

# from threading import Lock
# print_lock = Lock()

# def save_print(*args, **kwargs):
#   with print_lock:
#     print (*args, **kwargs)

async def clean_redis():
    r = await aioredis.create_redis(REDIS_CONNECTION_STRING)
    await r.execute(b'FLUSHALL')
    await r.execute(b'FLUSHDB')
    r.close()



async def consumer():

    group = 'group'
    consumer = "consumer-"+str(random.randint(1,10000))
    stream = 'mystream'
    redis = await aioredis.create_redis(REDIS_CONNECTION_STRING)
    
    print(consumer)

    try:
        await redis.execute(b'XGROUP', b'CREATE', stream,
                            "group", '$', 'MKSTREAM')
    except aioredis.errors.ReplyError as e:
        pass

    while True:
        # value = await redis.execute(
        #     b'XREADGROUP', b'GROUP', group, 'consumer', 'COUNT', 1, 'STREAMS', stream, '>'
        # )

        values = await redis.xread_group(
            group, consumer, ["mystream"],
            count=1,
            latest_ids=['>'])

        for v in values:
            if b'end' in v[2].keys():
                start = float(v[2][b'end'])
                end = time.time()
                print(f"f mass job took {(end - start)*1000} ms")
            elif b'time' in v[2].keys():
                start = float(v[2][b'time'])
                end = time.time()
                print(f"task {(end - start)*1000} ms")
            else:
                pass


            
async def mass_producer():
    redis = await aioredis.create_redis_pool(
    REDIS_CONNECTION_STRING,
    minsize=1,
    maxsize=5)
    while True:
        start = time.time()
        pipe = redis.pipeline()        

        for x in range(RECORDS):
            record = {'price': random.randint(10, 100)}
            pipe.xadd("mystream", record)

        # insert latest record itentifier
        record = {
            'end': str(time.time()).encode('utf-8'),
        }
        pipe.xadd("mystream", record)
        await pipe.execute()

        end = time.time()
        print(f"mass insert took {(end - start)*1000} ms")
        await asyncio.sleep(5)

async def simple_producer():
    redis = await aioredis.create_redis_pool(
    REDIS_CONNECTION_STRING,
    minsize=1,
    maxsize=5)
    while True:
        record = {'time': str(time.time()).encode('utf-8'), 'price': random.randint(10, 100)}
        redis.xadd("mystream", record)
        await asyncio.sleep(1)

def run(corofn, *args):
    policy = asyncio.get_event_loop_policy()
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        asyncio.set_event_loop(loop)
        tasks = [
            corofn(*args),
            corofn(*args),
            corofn(*args),
            corofn(*args),
        ]
        return loop.run_until_complete(asyncio.gather(*tasks))
    finally:
        loop.close()

async def main():
    executor = ProcessPoolExecutor(max_workers=4)
    #executor = ThreadPoolExecutor(max_workers=1)
    loop = asyncio.get_event_loop()

    await clean_redis()

    tasks = [
        simple_producer(),
        mass_producer(),
        loop.run_in_executor(executor, run, consumer),
        loop.run_in_executor(executor, run, consumer),
        loop.run_in_executor(executor, run, consumer),
    ]
    return await asyncio.gather(*tasks)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
