import uvloop
import asyncio
import aioredis
import time
import random
from aioprocessing import AioPipe, AioProcess



REDIS_CONNECTION_STRING = "redis://localhost:6380"
CONSUMERCOUNT = 20
RECORDS = 10000



async def ProducerWorker(stream, loop=None):
    redis = await aioredis.create_redis_pool(
    REDIS_CONNECTION_STRING,
    minsize=10,
    maxsize=20,
    loop=loop)

    while True:
        start = time.time()
        pipe = redis.pipeline()        
        record = {'time': str(time.time()).encode('utf-8'), 'price': random.randint(10, 100)}

        for x in range(RECORDS):
            pipe.xadd("mystream", record)

        # insert latest record itentifier
        record = {
            'time': str(time.time()).encode('utf-8'),
            'end': "y"
        }

        pipe.xadd("mystream", record)
        await pipe.execute()

        end = time.time()


        print(f"mass insert took {(end - start)*1000} ms")
        await asyncio.sleep(5)


async def ConsumerWorker(stream, group="1", loop=None):
    redis = await aioredis.create_redis(
        REDIS_CONNECTION_STRING,
        loop=loop
    )
    try:
        await redis.execute(b'XGROUP', b'CREATE', stream,
                            group, '$', 'MKSTREAM')
    except aioredis.errors.ReplyError as e:
        pass

    while True:
        value = await redis.xread_group(
            group,
            "consumer-"+str(random.randint(1,10000)), ["mystream"],
            count=1,
            latest_ids=['>'])

        # heavy work
        #await asyncio.sleep(0.1)

        if b'end' in value[0][2].keys():
            start = float(value[0][2][b'time'])
            end = time.time()

            print(f"f mass job took {(end - start)*1000} ms")

def subprocess_code(group):

    policy = asyncio.get_event_loop_policy()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    policy.set_event_loop(policy.new_event_loop())
    loop = asyncio.get_event_loop()

    async def f(loop):
        tasks = [ConsumerWorker("mystream", group=group, loop=loop) for _ in range(CONSUMERCOUNT)]
        return await asyncio.gather(*tasks)
    #spin concurrent workers

    loop.run_until_complete(f(loop))


async def launch_process_worker(group=None):
    process = AioProcess(target=subprocess_code, args=(group, ))
    process.start()
    #await asyncio.sleep(0)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()

try:
    tasks = [
        ProducerWorker("mystream", loop=loop),
        launch_process_worker(group="1"),
    ]

    
    asyncio.gather(*tasks)





    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()