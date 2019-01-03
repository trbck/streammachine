import time
import random

from streamengine import App

app = App("ExampleApp")


@app.agent("redis_stream", concurrency=5)
async def f(x):
    # print received stream result
    print(x)


@app.timer(1)
async def add():
    # timer function sends an item to the redis stream 'redis_stream' every second
    t = str(time.time())
    fields = {'time': t.encode('utf-8'), 'price': random.randint(10, 100)}
    await app.send_once("redis_stream", fields)


@app.on_start()
async def send_many_messages():
    # send many items at once, also to the stream 'redis_stream'
    RECORDS = 1000
    redis = await app.create_sender()
    pipe = redis.pipeline()
    for x in range(RECORDS):
        t = str(time.time())
        fields = {'time': t.encode('utf-8'), 'price': random.randint(10,100)}
        pipe.xadd("redis_stream", fields)
    await pipe.execute()



if __name__ == "__main__":
    app.main()
