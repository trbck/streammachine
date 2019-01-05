import time
import random

from streamengine import App

app = App("ExampleApp")


@app.agent("redis_stream", group="1", concurrency=20)
async def f(x):
    if b'theend' in x[0][2].keys():
        #end = float(x[0][2][b'theend'])
        end = time.time()
        start = app.config["start"]
        print(f"f mass job took {(end - start)*1000} ms")
    if b'add' in x[0][2].keys():
        print("f: "+str(x))

@app.agent("redis_stream", group="2", concurrency=5, processes=2)
async def f1(x):
    if b'theend' in x[0][2].keys():
        #end = float(x[0][2][b'theend'])
        end = time.time()
        start = app.config["start"]
        print(f"f1 mass job took {(end - start)*1000} ms")
    if b'add' in x[0][2].keys():
        print("f1: "+str(x))

# @app.timer(1)
# async def add():
#     # timer function sends an item to the redis stream 'redis_stream' every second
#     t = str(time.time())
#     fields = {'time': t.encode('utf-8'), 'add': "YES"}
#     await app.send_once("redis_stream", fields)

@app.event("on_start")
async def send_many_messages():
    # send many items at once, also to the stream 'redis_stream
    start = time.time()
    RECORDS = 10
    redis = await app.create_sender()
    pipe = redis.pipeline()
    for x in range(RECORDS):
        t = str(time.time())
        fields = {'time': t.encode('utf-8'), 'price': random.randint(10,100)}
        pipe.xadd("redis_stream", fields)

    #theend
    t = str(time.time())
    fields = {'start': t.encode('utf-8'), 'theend': "OK"}
    pipe.xadd("redis_stream", fields)
    await pipe.execute()

    end = time.time()
    print(f"mass insert took {(end - start)*1000} ms")

if __name__ == "__main__":
    app.main()
