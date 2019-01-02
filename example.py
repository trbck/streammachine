import time
import random
import aioredis

from streamengine import App

app = App("test")


@app.agent("test", concurrency=5)
async def f(x):
    if b'timer' in x[0][2].keys():
        start = float(x[0][2][b'start'])
        end = time.time()
        print(f"f job took {(end - start)*1000} ms")

    if b'theend' in x[0][2].keys():
        #end = float(x[0][2][b'theend'])
        end = time.time()
        start = app.config["start"]
        print(f"f mass job took {(end - start)*1000} ms")


@app.timer(1)
async def add():
    t = str(time.time())
    fields = {'start': t.encode('utf-8'), 'timer': 'YES'.encode('utf-8')}
    await app.send_once("test", fields)


@app.on_start()
async def send_mass_messages(loop):
    RECORDS = 10000
    start = time.time()
    redis = await app.create_sender()
    pipe = redis.pipeline()
    for x in range(RECORDS):
        t = str(time.time())
        fields = {'start': t.encode('utf-8')}
        pipe.xadd("test", fields)
    await pipe.execute()
    

    #theend; send last message and mark it as such
    t = str(time.time())
    fields = {'start': t.encode('utf-8'), 'theend': "YES"}
    await redis.xadd("test", fields)

    end = time.time()
    print(f"mass insert took {(end - start)*1000} ms")


if __name__ == "__main__":
    app.main()

    
