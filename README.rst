
===========================================
Python Stream Processing With Redis Streams
===========================================

For redis streams intro check: https://redis.io/topics/streams-intro


:Version: 0.1
:Keywords: stream, async, processing, data, queue, redis streams

**STREAMENGINE** aims to be a redis stream processing library on top of asyncio, aioredis and separating decorator logic for simplicity.
This project is heavily inspired by https://github.com/robinhood/faust but in are very early stage - support and suggestions a very welcome!


.. sourcecode:: python

    import time
    import random
    import aioredis

    from streamengine import App

    app = App("ExampleApp")


    @app.agent("redis_stream", concurrency=5)
    async def f(x):
        # print received stream result
        print(x)


    @app.timer(1)
    async def add():
        # sends an item to the redis stream 'redis_stream' every second
        t = str(time.time())
        fields = {'time': t.encode('utf-8'), 'price': random.randint(10, 100)}
        await app.send_once("redis_stream", fields)


    if __name__ == "__main__":
        app.main()


Installation
============
.. sourcecode:: python

    python setup.py install


