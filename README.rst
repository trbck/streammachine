
===========================================
Python Stream Processing With Redis Streams
===========================================

For redis streams intro check: https://redis.io/topics/streams-intro


:Version: 0.1
:Keywords: stream, async, processing, data, queue, redis streams

**STREAMENGINE** aims to be a redis stream processing library on top of asyncio, aioredis and separating decorator logic for simplicity.

This project is heavily inspired by https://github.com/robinhood/faust but in an early stage - support and suggestions are therefore very welcome!



.. sourcecode:: python

    import time
    import random

    from streamengine import App

    app = App("ExampleApp")


    @app.agent("mystream", concurrency=5)
    async def f(x):
        # Prints received stream record from 'mystream' stream.
        print(x)


    @app.timer(1)
    async def add():
        # Sends a record to the redis stream 'mystream' every second.
        record = {'timestamp': str(time.time()).encode('utf-8'), 'price': random.randint(10, 100)}
        await app.send_once("mystream", record)


    if __name__ == "__main__":
        app.main()


Installation
============
.. sourcecode:: python

    python setup.py install


