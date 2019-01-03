# streamengine

===========================================
Python Stream Processing With Redis Streams
===========================================

Check: https://redis.io/topics/streams-intro

|license|

:Version: 0.1

:Keywords: stream, async, processing, data, queue, redis

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
