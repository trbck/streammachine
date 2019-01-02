"""Python Stream Processing with Redis Streams."""
import sys
import inspect
import time
import random
import asyncio
import aioredis
import uvloop
import venusian
from collections import OrderedDict

__all__ = ["App"]

class App(object):
    def __init__(self, name):
        self.name = name
        self.config = dict()
        self.config["start"] = None

    def main(self):
        #get App calling module path to let venusian scanner know where to check for decoraters and functions
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])

        #venusian scan for decorators
        self.registry = Registry()
        scanner = venusian.Scanner(registry=self.registry)
        scanner.scan(mod)

        #start loop with registered decorated functions
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.get_event_loop()

        #self.loop.run_until_complete(self.initiate_tasks(self.loop))
        self.loop.create_task(self.initiate_tasks())
        self.loop.run_forever()
        self.loop.close()


    async def agent_consumer_container(self, wrapped_func, agent_config, loop):
        """
        Agent consumer container for listening to redis streams and subsequently call wrapped function with received information.

        """
        redis = await aioredis.create_redis(
            'redis://localhost:6380', loop=loop)

        try:
            await redis.execute(b'XGROUP', b'CREATE', agent_config["stream"],
                                agent_config["group"], '$', 'MKSTREAM')
        except aioredis.errors.ReplyError as e:
            pass

        while True:
            value = await redis.xread_group(
                agent_config["group"],
                agent_config["consumer"], [agent_config["stream"]],
                count=1,
                latest_ids=['>'])
            result = await wrapped_func(value)

    async def timer_container(self, wrapped_func, config):
        """
        Timer consumer container for listening to redis streams and subsequently call wrapped function with received information

        """
        while True:
            await asyncio.sleep(config["value"])
            result = await wrapped_func()

    async def on_start_container(self, wrapped_func):
        await wrapped_func()

    async def initiate_tasks(self):
        # TODO needs a clean and efficient approach!
        task_list = []

        for item in self.registry.registered:
            if item[0] == "agent":
                config = item[2].copy()
                config["consumer"] = str(item[1])

                if config["group"] == None:
                    config["group"] = "group"

                if config["concurrency"] != None:
                    if isinstance(config["concurrency"], int):
                        for x in range(config["concurrency"]):
                            c = config.copy()
                            c["consumer"] = str(item[1]) + " ID " + str(
                                random.randint(1, 1000))
                            task_list.append(
                                self.agent_consumer_container(
                                    item[1], c, self.loop))

                else:
                    task_list.append(
                        self.agent_consumer_container(item[1], config,
                                                      self.loop))

            if item[0] == "timer":
                config = item[2].copy()
                task_list.append(self.timer_container(item[1], config))

            if item[0] == "on_start":
                # do this first!
                await self.on_start_container(item[1])

        #add the time the other jobs (agents) should start, for later monitoring etc.
        self.config["start"] = time.time()

        #get all the prepared tasks (agents, timer, on_start)
        await asyncio.gather(*task_list)

    """
    send new records to redis stream
    """

    async def send_once(self, stream, value):
        """
        TODO improve performance if possible

        This is a one time stream sending function.
        """
        redis = await aioredis.create_redis(
            'redis://localhost:6380', loop=self.loop)
        await redis.xadd(stream, value)
        redis.close()

    async def create_sender(self, minsize=10, maxsize=20):
        """
        TODO improve performance if possible

        This returns a redis pool object to be returned to an timer or agent function. 
        """
        return await aioredis.create_redis_pool(
            'redis://localhost:6380',
            minsize=minsize,
            maxsize=maxsize,
            loop=self.loop)

    """
    Decorator classes (app.agent, app.timer, app.on_start, ...) - used in the script and scanned by venusian lib.
    """

    class agent(object):
        def __init__(self,
                     stream,
                     model=None,
                     group=None,
                     concurrency=None,
                     processes=None):
            self.decorater_type = "agent"
            self.config = {
                "stream": stream,
                "model": model,
                "group": group,
                "concurrency": concurrency,
                "processes": processes
            }

        def __call__(self, wrapped):
            me = self

            class Wrapper(object):
                def __init__(self, wrapped_func):
                    self.callback = wrapped

                def on_scan(self, scanner, name, obj):
                    def decorated(*args, **kwargs):
                        v = wrapped_func(*args, **kwargs)
                        return v

                    #self.callback = decorated
                    scanner.registry.add(me.decorater_type, self.callback,
                                         me.config)

                def __call__(self, *args, **kwargs):
                    return self.callback(*args, **kwargs)

            w = Wrapper(wrapped)
            venusian.attach(w, w.on_scan)
            return w

    class timer(object):
        def __init__(self, value):
            self.decorater_type = "timer"
            self.config = {"value": value}

        def __call__(self, wrapped):
            me = self

            class Wrapper(object):
                def __init__(self, wrapped_func):
                    self.callback = wrapped

                def on_scan(self, scanner, name, obj):
                    def decorated(*args, **kwargs):
                        v = wrapped_func(*args, **kwargs)
                        return v

                    #self.callback = decorated
                    scanner.registry.add(me.decorater_type, self.callback,
                                         me.config)

                def __call__(self, *args, **kwargs):
                    return self.callback(*args, **kwargs)

            w = Wrapper(wrapped)
            venusian.attach(w, w.on_scan)
            return w

    class on_start(object):
        def __init__(self):
            self.decorater_type = "on_start"

        def __call__(self, wrapped):
            me = self

            class Wrapper(object):
                def __init__(self, wrapped_func):
                    self.callback = wrapped

                def send(self):
                    print("in test")

                def on_scan(self, scanner, name, obj):
                    def decorated(*args, **kwargs):
                        v = wrapped_func(*args, **kwargs)
                        return v

                    #self.callback = decorated
                    scanner.registry.add(me.decorater_type, self.callback, [])

                def __call__(self, *args, **kwargs):
                    return self.callback(*args, **kwargs)

            w = Wrapper(wrapped)
            venusian.attach(w, w.on_scan)
            return w


class Registry(object):
    """venusian registry class
    
    Arguments:
        None
    """

    def __init__(self):
        self.registered = []

    def add(self, name, obj, config):
        self.registered.append((name, obj, config))


if __name__ == "__main__":
    pass
