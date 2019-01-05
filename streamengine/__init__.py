"""Python Stream Processing with Redis Streams."""
import sys
import inspect
import time
import random
import asyncio
import aioredis
import pickle


REDIS_CONNECTION_STRING = "redis://localhost:6380"
#from distex import Pool
#from multiprocessing import Pool
#import multiprocessing
#from multiprocessing import Process

from concurrent.futures import ProcessPoolExecutor

import uvloop
import venusian
from collections import OrderedDict

from .util import Registry, Agent, Timer, Event

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

__all__ = ["App"]

class App():
    def __init__(self, name, to_scan=True):
        self.name = name
        self.config = dict()
        self.config["start"] = None
        self.config["to_scan"] = to_scan

        # venusian pre scan for agent and timer decorators
        self.registry = Registry()
        self.agent = Agent
        self.timer = Timer
        self.event = Event

    def main(self):
        # entry point of the script
        self.loop = asyncio.get_event_loop()

        if self.config["to_scan"] == True:
            # get App calling module path to let venusian scanner know where to check for decoraters and functions
            frm = inspect.stack()[1]
            mod = inspect.getmodule(frm[0])
            scanner = venusian.Scanner(registry=self.registry)
            scanner.scan(mod)

        self.loop.create_task(self.__initiate_tasks())
        self.loop.run_forever()
        self.loop.close()

    @property
    def agents_with_processes_count(self):
        return [
            x for x in self.registry.registered
            if x[0] == "agent" and x[2]["processes"] != None
        ].copy()

    async def agent_consumer_container(self, wrapped_func, agent_config, loop):
        """
        Agent consumer container for listening to redis streams and subsequently call wrapped function with received information.

        """
        redis = await aioredis.create_redis(REDIS_CONNECTION_STRING, loop=loop)

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

    async def start_agents_in_seperate_process(self):
        pass

    def start_sub_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.start_agents_in_seperate_process())

    async def start_process_container(self, executor, id):
        await asyncio.get_event_loop().run_in_executor(executor, sub_loop)

    async def __initiate_tasks(self):
        # TODO needs a clean and efficient approach!

        task_list = []

        for item in self.registry.registered:
            if item[0] == "agent":
                config = item[2].copy()
                config["consumer"] = str(item[1])

                if config["group"] == None:
                    config["group"] = "group"

                if isinstance(config["concurrency"], int) and not isinstance(config["processes"], int):
                    # concurrency is set, multiprocessing not set
                    for x in range(config["concurrency"]):
                        c = config.copy()
                        c["consumer"] = str(item[1]) + " ID " + str(
                            random.randint(1, 1000))
                        task_list.append(
                            self.agent_consumer_container(
                                item[1], c, self.loop)
                                )

                elif isinstance(config["concurrency"], int) and isinstance(config["processes"], int):
                    # concurrency IS set, multiprocessing IS set
                    # trigger process processing

                    reg = self.agents_with_processes_count
                    reg[0][2]["processes"] = None
                    frm = inspect.stack()[1]
                    mod = inspect.getmodule(frm[0])

                    print(str(reg[0][1]))


                    #self.loop.create_task(start_process_container(ProcessPoolExecutor(), reg))


                # elif not isinstance(config["concurrency"], int) and isinstance(config["processes"], int):
                #     # concurrency is NOT set, multiprocessing IS set
                #     task_list.append(
                #         self.agent_consumer_container(item[1], config,
                #                                       self.loop))


                else:
                    # concurrency is not set, multiprocessing also not set == 1 agent container to be started here
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
        redis = await aioredis.create_redis(REDIS_CONNECTION_STRING, loop=self.loop)
        await redis.xadd(stream, value)
        redis.close()

    async def create_sender(self, minsize=10, maxsize=20):
        """
        TODO improve performance if possible

        This returns a redis pool object to be returned to an timer or agent function. 
        """
        return await aioredis.create_redis_pool(
            REDIS_CONNECTION_STRING,
            minsize=minsize,
            maxsize=maxsize,
            loop=self.loop)
        

if __name__ == "__main__":
    pass
