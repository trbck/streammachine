import time
import random

from streamengine import App

app = App("ExampleApp")


@app.agent("somestream", concurrency=5)
async def f(x):
    # print received stream record
    print(x)


@app.timer(1)
async def add():
    # sends a new item to the redis stream 'somestream' every second
    record = {'time': str(time.time()).encode('utf-8'), 'price': random.randint(10, 100)}
    await app.send_once("somestream", record)


if __name__ == "__main__":
    app.main()