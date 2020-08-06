import asyncio
import time
import aioredis
import random
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

loop = asyncio.get_event_loop()


async def add_message_with_sleep(redis, loop, stream):

    for _ in range(10):
        temperature = str(random.randrange(20, 30))
        humidity = str(random.randrange(10, 20))
        fields = {'temperature': temperature.encode('utf-8'),
                  'humidity': humidity.encode('utf-8')}
        print(_,fields)
        await redis.xadd(stream, fields)

    #asyncio.sleep(5)
    time.sleep(5)

    print('Data for This Round Produced!!!')
    loop.create_task(main())


async def main():
    redis = await aioredis.create_redis('redis://localhost', loop=loop)
    stream = 'chennai'
    await add_message_with_sleep(redis, loop, stream)
    # redis.close()
    # await redis.wait_closed()

loop.create_task(main())
loop.run_forever()
