import asyncio
import time
import aioredis
import random
import uvloop
import pyfirmata
import time



asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()


#board = pyfirmata.Arduino('/dev/ttyACM0')
async def read_temperature_and_humidity(board):

    sw = board.digital[10].read()
    if sw is True:
        board.digital[13].write(1)
    else:
        board.digital[13].write(0)

    temperature=board.digital[15].read()
    humidity=board.digital[16].read()

    return temperature,humidity


async def add_message_with_sleep(redis, loop, stream):

    for _ in range(10):

        #temperature, humidity = await read_temperature_and_humidity(board=board)

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
