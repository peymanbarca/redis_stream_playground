import sys
import asyncio
import time
import aioredis
import random
import uvloop
import json

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()


async def process_result_msg(records,result):
    print(records, " processing {result} : ", result)
    msg_producer_stream = (result[0][0]).decode('ascii')
    msg_id = (result[0][1]).decode('ascii')
    print(msg_producer_stream,msg_id)
    return msg_producer_stream,msg_id

async def process_message_legacy(redis, loop, group, consumer, streams):

    records = 0
    while True:
        result = await redis.xread_group(group, consumer, streams, count=1, latest_ids=['>'])
        if result:
            records += 1
            msg_producer_stream, msg_id = await process_result_msg(records,result)
            try:
                ack = await redis.xack(stream=msg_producer_stream,group_name=group,id=msg_id)
                if ack!=1:
                    print('Failure in Acknowledgment')
            except Exception as e:
                print(e)

        else:
            print("Timeout")
            break


async def process_message(redis, loop, group, consumer, streams,records_consumed):



    result = await redis.xread_group(group, consumer, streams, count=1, latest_ids=['>'])
    if result:
        records_consumed+=1
        msg_producer_stream, msg_id = await process_result_msg(records_consumed,result)
        try:
            ack = await redis.xack(stream=msg_producer_stream, group_name=group, id=msg_id)
            if ack != 1:
                print('Failure in Acknowledgment')
        except Exception as e:
            print(e)

    else:
        print("Timeout")
        loop.stop()

    loop.create_task(main(consumer,records_consumed))


async def main(consumer,records_consumed):
    redis = await aioredis.create_redis('redis://localhost', loop=loop)
    streams = ['chennai']
    group = 'mygroup'

    for stream in streams:
        exists = await redis.exists(stream)
        if not exists:
            await redis.xadd(stream, {b'foo': b'bar'})

        try:
            await redis.xgroup_create(stream, group)
        except aioredis.errors.ReplyError as e:
            print("Consumer group already exists")

    records_consumed = await process_message(redis, loop, group, consumer, streams,records_consumed)
    redis.close()
    await redis.wait_closed()


if len(sys.argv) < 2:
    print("Consumer name not passed")
    consumer = "mamad_consumer"
else:
    consumer = sys.argv[1]


#Legacy Run
#loop.run_until_complete(main(consumer))

#Modern Run
records_consumed=0
loop.create_task(main(consumer,records_consumed))
loop.run_forever()
