import json
import pika
import asyncio
import websockets

def callback(ch, method, properties, body):
    print(json.loads(body))

async def my_connect():
    async with websockets.connect('ws://localhost:8000/') as websocket:
        for i in range(1, 2, 1):
            req = {'date_from': '2021-01-19', 'time_from': '08', 'time_to': '08', 'data_type': 'all', 'monitor_stocks': ['000020', '005930'], 'frequency': 'D'}
            await websocket.send(json.dumps(req))
            data_rcv = await websocket.recv()
            print(data_rcv) # socket 서버에서 토픽명 전달 받음
                            # 전달받은 토픽명을 리스닝하며 데이터를 스트리밍 받는다

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='localhost'))
            channel = connection.channel()

            channel.exchange_declare(exchange='loris_data', exchange_type='direct')

            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            channel.queue_bind(
                exchange='loris_data', queue=queue_name, routing_key=data_rcv
            )

            channel.basic_consume(
                queue=queue_name, on_message_callback=callback, auto_ack=True
            )

            while channel._consumer_infos:
                channel.connection.process_data_events(time_limit=1)


asyncio.get_event_loop().run_until_complete(my_connect())