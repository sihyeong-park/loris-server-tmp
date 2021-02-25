"""
FastAPI 소켓 서버로부터 클라이언트의 요구 사항을 받아서 RabbitMQ로 데이터를 뿌려주는 역할 수행
"""
import os
import sys
import zmq
import json
import uuid
import pika
import pickle
import datetime
import pandas as pd
import zstandard as zstd
from multiprocessing import Process, Queue

trade_cols = [
    'code',
    'trade_date',
    'timestamp',
    'current_price',
    'open_price',
    'high',
    'low',
    'volume',
    'cum_volume',
    'trade_sell_hoga1',
    'trade_buy_hoga1',
    'rotation',
    'strength',
    'mkt_type',
    'mkt_cap'
]

orderbook_cols = [
    'code',
    'hoga_date',
    'timestamp',
    'sell_hoga1',
    'sell_hoga2',
    'sell_hoga3',
    'sell_hoga4',
    'sell_hoga5',
    'sell_hoga6',
    'sell_hoga7',
    'sell_hoga8',
    'sell_hoga9',
    'sell_hoga10',
    'buy_hoga1',
    'buy_hoga2',
    'buy_hoga3',
    'buy_hoga4',
    'buy_hoga5',
    'buy_hoga6',
    'buy_hoga7',
    'buy_hoga8',
    'buy_hoga9',
    'buy_hoga10',
    'sell_hoga1_stack',
    'sell_hoga2_stack',
    'sell_hoga3_stack',
    'sell_hoga4_stack',
    'sell_hoga5_stack',
    'sell_hoga6_stack',
    'sell_hoga7_stack',
    'sell_hoga8_stack',
    'sell_hoga9_stack',
    'sell_hoga10_stack',
    'buy_hoga1_stack',
    'buy_hoga2_stack',
    'buy_hoga3_stack',
    'buy_hoga4_stack',
    'buy_hoga5_stack',
    'buy_hoga6_stack',
    'buy_hoga7_stack',
    'buy_hoga8_stack',
    'buy_hoga9_stack',
    'buy_hoga10_stack',
    'total_buy_hoga_stack',
    'total_sell_hoga_stack',
    'net_buy_hoga_stack',
    'net_sell_hoga_stack',
    'ratio_buy_hoga_stack',
    'ratio_sell_hoga_stack'
]


# operational function
def process_zst_to_csv():
    """
    데이터를 스트리밍할때 csv 파일을 chunking하는게 더 memory efficient하기 때문에 미리 csv로 변경하여두기

    csv로 변형된 파일들은 chunk로 불러서 스트리밍해줄 수 있다.
    """
    dctx = zstd.ZstdDecompressor()

    gd_path = '/Volumes/GoogleDrive/공유 드라이브/Project_TBD/Stock_Data/real_time/kiwoom_data'
    gd_files = sorted(os.listdir(gd_path))

    for file in gd_files:
        csv_filename = file.split('.')[0] + '.csv'
        if not os.path.exists(csv_filename):
            f = open(f'{gd_path}/{file}', 'rb')
            data = pickle.loads(dctx.decompress(f.read()))
            data.to_csv(f'{gd_path}/{csv_filename}')
            print(f'{csv_filename} DONE')


def stream_data(id, queue, res_queue):
    """
    소켓 서버로부터 request 딕셔너리를 받아서 이에 맞는 데이터를 클라이언트로 스트리밍해주는 기능
    zst파일로 받아가는 옵션과는 별개로 csv 파일을 chunking하여 스트리밍해주도록 하기

    request는:
    - date_from
    - date_to
    - time_from
    - time_to
    - asset_type
    - data_type
    - frequency: T, M, D

    등을 인자값으로 호출할 수 있다.
    """

    gd_path = '/Volumes/GoogleDrive/공유 드라이브/Project_TBD/Stock_Data/real_time/kiwoom_data'
    fn_path = '/Volumes/GoogleDrive/공유 드라이브/Project_TBD/Stock_Data/fnguide'

    while True:
        req = queue.get()

        routing_key = str(uuid.uuid1())
        res_queue.put(routing_key)

        frequency = req.get('frequency', 'T')
        date_from = req.get('date_from')
        date_to = req.get('date_to')
        time_from = req.get('time_from', '08')
        time_to = req.get('time_to', '16')
        asset_type = req.get('asset_type', 'stocks')
        if frequency == 'T':
            data_type = req.get('data_type', 'trade')
        else:
            data_type = req.get('data_type', 'adj_prc')
            if data_type not in ['cls_prc', 'adj_prc']:
                # daily는 default로 수정주가를 사용하고, minute는 종가를 사용한다.
                data_type = 'cls_prc' if frequency == 'M' else 'adj_prc'
        monitor_stocks = req.get('monitor_stocks', [])

        if date_to is None:
            if frequency == 'T':
                date_to = date_from
            else:
                date_to = datetime.datetime.now().strftime('%Y-%m-%d')

        if frequency == 'T':
            files = [f for f in os.listdir(gd_path) if '.csv' in f]
            dates = list(set([f.split('_')[0] for f in files]))
            times = list(set([f.replace('.csv', '').split('_')[1] for f in files]))

            stream_dates = sorted([d for d in dates if d >= date_from and d <= date_to])
            stream_times = sorted([t for t in times if t >= time_from and t <= time_to])
            stream_files = [f'{d}_{t}.csv' for t in stream_times for d in stream_dates]
        else:
            files = [f for f in os.listdir(fn_path) if '.csv' in f]
            print(files)

            for chunk in pd.read_csv(f'{fn_path}/price.csv', chunksize=100000, index_col=None):
                print(chunk)
                data_dates = list(set([str(d)[:4] for d in chunk['date']]))
                print(data_dates)
                break

        # if asset_type == 'stocks':
        #     asset_type_mask = lambda x: x.loc[x['code'].str.len() == 6]
        # elif asset_type == 'futures':
        #     asset_type_mask = lambda x: x.loc[x['code'].str.len() == 8]
        # else:
        #     asset_type_mask = lambda x: x.loc[(x['code'].str.len() == 6) | (x['code'].str.len() == 8)]

        # if data_type == 'trade':
        #     data_type_mask = lambda x: x.loc[x['trade_date'].notnull()][x.columns.intersection(trade_cols)]
        # elif data_type == 'orderbook':
        #     data_type_mask = lambda x: x.loc[x['hoga_date'].notnull()][x.columns.intersection(orderbook_cols)]
        # else:
        #     data_type_mask = lambda x: x[x.columns.intersection(trade_cols + orderbook_cols)]

        # monitor_stocks_mask = lambda x: x.loc[x['code'].isin(monitor_stocks)]

        # connection = pika.BlockingConnection(
        #     pika.ConnectionParameters(host='localhost'))
        # channel = connection.channel()

        # channel.exchange_declare(exchange='loris_data', exchange_type='direct')

        # for f in stream_files:
        #     for chunk in pd.read_csv(f'{gd_path}/{f}', chunksize=100000, index_col=None):
        #         chunk.sort_values('timestamp', inplace=True)

        #         df = asset_type_mask(chunk)
        #         df = data_type_mask(df)
        #         if len(monitor_stocks) > 0:
        #             df = monitor_stocks_mask(df)

        #         for i in range(len(df)):
        #             row = df.iloc[i].T.to_dict()
        #             channel.basic_publish(
        #                 exchange='loris_data', routing_key=routing_key, body=json.dumps(row)
        #             )


if __name__ == '__main__':
    streamer_cnt = 10
    q = [Queue() for i in range(streamer_cnt)]
    res_q = [Queue() for i in range(streamer_cnt)]
    s = [Process(target=stream_data, args=(i, q[i], res_q[i])) for i in range(streamer_cnt)]
    _ = [p.start() for p in s]

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f'tcp://*:8888')
    print(f'Started ZeroMQ service on tcp://*:8888')

    worker_num = 0

    while True:
        message = socket.recv()
        req = json.loads(message.decode())

        q[worker_num].put(req)
        routing_key = res_q[worker_num].get()

        socket.send(routing_key.encode())

        worker_num += 1
        if worker_num >= streamer_cnt:
            worker_num = 0

