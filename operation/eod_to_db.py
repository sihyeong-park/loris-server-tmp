import os
import time
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db import MinutePrice

from dotenv import load_dotenv

load_dotenv()

api_key = {
    1: os.getenv('API_1'),
    2: os.getenv('API_2'),
    3: os.getenv('API_3')
}

def get_exchange_code_list(exchange, api_num):
    # exchange: KO, KQ
    res = requests.get(f'https://eodhistoricaldata.com/api/exchange-symbol-list/{exchange}?api_token={api_key[api_num]}&fmt=json')
    return res.json()

def get_intraday_data(ticker, exchange, api_num):
    res = requests.get(f'https://eodhistoricaldata.com/api/intraday/{ticker}.{exchange}?fmt=json&interval=5m&api_token={api_key[api_num]}')
    return res.json()


if __name__ == '__main__':
    db = create_engine('mysql://root:qraft@10.0.1.245:3306/fnguide?charset=utf8mb4')
    session = sessionmaker(bind=db)
    s = session()

    ko = get_exchange_code_list('KO', 1)
    kq = get_exchange_code_list('KQ', 1)

    ko_codes = [d['Code'] for d in ko]
    kq_codes = [d['Code'] for d in kq]

    api_num = 1

    c_cnt = 0
    for code_list in [ko_codes, kq_codes]:
        for code in code_list:
            res = s.execute(f"SELECT DISTINCT date FROM minute_price WHERE code='{code}';")
            existing_dates = []
            for dates in res:
                existing_dates.append(dates[0])

            exchange = 'KO' if code in ko_codes else 'KQ'

            data = get_intraday_data(code, exchange, api_num)

            cnt = 0
            for d in data:
                if datetime.datetime.fromtimestamp(d['timestamp']).strftime('%Y%m%d%H%M%S') in existing_dates:
                    continue
                min_prc_list = []
                if not ((d['open'] is None) and (d['high'] is None) and (d['low'] is None) and (d['close'] is None) and (d['volume'] is None)):
                    min_prc = MinutePrice(**{
                        'date': datetime.datetime.fromtimestamp(d['timestamp']).strftime('%Y%m%d%H%M%S'),
                        'code': code,
                        'strt_prc': int(d['open']) if d['open'] is not None else d['open'],
                        'high_prc': int(d['high']) if d['high'] is not None else d['high'],
                        'low_prc': int(d['low']) if d['low'] is not None else d['low'],
                        'cls_prc': int(d['close']) if d['close'] is not None else d['close'],
                        'trd_qty': int(d['volume']) if d['volume'] is not None else d['volume']
                    })
                    min_prc_list.append(min_prc)
                    cnt += 1
            s.bulk_save_objects(min_prc_list)
            s.commit()
            print(f'{c_cnt} {code} saved {cnt} data points to db')
            c_cnt += 1


            api_num += 1
            if api_num > 3:
                api_num = 1
            time.sleep(0.1)