"""
Save Google Drive min data to DB
"""

import os
import pickle
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db import MinutePrice


def save_min_batch_1(s):
    path = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(f'{path}\\min_batch_1_done.pkl'):
        with open(f'{path}\\min_batch_1_done.pkl', 'rb') as f:
            done_pickle_list = pickle.load(f)
    else:
        done_pickle_list = []
        with open(f'{path}\\min_batch_1_done.pkl', 'wb') as f:
            pickle.dump(done_pickle_list, f)

    files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\Minute')
    cnt = 0
    for f in files:
        code = f.split('.')[0].split('_')[0]

        if f in done_pickle_list:
            print(f'({cnt}/{len(files)}) Skipped Minute Price data: {code}')
            cnt += 1
            continue

        res = s.execute(f"SELECT DISTINCT date FROM minute_price WHERE code='{code}';")
        existing_dates = []
        for dates in res:
            existing_dates.append(dates[0])

        data = pd.read_csv(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\Minute\\{f}')
        min_list = []
        for i in range(len(data)):
            d = data.iloc[i].to_dict()
            if str(d['date']) in existing_dates:
                continue
            m = MinutePrice(
                date=str(d['date']),
                code=code,
                strt_prc=int(d['open']),
                high_prc=int(d['high']),
                low_prc=int(d['low']),
                cls_prc=int(d['close']),
                trd_qty=int(d['volume'])
            )
            min_list.append(m)
        s.bulk_save_objects(min_list)
        s.commit()
        print(f'({cnt}/{len(files)}) Saved Minute Price data: {code} / Count: {len(min_list)}')
        done_pickle_list.append(f)
        with open(f'{path}\\min_batch_1_done.pkl', 'wb') as f:
            pickle.dump(done_pickle_list, f)
        cnt += 1

def save_min_batch_2(s):
    path = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(f'{path}\\min_batch_2_done.pkl'):
        with open(f'{path}\\min_batch_2_done.pkl', 'rb') as f:
            done_pickle_list = pickle.load(f)
    else:
        done_pickle_list = []
        with open(f'{path}\\min_batch_2_done.pkl', 'wb') as f:
            pickle.dump(done_pickle_list, f)

    files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\minute_8_25_10_19')
    cnt = 0
    for f in files:
        code = f.split('.')[0].split('_')[0]

        if f in done_pickle_list:
            print(f'({cnt}/{len(files)}) Skipped Minute Price data: {code}')
            cnt += 1
            continue

        res = s.execute(f"SELECT DISTINCT date FROM minute_price WHERE code='{code}';")
        existing_dates = []
        for dates in res:
            existing_dates.append(dates[0])

        data = pd.read_csv(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\minute_8_25_10_19\\{f}')
        min_list = []
        for i in range(len(data)):
            d = data.iloc[i].to_dict()
            if str(d['date']) in existing_dates:
                continue
            m = MinutePrice(
                date=str(d['date']),
                code=code,
                strt_prc=int(d['open']),
                high_prc=int(d['high']),
                low_prc=int(d['low']),
                cls_prc=int(d['close']),
                trd_qty=int(d['volume'])
            )
            min_list.append(m)
        s.bulk_save_objects(min_list)
        s.commit()
        print(f'({cnt}/{len(files)}) Saved Minute Price data: {code} / Count: {len(min_list)}')
        done_pickle_list.append(f)
        with open(f'{path}\\min_batch_2_done.pkl', 'wb') as f:
            pickle.dump(done_pickle_list, f)
        cnt += 1

def save_futures_min_batch(s):
    files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\선물1분봉')
    pass


if __name__ == '__main__':
    db = create_engine('mysql://root:qraft@10.0.1.245:3306/fnguide?charset=utf8mb4')
    session = sessionmaker(bind=db)
    s = session()

    save_min_batch_1(s)
    save_min_batch_2(s)
    save_futures_min_batch(s)