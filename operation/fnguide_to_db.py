"""
매일 업데이트되는 FnGuide 데이터를 DB에 넣는 역할
"""

import os
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db import (
    Info,
    Price,
    Index,
    MarketCapital,
    BuySell,
    Factor,
    ETF,
    Future,
)

"""
Google Drive에 저장되어 있는 FnGuide 데이터를 항목별로 확인하여 DB에 업데이트되어 있다면 넘기고,
만약 DB에 정보가 없다면 업데이트하는 방식으로 진행

기존에 데이터는 csv로 만들어 두었고, 선물 데이터 같은 경우 최근에 수집하기 시작하여 json 파일 형식이다.
파일 형식에 따라 데이터를 불러오는 방식이 살짝 다를 수 있다.
"""

def save_info_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM info;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    info_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\info')
    cnt = 0
    for f in info_files:
        if '._' in f:
            continue
        date = f.split('_')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\info\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        info_list = []
        for d in data_list:
            p = Info(
                date=date,
                code=d[0],
                name=d[1],
                stk_kind=d[2],
                mkt_gb=d[3],
                mkt_cap=int(d[4]),
                mkt_cap_size=d[5],
                frg_hlg=float(d[6]),
                mgt_gb=d[7]
            )
            info_list.append(p)
        s.bulk_save_objects(info_list)
        s.commit()
        print(f'({cnt}/{len(info_files)}) Saved Info data: {date}')
        cnt += 1

def save_ohlcv_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM price;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    ohlcv_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\ohlcv')
    cnt = 0
    for f in ohlcv_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\ohlcv\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        price_list = []
        for d in data_list:
            p = Price(
                date=date,
                code=d[0],
                name=d[1],
                strt_prc=int(d[2]),
                high_prc=int(d[3]),
                low_prc=int(d[4]),
                cls_prc=int(d[5]),
                adj_prc=int(d[6]),
                trd_qty=float(d[7]),
                trd_amt=float(d[8]),
                shtsale_trd_qty=float(d[9])
            )
            price_list.append(p)
        s.bulk_save_objects(price_list)
        s.commit()
        print(f'({cnt}/{len(ohlcv_files)}) Saved Price data: {date}')
        cnt += 1

def save_index_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM index_data;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    index_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\index')
    cnt = 0
    for f in index_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\index\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        index_list = []
        for d in data_list:
            idx = Index(
                date=date,
                code=d[0],
                name=d[1],
                strt_prc=float(d[2]),
                high_prc=float(d[3]),
                low_prc=float(d[4]),
                cls_prc=float(d[5]),
                trd_qty=float(d[6]),
                trd_amt=float(d[7])
            )
            index_list.append(idx)
        s.bulk_save_objects(index_list)
        s.commit()
        print(f'({cnt}/{len(index_files)}) Saved Index data: {date}')
        cnt += 1

def save_market_capital_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM market_capital;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    market_capital_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\mkt_cap')
    cnt = 0
    for f in market_capital_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\mkt_cap\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        mkt_cap_list = []
        for d in data_list:
            mc = MarketCapital(
                date=date,
                code=d[0],
                name=d[1],
                comm_stk_qty=int(d[2]),
                pref_stk_qty=int(d[3])
            )
            mkt_cap_list.append(mc)
        s.bulk_save_objects(mkt_cap_list)
        s.commit()
        print(f'({cnt}/{len(market_capital_files)}) Saved MarketCapital data: {date}')
        cnt += 1

def save_buy_sell_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM buy_sell;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    buy_sell_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\buysell')
    cnt = 0
    for f in buy_sell_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\buysell\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        buy_sell_list = []
        for d in data_list:
            bs = BuySell(
                date=date,
                code=d[0],
                name=d[1],
                forgn_b=float(d[2]),
                forgn_s=float(d[3]),
                forgn_n=float(d[4]),
                private_b=float(d[5]),
                private_s=float(d[6]),
                private_n=float(d[7]),
                inst_sum_b=float(d[8]),
                inst_sum_s=float(d[9]),
                inst_sum_n=float(d[10]),
                trust_b=float(d[11]),
                trust_s=float(d[12]),
                trust_n=float(d[13]),
                pension_b=float(d[14]),
                pension_s=float(d[15]),
                pension_n=float(d[16]),
                etc_inst_b=float(d[17]),
                etc_inst_s=float(d[18]),
                etc_inst_n=float(d[19])
            )
            buy_sell_list.append(bs)
        s.bulk_save_objects(buy_sell_list)
        s.commit()
        print(f'({cnt}/{len(buy_sell_files)}) Saved BuySell data: {date}')
        cnt += 1

def save_factor_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM factor;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    factor_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\factor')
    cnt = 0
    for f in factor_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\factor\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        factor_list = []
        for d in data_list:
            ft = Factor(
                date=date,
                code=d[0],
                name=d[1],
                per=float(d[2]) if d[2] != '' else None,
                pbr=float(d[3]) if d[3] != '' else None,
                pcr=float(d[4]) if d[4] != '' else None,
                psr=float(d[5]) if d[5] != '' else None,
                divid_yield=float(d[6]) if d[6] != '' else None,
            )
            factor_list.append(ft)
        s.bulk_save_objects(factor_list)
        s.commit()
        print(f'({cnt}/{len(factor_files)}) Saved Factor data: {date}')
        cnt += 1

def save_etf_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM etf;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    etf_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\etf')
    cnt = 0
    for f in etf_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\etf\\{f}', 'r', encoding='utf8')
        data = file.read()
        data = data.replace(',', '')
        data_list = data.split(date)
        data_list = [[d.strip() for d in data.split('|')[1:]] for data in data_list[1:]]
        etf_list = []
        for d in data_list:
            etf = ETF(
                date=date,
                code=d[0],
                name=d[1],
                cls_prc=int(d[2]),
                trd_qty=int(d[3]),
                trd_amt=int(d[4]),
                etf_nav=float(d[5]),
                spread=float(d[6])
            )
            etf_list.append(etf)
        s.bulk_save_objects(etf_list)
        s.commit()
        print(f'({cnt}/{len(etf_files)}) Saved ETF data: {date}')
        cnt += 1

def save_future_to_db(s):
    res = s.execute('SELECT DISTINCT date FROM future;')
    existing_dates = []
    for dates in res:
        existing_dates.append(dates[0])
    future_files = os.listdir('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\futures')
    cnt = 0
    for f in future_files:
        if '._' in f:
            continue
        date = f.split('.')[0]
        if date in existing_dates:
            cnt += 1
            continue
        file = open(f'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\futures\\{f}', 'r', encoding='utf8')
        data_list = json.load(file)
        future_list = []
        for d in data_list:
            f = Future(
                date=date,
                name=d['ITEMNM'].strip(),
                cls_prc=float(d['CLS_PRC'].replace(',', '').strip()),
                theo_prc_account=float(d['THEO_PRC_ACCOUNT'].replace(',', '').strip()),
                basis_mkt=float(d['BASIS_MKT'].replace(',', '').strip()),
                strd_index=int(d['STRD_INDEX'].replace(',', '').strip()),
                trd_qty=int(d['TRD_QTY'].replace(',', '').strip()),
                trd_amt=int(d['TRD_AMT'].replace(',', '').strip()),
                remain_day_cnt=int(d['REMAIN_DAY_CNT'].replace(',', '').strip()),
                unsetl_qty=int(d['UNSETL_QTY'].replace(',', '').strip())
            )
            future_list.append(f)
        s.bulk_save_objects(future_list)
        s.commit()
        print(f'({cnt}/{len(future_files)}) Saved Future data: {date}')
        cnt += 1


if __name__ == '__main__':
    db = create_engine('mysql://root:qraft@10.0.1.245:3306/fnguide?charset=utf8mb4')
    session = sessionmaker(bind=db)
    s = session()

    save_info_to_db(s)
    save_ohlcv_to_db(s)
    save_index_to_db(s)
    save_market_capital_to_db(s)
    save_buy_sell_to_db(s)
    save_factor_to_db(s)
    save_etf_to_db(s)
    save_future_to_db(s)