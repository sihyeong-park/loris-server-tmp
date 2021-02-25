"""
일봉 데이터를 정리하여 FTP 서버로 넣는 역할
"""

import pickle
import datetime
import pandas as pd
import zstandard as zstd
from sqlalchemy import create_engine

def get_info_data(db):
    df = pd.read_sql('SELECT * FROM info;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_price_data(db):
    """
    잠정적으로 가격 데이터 20년으로 수정
    """
    df = pd.read_sql("SELECT * FROM price WHERE date >= '20000101';", db)
    df.drop('id', axis=1, inplace=True)
    return df

# def get_minute_price_data(db):
#     df = pd.read_sql('SELECT * FROM minute_price;', db)
#     df.drop('id', axis=1, inplace=True)
#     return df

def get_index_data(db):
    df = pd.read_sql('SELECT * FROM index_data;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_market_capital_data(db):
    df = pd.read_sql('SELECT * FROM market_capital;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_buy_sell_data(db):
    df = pd.read_sql('SELECT * FROM buy_sell;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_factor_data(db):
    df = pd.read_sql('SELECT * FROM factor;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_etf_data(db):
    df = pd.read_sql('SELECT * FROM etf;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def get_futures_data(db):
    df = pd.read_sql('SELECT * FROM future;', db)
    df.drop('id', axis=1, inplace=True)
    return df

def split_data_by_year():
    """
    csv파일을 생성한 다음 함수 호출할 것
    """
    loop_data = ['price', 'market_capital', 'buy_sell', 'factor', 'etf']

    for d in loop_data:
        data = pd.read_csv(f'D:\\loris\\fnguide\\{d}.csv', index_col=None)
        dates = data['date']
        data['year'] = dates.apply(lambda x: str(x)[:4])
        uni_dates = sorted(list(set(data['year'])))
        for date in uni_dates:
            tmp = data.loc[data['year'] == date]
            tmp.to_csv(f'D:\\loris\\fnguide\\{d}_{date}.csv')
            print(f'D:\\loris\\fnguide\\{d}_{date}.csv DONE')


if __name__ == '__main__':
    cctx = zstd.ZstdCompressor()
    db = create_engine('mysql://root:qraft@10.0.1.245:3306/fnguide?charset=utf8mb4')

    # info = get_info_data(db)
    # info.to_csv('D:\\loris\\fnguide\\info.csv')
    # c = cctx.compress(pickle.dumps(info))
    # with open('D:\\loris\\fnguide\\info.zst', 'wb') as f:
    #     f.write(c)
    # print('info DONE')
    #
    # price = get_price_data(db)
    # price.to_csv('D:\\loris\\fnguide\\price.csv')
    # c = cctx.compress(pickle.dumps(price))
    # with open('D:\\loris\\fnguide\\price.zst', 'wb') as f:
    #     f.write(c)
    # print('price DONE')
    #
    # # minute_price = get_minute_price_data(db)
    # # c = cctx.compress(pickle.dumps(minute_price))
    # # with open('D:\\loris\\fnguide\\minute_price.zst', 'wb') as f:
    # #     f.write(c)
    # # print('minute_price DONE')
    #
    # index = get_index_data(db)
    # index.to_csv('D:\\loris\\fnguide\\index_data.csv')
    # c = cctx.compress(pickle.dumps(index))
    # with open('D:\\loris\\fnguide\\index_data.zst', 'wb') as f:
    #     f.write(c)
    # print('index DONE')
    #
    # mkt_cap = get_market_capital_data(db)
    # mkt_cap.to_csv('D:\\loris\\fnguide\\market_capital.csv')
    # c = cctx.compress(pickle.dumps(mkt_cap))
    # with open('D:\\loris\\fnguide\\market_capital.zst', 'wb') as f:
    #     f.write(c)
    # print('mkt_cap DONE')
    #
    # buy_sell = get_buy_sell_data(db)
    # buy_sell.to_csv('D:\\loris\\fnguide\\buy_sell.csv')
    # c = cctx.compress(pickle.dumps(buy_sell))
    # with open('D:\\loris\\fnguide\\buy_sell.zst', 'wb') as f:
    #     f.write(c)
    # print('buy_sell DONE')
    #
    # factor = get_factor_data(db)
    # factor.to_csv('D:\\loris\\fnguide\\factor.csv')
    # c = cctx.compress(pickle.dumps(factor))
    # with open('D:\\loris\\fnguide\\factor.zst', 'wb') as f:
    #     f.write(c)
    # print('factor DONE')
    #
    # etf = get_etf_data(db)
    # etf.to_csv('D:\\loris\\fnguide\\etf.csv')
    # c = cctx.compress(pickle.dumps(etf))
    # with open('D:\\loris\\fnguide\\etf.zst', 'wb') as f:
    #     f.write(c)
    # print('etf DONE')
    #
    # futures = get_futures_data(db)
    # futures.to_csv('D:\\loris\\fnguide\\futures.csv')
    # c = cctx.compress(pickle.dumps(futures))
    # with open('D:\\loris\\fnguide\\future.zst', 'wb') as f:
    #     f.write(c)
    # print('futures DONE')

    split_data_by_year()

    # with open('D:\\loris\\fnguide\\updated.pkl', 'wb') as f:
    #     pickle.dump({'updated': datetime.datetime.now().strftime('%Y%m%d')}, f)
