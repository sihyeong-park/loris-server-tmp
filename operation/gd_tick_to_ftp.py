"""
틱데이터를 정리하여 FTP 서버로 넣는 역할
"""

import os
import pickle
import warnings
import datetime
import pandas as pd
import zstandard as zstd

cctx = zstd.ZstdCompressor()
dctx = zstd.ZstdDecompressor()

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


class DataHandler:

    def __init__(self):
        self.stocks_path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\real_time\\kiwoom_stocks'
        self.futures_path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\real_time\\kiwoom_futures'
        self.all_path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\real_time\\kiwoom_data'

        self._get_dates()

    def _get_dates(self):
        self.stocks_dates = os.listdir(self.stocks_path)
        self.futures_dates = os.listdir(self.futures_path)

    def make_full_data(self, date):
        """
        더 빠른 렌더링을 위해서 매일 raw 데이터를 하나로 합쳐두는 작업
        주식 + 선물 / 체결 + 호가 데이터를 시간대별로 나누어 둔다
        """
        stock_path = f'{self.stocks_path}\\{date}'
        future_path = f'{self.futures_path}\\{date}'

        stock_dates = sorted(os.listdir(stock_path))
        future_dates = sorted(os.listdir(future_path))

        if len(stock_dates) != len(future_dates):
            warnings.warn('주식/선물의 데이터가 맞지 않습니다. 주의하셔야 합니다.')

        stock_times = [name.replace('.csv', '').split('_')[-1] for name in stock_dates]
        future_times = [name.replace('.csv', '').split('_')[-1] for name in stock_dates]
        supporting_times = sorted(list(set(stock_times + future_times)))

        for t in supporting_times:
            final_df = pd.DataFrame()

            for asset_type in ['stocks', 'futures']:
                print(f'{date} {t}시 데이터 - making {asset_type}')
                path = stock_path if asset_type == 'stocks' else future_path
                try:
                    trade_tmp = pd.read_csv(f'{path}\\{asset_type}_trade_{date.replace("-", "")}_{t}.csv',
                                            names=trade_cols)
                    trade_tmp['timestamp'] = trade_tmp['timestamp'].astype(str)
                    trade_tmp['timestamp'] = pd.to_datetime(trade_tmp['timestamp'], format='%Y%m%d%H%M%S.%f')
                except:
                    trade_tmp = pd.DataFrame(columns=trade_cols)

                try:
                    orderbook_tmp = pd.read_csv(f'{path}\\{asset_type}_orderbook_{date.replace("-", "")}_{t}.csv',
                                                names=orderbook_cols)
                    orderbook_tmp['timestamp'] = orderbook_tmp['timestamp'].astype(str)
                    orderbook_tmp['timestamp'] = pd.to_datetime(orderbook_tmp['timestamp'], format='%Y%m%d%H%M%S.%f')
                except:
                    orderbook_tmp = pd.DataFrame(columns=orderbook_cols)

                merged_df = pd.merge(trade_tmp, orderbook_tmp, how='outer', on=['timestamp', 'code'])
                merged_df.drop(['rotation', 'strength', 'mkt_type', 'mkt_cap'], axis=1, inplace=True)
                final_df = pd.concat([final_df, merged_df], axis=0)

            final_df = final_df.sort_values('timestamp')
            df_compressed = cctx.compress(pickle.dumps(final_df))
            with open(f'{self.all_path}\\{date}_{t}.zst', 'wb') as f:
                f.write(df_compressed)

    def get_data(self, date_filter, time_filter):
        with open(f'{self.all_path}\\{date_filter}_{time_filter}.zst', 'rb') as f:
            raw = f.read()
            data = pickle.loads(dctx.decompress(raw))
            data = data.reset_index()
            data.drop('index', axis=1, inplace=True)

            start_idx = 0
            step = 5000
            total_loops = len(data) // step

            yield total_loops

            for i in range(1, total_loops + 1):
                if i != total_loops:
                    data_rng = data.iloc[start_idx:i * step, :]
                    start_idx = i * step
                else:
                    data_rng = data.iloc[start_idx:, :]
                yield data_rng


if __name__ == '__main__':
    dh = DataHandler()

    dates = [
        '2021-02-18'
    ]

    for d in dates:
        dh.make_full_data(d)

    # while True:
    #     print(next(data_gen))