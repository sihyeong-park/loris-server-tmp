import os
import io
import gzip
import pickle
import pandas as pd
import zstandard as zstd

cctx = zstd.ZstdCompressor()

gd_krx_path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\krx'
files = os.listdir(gd_krx_path)

with open(f'{gd_krx_path}\\isin.pkl', 'rb') as f:
    """
    isin_info: 종목명, 티커, ETF, 시장
    """
    isin_info = pickle.load(f)

def save_as_zstd(data, filename):
    c = cctx.compress(pickle.dumps(data))
    with open(f'{gd_krx_path}\\minute\\{filename}', 'wb') as f:
        f.write(c)
    print(f'Saved {filename}')

for f in files:
    if ('.csv.gz' in f) and ('stktrd' in f.lower()):
        """
        체결 데이터 처리 방법
        """
        date = f.split('.')[0].split('_')[1]
        file = gzip.open(f'{gd_krx_path}\\{f}', 'rb')
        df = pd.read_csv(io.StringIO(file.read().decode()), sep=',', header=None)
        df = df[[0, 4, 5, 30, 31]]
        df.columns = ['date', 'current_price', 'volume', 'trade_time', 'code']
        df['trade_date'] = df['date'].astype(str) + df['trade_time'].astype(str).apply(lambda x: x.zfill(9))
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d%H%M%S%f')
        df.drop(['date', 'trade_time'], axis=1, inplace=True)

        codes = list(set(df['code']))
        for code in codes:
            kr_code = isin_info.get(code, {}).get('티커', code)
            tmp = df.loc[df['code'] == code]
            tmp.index = tmp['trade_date']
            open_prc = tmp['current_price'].resample('1T').first()
            high_prc = tmp['current_price'].resample('1T').max()
            low_prc = tmp['current_price'].resample('1T').min()
            close_prc = tmp['current_price'].resample('1T').last()
            volume = tmp['volume'].resample('1T').sum()
            min_df = pd.concat([open_prc, high_prc, low_prc, close_prc, volume], axis=1)
            min_df.columns = ['open', 'high', 'low', 'close', 'volume']
            save_as_zstd(min_df, f'{kr_code}_{code}_{date}.zst')