import io
import time
import gzip
import pickle
import pandas as pd
from datetime import datetime

col_map = {
    'trd_price': { "col_args": 4, 'map': lambda prc: prc },
    'trd_vol': { "col_args": 5, 'map': lambda vol: vol },
    'trd_datetime': { "col_args": (0, 30),
                      'map': lambda dt, tm: datetime.strptime(f"{dt}T{int(tm / 100000)}00", "%Y%m%dT%H%M00") },
    'isu_cd': { "col_args": 31, 'map': lambda cd: cd},
}

reduce_init = pd.DataFrame([], columns=['isu_cd', 'trd_datetime', 'open', 'high', 'low', 'close', 'vol'])

def _minute_timebar_reducer(df, iterate):
    """
    분봉으로 aggregate 하는 reducer
    :param df: accumulator
    :param iterate: curr (row)
    :return: reduce 결과
    """
    (i, row) = iterate

    trd_prc = col_map['trd_price']['map'](row[col_map['trd_price']['col_args']])
    trd_vol = col_map['trd_vol']['map'](row[col_map['trd_vol']['col_args']])
    trd_datetime = col_map['trd_datetime']['map'](*row[list(col_map['trd_datetime']['col_args'])])
    isu_cd = col_map['isu_cd']['map'](row[col_map['isu_cd']['col_args']])

    # trd_vol = row[4]
    # trd_vol = row[5]
    # trd_datetime = datetime.datetime.strptime(f"{row[0]}T{int(row[30] / 100000)}00", "%Y%m%dT%H%M00")
    # isu_cd = row[31]

    # target_row = (data['trd_datetime']==trd_datetime) & (data['isu_cd']==isu_cd)
    target_row = df[(df['trd_datetime'] == trd_datetime) & (df['isu_cd'] == isu_cd)]

    if target_row.empty:
        df = df.append({
            'isu_cd': isu_cd,
            'trd_datetime': trd_datetime,
            'open': trd_prc,
            'close': trd_prc,
            'high': trd_prc,
            'low': trd_prc,
            'vol': trd_vol
        },
            ignore_index=True)
    else:
        df.loc[target_row.index, 'close'] = trd_prc
        df.loc[target_row.index, 'high'] = max(int(target_row['high']), trd_prc)
        df.loc[target_row.index, 'low'] = min(int(target_row['low']), trd_prc)
        df.loc[target_row.index, 'vol'] += trd_vol

    return df

def _vol_1000_timebar_reducer(df, iterate):
    """
       분봉으로 aggregate 하는 reducer
    :param df: accumulator
    :param iterate: curr (row)
    :return: reduce 결과
    """

    VOL_CUT = 1000

    (i, row) = iterate

    trd_prc = col_map['trd_price']['map'](row[col_map['trd_price']['col_args']])
    trd_vol = col_map['trd_vol']['map'](row[col_map['trd_vol']['col_args']])
    trd_datetime = col_map['trd_datetime']['map'](*row[list(col_map['trd_datetime']['col_args'])])
    isu_cd = col_map['isu_cd']['map'](row[col_map['isu_cd']['col_args']])

    if df[(df['isu_cd'] == isu_cd)].empty:
        df = df.append({
            'isu_cd': isu_cd,
            'trd_datetime': trd_datetime,
            'open': trd_prc,
            'close': trd_prc,
            'high': trd_prc,
            'low': trd_prc,
            'vol': trd_vol
        },
            ignore_index=True)
    else:
        target_idx = df.index[df['isu_cd'] == isu_cd][-1]
        if int(df.iloc[target_idx]['vol']) > VOL_CUT:
            df = df.append({
                'isu_cd': isu_cd,
                'trd_datetime': trd_datetime,
                'open': trd_prc,
                'close': trd_prc,
                'high': trd_prc,
                'low': trd_prc,
                'vol': trd_vol
            },
                ignore_index=True)
        else:
            df.loc[target_idx, 'close'] = trd_prc
            df.loc[target_idx, 'high'] = max(int(df.iloc[target_idx]['high']), trd_prc)
            df.loc[target_idx, 'low'] = min(int(df.iloc[target_idx]['low']), trd_prc)
            df.loc[target_idx, 'vol'] += trd_vol

    return df


if __name__ == '__main__':
    with open('G:\\공유 드라이브\\Project_TBD\\Stock_Data\\krx\\isin.pkl', 'rb') as f:
        """
        isin_info: 종목명, 티커, ETF, 시장
        """
        isin_info = pickle.load(f)

    path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\krx\\STKTRD_20201030.csv.gz'
    file = gzip.open(path, 'rb')
    data = pd.read_csv(io.StringIO(file.read().decode()), sep=',', header=None).sort_values(col_map['trd_datetime']['col_args'][-1])

    start = time.time()
    df = reduce_init
    for (i, iterate) in enumerate(data.iterrows()):
        df = _vol_1000_timebar_reducer(df, iterate)
        if i % 10000 == 0:
            print(f'{i} / {len(df)} DONE')
    end = time.time()

    print(end - start)
    print(df)
