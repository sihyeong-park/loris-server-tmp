import os
import pickle
import pandas as pd

"""
ebest로 받은 데이터를 종목별로 구분하여 저장하는 역할
"""

files = os.listdir('./data')
codes = list(set([f.split('_')[0] for f in files]))

cnt = 0
for code in codes:
    f = [f for f in files if code == f.split('_')[0]]
    full_df = pd.DataFrame()
    for pkl in f:
        pkl_f = open(f'./data/{pkl}', 'rb')
        data = pickle.load(pkl_f)
        if data != []:
            df = pd.DataFrame(data)
            full_df = pd.concat([full_df, df], axis=0)
    if len(full_df) > 0:
        full_df.to_csv(f'./minute/{code}.csv')
    print(f'{cnt} / {len(codes)} {code}')
    cnt += 1