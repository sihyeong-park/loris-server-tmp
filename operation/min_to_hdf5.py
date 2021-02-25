import os
import json
import h5py
import pickle
import pandas as pd
import zstandard as zstd

gd_path = 'G:\\공유 드라이브\\Project_TBD\\Stock_Data\\krx\\compressed'

def _get_all_codes(market_type):
    """
    각 폴더별 파일명으로 어떤 종목이 있는지 파악하는 함수
    """
    path = f'{gd_path}\\{market_type}'
    date_paths = os.listdir(path)
    all_codes = []
    for date in date_paths:
        files = os.listdir(f'{path}\\{date}')
        codes = [f.split('_')[0] for f in files]
        all_codes = list(set(all_codes).union(set(codes)))
    return sorted(all_codes)

# array index definition
def get_minute_table():
    return {
        date: i for i, date in
        enumerate([d.strftime('%H%M%S') for d in pd.date_range('08:30', '16:30', freq='T')])
    }

def get_code_table():
    codes = _get_all_codes('kospi')
    kosdaq_codes = _get_all_codes('kosdaq')
    codes.extend(kosdaq_codes)
    return {c: i for i, c in enumerate(codes)}

def get_date_table():
    all_dates = []
    for market_type in ['kospi', 'kosdaq']:
        path = f'{gd_path}\\{market_type}'
        date_paths = os.listdir(path)
        all_dates = list(set(all_dates).union(set(date_paths)))
    all_dates = sorted(all_dates)
    return {d: i for i, d in enumerate(all_dates)}

def get_field_table():
    return {
        'open': 0,
        'high': 1,
        'low': 2,
        'close': 3,
        'volume': 4
    }


def data_generator(market_type=None):
    dctx = zstd.ZstdDecompressor()

    market_list = ['kospi', 'kosdaq'] if market_type is None else [market_type]

    for market in market_list:
        path = f'{gd_path}\\{market}'
        date_paths = sorted(os.listdir(path), reverse=True)
        for date in date_paths:
            date_path = f'{path}\\{date}'
            files = os.listdir(date_path)
            for f in files:
                with open(f'{date_path}\\{f}', 'rb') as file:
                    dt = file.read()
                yield date, f.split('_')[0], pickle.loads(dctx.decompress(dt))


class Hdf5Handler:

    def __init__(self, filename, dataset, date_table, min_table, code_table, field_table, mode='w'):
        self.shape = (len(date_table), len(min_table), len(code_table), len(field_table))

        self.date_table = date_table
        self.min_table = min_table
        self.code_table = code_table
        self.field_table = field_table

        self.f = h5py.File(filename, mode)

        if dataset not in list(self.f.keys()):
            self.dset = self.f.create_dataset(dataset, self.shape, dtype='f')
            self._save_attrs()
        else:
            self.dset = self.f[dataset]

        self.done_dates = {}
        if not os.path.exists('done_dates.pkl'):
            with open('done_dates.pkl', 'wb') as file:
                pickle.dump(self.done_dates, file)
        else:
            with open('done_dates.pkl', 'rb') as file:
                self.done_dates = pickle.load(file)

    def _save_attrs(self):
        self.dset.attrs['date_table'] = json.dumps(self.date_table)
        self.dset.attrs['min_table'] = json.dumps(self.min_table)
        self.dset.attrs['code_table'] = json.dumps(self.code_table)
        self.dset.attrs['field_table'] = json.dumps(self.field_table)

    def _record_done(self, date, code):
        if date not in list(self.done_dates.keys()):
            self.done_dates[date] = []
        self.done_dates[date].append(code)
        with open('done_dates.pkl', 'wb') as file:
            pickle.dump(self.done_dates, file)
        print(f'{date} {code} {len(self.done_dates[date])} DONE')

    def insert(self, date, code, data):
        min_index = pd.to_datetime([date + m for m in min_table.keys()])
        data = data.loc[data.index.isin(min_index)]
        data = pd.concat([pd.DataFrame(index=min_index), data], axis=1)
        # data.fillna(0.0, inplace=True)

        date_idx = self.date_table[date]
        code_idx = self.code_table[code]

        self.dset[date_idx, :, code_idx, :] = data.values
        self._record_done(date, code)


class Hdf5Client:

    def __init__(self):
        self.f = h5py.File('test.hdf5', 'r')
        self.dset = self.f['test_dataset']

        self.date_table = json.loads(self.dset.attrs['date_table'])
        self.min_table = json.loads(self.dset.attrs['min_table'])
        self.code_table = json.loads(self.dset.attrs['code_table'])
        self.field_table = json.loads(self.dset.attrs['field_table'])

    def get_minute_data(self, date_from: str, date_to: str = None, time_from: str = '0830', time_to: str = '1600', code: str or list = [], fields: list = []):
        # date idx
        date_from = date_from.replace('-', '')
        date_to = date_to.replace('-', '') if date_to is not None else date_to

        if date_to is None:
            date_to = date_from

        date_from_idx = self.date_table.get(date_from, 0)
        date_to_idx = self.date_table.get(date_to, 0)

        if date_from_idx == date_to_idx:
            date_to_idx = date_from_idx + 1

        # time idx
        time_from = time_from.ljust(6, '0')
        time_to = time_to.ljust(6, '0')
        time_idx = [t for t in self.min_table.keys() if (t >= time_from) and (t <= time_to)]
        time_idx = [self.min_table[t] for t in time_idx]

        # code idx
        if code == '' or code == []:
            code = list(self.code_table.keys())

        if type(code) == str:
            code = [code]

        code_idx = [self.code_table[c] for c in code]

        # field idx
        if fields == []:
            fields = list(self.field_table.keys())

        field_idx = [self.field_table[f] for f in fields]

        return self.dset[date_from_idx:date_to_idx, time_idx, code_idx, field_idx]




if __name__ == '__main__':
    date_table = get_date_table()
    min_table = get_minute_table()
    code_table = get_code_table()
    field_table = get_field_table()

    # h = Hdf5Handler('test.hdf5', 'test_dataset', date_table, min_table, code_table, field_table)

    # dg = data_generator('kospi')
    #
    # while True:
    #     data_tuple = next(dg)
    #     h.insert(data_tuple[0], data_tuple[1], data_tuple[2])


    d = Hdf5Client()
    data = d.get_minute_data('20201020', time_from='0900', time_to='1530', code='005930')

    print('done')