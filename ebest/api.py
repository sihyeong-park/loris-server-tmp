import os
import time
import pickle
import pythoncom
import win32com.client
from dotenv import load_dotenv

from ebest.handler import XAQueryLoginHandler, XAQueryTransactionHandler
from ebest.realtime import EbestRealTimeAPI
"""
https://sourceforge.net/projects/pywin32/files/?source=navbar
"""

load_dotenv()

USER = os.getenv('USER')
PASS = os.getenv('PASS')
CERT_PASS = os.getenv('CERT_PASS')


class EbestAPI:

    def __init__(self, username=None, password=None, cert_password=None, server_type='demo', real_time=False):
        self.login_session = win32com.client.DispatchWithEvents('XA_Session.XASession', XAQueryLoginHandler)
        self.tr_session = win32com.client.DispatchWithEvents('XA_DataSet.XAQuery', XAQueryTransactionHandler)

        self.username = username if username is not None else USER
        self.password = password if password is not None else PASS
        self.cert_password = cert_password if cert_password is not None else CERT_PASS
        self.accounts = []

        self.stocks_list = []
        self.kospi_list = []
        self.kosdaq_list = []
        self.futures_list = []
        self.futures_basecode_list = []

        # 로그인
        # 종목 리스트 등록해두기
        self.login(server_type)
        self.get_stocks_code_list()
        self.get_futures_code_list()

        if real_time:
            self.real_time_api = EbestRealTimeAPI(self.stocks_list, self.kospi_list, self.kosdaq_list, self.futures_list)

    def login(self, server_type: str):
        # server_type: hts, demo (실계좌, 모의계좌)
        self.login_session.ConnectServer(f'{server_type}.ebestsec.co.kr', 20001)
        self.login_session.Login(self.username, self.password, self.cert_password, 0, 0)

        while XAQueryLoginHandler.login_state == 0:
            pythoncom.PumpWaitingMessages()

        num_account = self.login_session.GetAccountListCount()
        for i in range(num_account):
            account = self.login_session.GetAccountList(i)
            self.accounts.append(account)

    def get_stocks_code_list(self):
        self.tr_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t8436.res'

        for market_type in [0, 1]:
            # 0: 전체, 1: 코스피, 2: 코스닥
            XAQueryTransactionHandler.tr_state = 0

            self.tr_session.SetFieldData('t8436InBlock', 'gubun', 0, str(market_type))
            self.tr_session.Request(False)

            while XAQueryTransactionHandler.tr_state == 0:
                pythoncom.PumpWaitingMessages()

            occurs_count = self.tr_session.GetBlockCount('t8436OutBlock')

            for i in range(occurs_count):
                shcode = self.tr_session.GetFieldData('t8436OutBlock', 'shcode', i)

                if market_type == 0:
                    self.stocks_list.append(shcode)
                elif market_type == 1:
                    self.kospi_list.append(shcode)

        self.kosdaq_list = list(set(self.stocks_list) - set(self.kosdaq_list))

        return list(set(self.stocks_list))

    def get_futures_code_list(self):
        self.tr_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t8401.res'

        XAQueryTransactionHandler.tr_state = 0

        self.tr_session.SetFieldData('t8401InBlock', 'dummy', 0, '')
        self.tr_session.Request(False)

        while XAQueryTransactionHandler.tr_state == 0:
            pythoncom.PumpWaitingMessages()

        occurs_count = self.tr_session.GetBlockCount('t8401OutBlock')

        stock_futures_basecode_dict = {}
        stock_futures_basecode_pickle = {}

        for i in range(occurs_count):
            shcode = self.tr_session.GetFieldData('t8401OutBlock', 'shcode', i)
            basecode = self.tr_session.GetFieldData('t8401OutBlock', 'basecode', i)
            self.futures_list.append(shcode)

            stock_futures_basecode_dict[shcode] = basecode[1:]  # basecode 앞의 "A" 제거
            stock_futures_basecode_pickle[shcode[1:3]] = basecode[1:]

        ## stock_futures_basecode_pickle(as Dict) 업데이트
        with open('stock_futures_basecode_idx.pickle', 'wb') as f:
            pickle.dump(stock_futures_basecode_pickle, f)

        ### 최근월물/ 차근월물만 뽑아내는 종목리스트로 바꾸기 (추후 보완) ###
        fu_code_ls = list(set(map(lambda x: x[1:3], self.futures_list)))

        total_fu_code = []
        for fu_code in fu_code_ls:
            fut_tmp = []
            for i in range(len(self.futures_list)):
                fu_code_i = self.futures_list[i][1:3]
                if fu_code_i == fu_code:
                    if self.futures_list[i][0] == "1":  # 선물이면 종목코드의 첫자리가 1 이여야 함.
                        fut_tmp.append(self.futures_list[i])
                else:
                    pass
            total_fu_code.append(fut_tmp)

        total_fu_code = list(map(lambda x: x[:1], total_fu_code))  # 더 원월물까지 포함하고 싶으면 1을 바꾸면됨

        flatten_fu_code = []
        for fu_code in total_fu_code:
            flatten_fu_code = flatten_fu_code + fu_code

        self.futures_list = flatten_fu_code  # 주식선물(근월물)만으로 filter된 stock_futures_code_list 생성

        basecode_by_dict = []
        for fu_code in self.futures_list:
            basecode_by_dict.append(stock_futures_basecode_dict[fu_code])

        self.futures_basecode_list = list(set(basecode_by_dict))  # 주식선물에 대한 base code list

        return self.futures_list, self.futures_basecode_list

    def get_current_stock_info(self):
        self.tr_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t1102.res'
        pass

    def get_daily_price_data(self, code, date_from, date_to, save=False):
        self.tr_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t8413.res'

        XAQueryTransactionHandler.tr_state = 0

        self.tr_session.SetFieldData('t8413InBlock', 'shcode', 0, code)
        self.tr_session.SetFieldData('t8413InBlock', 'gubun', 0, '2')
        self.tr_session.SetFieldData('t8413InBlock', 'sdate', 0, date_from)
        self.tr_session.SetFieldData('t8413InBlock', 'edate', 0, date_to)
        self.tr_session.SetFieldData('t8413InBlock', 'comp_yn', 0, 'N')

        self.tr_session.Request(0)

        while XAQueryTransactionHandler.tr_state == 0:
            pythoncom.PumpWaitingMessages()

        occurs_count = self.tr_session.GetBlockCount('t8413OutBlock1')

        data = []

        for i in range(occurs_count):
            date = self.tr_session.GetFieldData('t8413OutBlock1', 'date', i)
            t = self.tr_session.GetFieldData('t8413OutBlock1', 'time', i)
            open_p = self.tr_session.GetFieldData('t8413OutBlock1', 'open', i)
            high = self.tr_session.GetFieldData('t8413OutBlock1', 'high', i)
            low = self.tr_session.GetFieldData('t8413OutBlock1', 'low', i)
            close = self.tr_session.GetFieldData('t8413OutBlock1', 'close', i)
            vol = self.tr_session.GetFieldData('t8413OutBlock1', 'jdiff_vol', i)
            jongchk = self.tr_session.GetFieldData('t8413OutBlock1', 'jongchk', i)  # 수정구분
            rate = self.tr_session.GetFieldData('t8413OutBlock1', 'rate', i)  # 수정비율
            sign = self.tr_session.GetFieldData('t8413OutBlock1', 'sign', i)  # 종가등락구분 (1: 상한, 2: 상승, 3: 보합)

            data.append({
                'date': date,
                'code': code,
                'time': t,
                'strt_prc': int(open_p),
                'high_prc': int(high),
                'low_prc': int(low),
                'cls_prc': int(close),
                'trd_qty': int(vol),
                'jongchk': int(jongchk),
                'rate': float(rate),
                'sign': sign
            })

        if save:
            path = os.path.dirname(os.path.abspath(__file__))
            with open(f'{path}\\data\\{code}_{date}_daily.pkl', 'wb') as f:
                pickle.dump(data, f)
            print(f'{code} {date} DAILY SAVED')
        time.sleep(3)
        return data

    def get_minute_price_data(self, code, date, save=False):
        self.tr_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t8412.res'

        XAQueryTransactionHandler.tr_state = 0

        self.tr_session.SetFieldData('t8412InBlock', 'shcode', 0, code)
        self.tr_session.SetFieldData('t8412InBlock', 'ncnt', 0, 1)
        self.tr_session.SetFieldData('t8412InBlock', 'sdate', 0, date)
        self.tr_session.SetFieldData('t8412InBlock', 'edate', 0, date)
        self.tr_session.SetFieldData('t8412InBlock', 'comp_yn', 0, 'N')

        self.tr_session.Request(0)

        while XAQueryTransactionHandler.tr_state == 0:
            pythoncom.PumpWaitingMessages()

        occurs_count = self.tr_session.GetBlockCount('t8412OutBlock1')

        data = []

        for i in range(occurs_count):
            date = self.tr_session.GetFieldData('t8412OutBlock1', 'date', i)
            t = self.tr_session.GetFieldData('t8412OutBlock1', 'time', i)
            open_p = self.tr_session.GetFieldData('t8412OutBlock1', 'open', i)
            high = self.tr_session.GetFieldData('t8412OutBlock1', 'high', i)
            low = self.tr_session.GetFieldData('t8412OutBlock1', 'low', i)
            close = self.tr_session.GetFieldData('t8412OutBlock1', 'close', i)
            vol = self.tr_session.GetFieldData('t8412OutBlock1', 'jdiff_vol', i)
            jongchk = self.tr_session.GetFieldData('t8412OutBlock1', 'jongchk', i) # 수정구분
            rate = self.tr_session.GetFieldData('t8412OutBlock1', 'rate', i) # 수정비율
            sign = self.tr_session.GetFieldData('t8412OutBlock1', 'sign', i) # 종가등락구분 (1: 상한, 2: 상승, 3: 보합)

            data.append({
                'date': date,
                'code': code,
                'time': t,
                'strt_prc': int(open_p),
                'high_prc': int(high),
                'low_prc': int(low),
                'cls_prc': int(close),
                'trd_qty': int(vol),
                'jongchk': int(jongchk),
                'rate': float(rate),
                'sign': sign
            })

        if save:
            path = os.path.dirname(os.path.abspath(__file__))
            with open(f'{path}\\data\\{code}_{date}.pkl', 'wb') as f:
                pickle.dump(data, f)
            print(f'{code} {date} SAVED')
        time.sleep(3)
        return data


def minute_data_worker(id, queue):
    ebest = EbestAPI()

    while True:
        req = queue.get()

        code = req['code']
        date = req['date']

        path = os.path.dirname(os.path.abspath(__file__))
        if not os.path.exists(f'{path}\\data\\{code}_{date}.pkl'):
            print(f'[{id}]')
            ebest.get_minute_price_data(code, date, save=True)



if __name__ == '__main__':
    import datetime
    from multiprocessing import Queue, Process

    queues = [Queue() for i in range(2)]
    processes = [Process(target=minute_data_worker, args=(i, queues[i])) for i in range(2)]
    _ = [p.start() for p in processes]

    ebest = EbestAPI()

    today = datetime.datetime.now().strftime('%Y%m%d')
    samsung_1yr = ebest.get_daily_price_data('005930', '20200101', today)
    dates = [d['date'] for d in samsung_1yr]

    api_num = 1

    d_cnt = 0
    for date in dates:
        c_cnt = 0
        for code in ebest.stocks_list:

            req = {'code': code, 'date': date}

            if api_num == 1 or api_num == 2:
                queues[api_num - 1].put(req)
            else:
                path = os.path.dirname(os.path.abspath(__file__))
                if not os.path.exists(f'{path}\\data\\{code}_{date}.pkl'):
                    print(f'[3] ({d_cnt}/{len(dates)}) ({c_cnt}/{len(ebest.stocks_list)})')
                    ebest.get_minute_price_data(code, date, save=True)

            api_num += 1
            if api_num > 3:
                api_num = 1

            c_cnt += 1
        d_cnt += 1