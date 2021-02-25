import os
import pythoncom
import win32com.client
from ebest.handler import XAQueryRealTimeHandler


class EbestRealTimeAPI:

    def __init__(self,
                 stocks_list=[],
                 kospi_list=[],
                 kosdaq_list=[],
                 futures_list=[]):
        self.stocks_list = stocks_list
        self.kospi_list = kospi_list
        self.kosdaq_list = kosdaq_list
        self.futures_list = futures_list

        self.register_market_event()
        self.register_kospi_trade_event()
        self.register_kospi_orberbook_event()
        self.register_kosdaq_trade_event()
        self.register_kosdaq_orberbook_event()

        while True:
            pythoncom.PumpWaitingMessages()

    def register_market_event(self):
        self.real_market_event_session = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XAQueryRealTimeHandler)
        self.real_market_event_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\JIF.res'
        self.real_market_event_session.SetFieldData('InBlock', 'jangubun', '0')
        self.real_market_event_session.AdviseRealData()

    def register_kospi_trade_event(self):
        self.real_kospi_trade_session = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XAQueryRealTimeHandler)
        self.real_kospi_trade_session = win32com.client.DispatchWithEvents('XA_DataSet.XAReal', XAQueryRealTimeHandler)
        self.real_kospi_trade_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\S3_.res'

        for shcode in self.kospi_list:
            self.real_kospi_trade_session.SetFieldData('InBlock', 'shcode', shcode)
            self.real_kospi_trade_session.AdviseRealData()

        print(f'KOSPI 주식 체결 종목 등록: {len(self.kospi_list)}')

    def register_kospi_orberbook_event(self):
        self.real_kospi_orderbook_session = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XAQueryRealTimeHandler)
        self.real_kospi_orderbook_session = win32com.client.DispatchWithEvents('XA_DataSet.XAReal', XAQueryRealTimeHandler)
        self.real_kospi_orderbook_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\H1_.res'

        for shcode in self.kospi_list:
            self.real_kospi_orderbook_session.SetFieldData('InBlock', 'shcode', shcode)
            self.real_kospi_orderbook_session.AdviseRealData()

        print(f'KOSPI 주식 호가잔량 종목 등록: {len(self.kospi_list)}')

    def register_kosdaq_trade_event(self):
        self.real_kosdaq_trade_session = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XAQueryRealTimeHandler)
        self.real_kosdaq_trade_session = win32com.client.DispatchWithEvents('XA_DataSet.XAReal', XAQueryRealTimeHandler)
        self.real_kosdaq_trade_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\K3_.res'

        for shcode in self.kosdaq_list:
            self.real_kosdaq_trade_session.SetFieldData('InBlock', 'shcode', shcode)
            self.real_kosdaq_trade_session.AdviseRealData()

        print(f'KOSPI 주식 체결 종목 등록: {len(self.kosdaq_list)}')

    def register_kosdaq_orberbook_event(self):
        self.real_kosdaq_orderbook_session = win32com.client.DispatchWithEvents("XA_DataSet.XAReal", XAQueryRealTimeHandler)
        self.real_kosdaq_orderbook_session = win32com.client.DispatchWithEvents('XA_DataSet.XAReal', XAQueryRealTimeHandler)
        self.real_kosdaq_orderbook_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\HA_.res'

        for shcode in self.kosdaq_list:
            self.real_kosdaq_orderbook_session.SetFieldData('InBlock', 'shcode', shcode)
            self.real_kosdaq_orderbook_session.AdviseRealData()

        print(f'KOSPI 주식 호가잔량 종목 등록: {len(self.kosdaq_list)}')

    def register_futures_real_event(self):
        pass