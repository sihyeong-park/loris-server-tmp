import os
import pythoncom
import win32com.client
from ebest.handler import XAQueryTransactionHandler


class EbestExecutionAPI:

    def __init__(self, accno, password):
        self.accno = accno
        self.password = password

        self.jango_session = win32com.client.DispatchWithEvents('XA_DataSet.XAQuery', XAQueryTransactionHandler)

        self.jango_session.ResFileName = 'C:\\eBEST\\xingAPI\\Res\\t0424.res'

    def get_jango_data(self):
        pass