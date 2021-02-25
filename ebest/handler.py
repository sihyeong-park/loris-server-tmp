import datetime


class XAQueryLoginHandler:
    login_state = 0

    def OnLogin(self, code, msg):
        if code == '0000':
            print('logged in')
            XAQueryLoginHandler.login_state = 1
        else:
            raise Exception('이베스트 API 로그인 실패하였습니다.')

class XAQueryTransactionHandler:
    tr_state = 0

    def OnReceiveData(self, code):

        if code == 't1102':
            """
            주식 현재가(시세) 조회
            """
            XAQueryTransactionHandler.tr_state = 1

        if code == 't0424':
            """
            주식 잔고 2
            """
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8430':
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8413':
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8412':
            """
            주식 분봉 데이터
            """
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8415':
            """
            선물 분봉 데이터
            """
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8436':
            """
            주식 종목 코드
            """
            XAQueryTransactionHandler.tr_state = 1

        if code == 't8401':
            """
            주식 선물 종목 코드
            """
            XAQueryTransactionHandler.tr_state = 1

class XAQueryRealTimeHandler:

    def OnReceiveRealData(self, code):

        if code == 'JIF':
            """
            장운영정보
            """
            jangubun = self.GetFieldData('OutBlock', 'jangubun')
            jstatus = self.GetFieldData('OutBlock', 'jstatus')

            print('jangubun: ', jangubun, 'jstatus: ', jstatus)

            if jangubun == '1' and jstatus == '21':
                market_open = {'type': 'Market_Open'}

            if jangubun == '1' and jstatus == '31':
                market_close = {'type': 'Market_Close'}

        if (code == 'S3_') or (code == 'K3_'):
            """
            코스피/코스닥 주식 체결
            """
            tick_data = {'type': 'tick',
                         'code': self.GetFieldData('OutBlock', 'shcode'),
                         'trade_date': self.GetFieldData('OutBlock', 'chetime'),
                         'timestamp': datetime.datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3],
                         'current_price': int(self.GetFieldData('OutBlock', 'price')),
                         'open': int(self.GetFieldData('OutBlock', 'open')),
                         'high': int(self.GetFieldData('OutBlock', 'high')),
                         'low': int(self.GetFieldData('OutBlock', 'low')),
                         'volume': int(self.GetFieldData('OutBlock', 'cvolume')),
                         'cum_volume': int(self.GetFieldData('OutBlock', 'volume')),
                         'trade_sell_hoga1': int(self.GetFieldData('OutBlock', 'offerho')),
                         'trade_buy_hoga1': int(self.GetFieldData('OutBlock', 'bidho'))}
            print(tick_data)

        if (code == 'H1_') or (code == 'HA_'):
            """
            코스피/코스닥 주식 호가
            """
            hoga_data = {'type': 'hoga',
                         'code': self.GetFieldData('OutBlock', 'shcode'),
                         'hoga_date': int(self.GetFieldData('OutBlock', 'hotime')),
                         'timestamp': datetime.datetime.now().strftime("%Y%m%d%H%M%S.%f")[:-3]}
            for i in range(1, 11):
                hoga_data['buy_hoga' + str(i)] = int(self.GetFieldData('OutBlock', 'bidho' + str(i)))
                hoga_data['sell_hoga' + str(i)] = int(self.GetFieldData('OutBlock', 'offerho' + str(i)))
                hoga_data['buy_hoga' + str(i) + '_stack'] = int(self.GetFieldData('OutBlock', 'bidrem' + str(i)))
                hoga_data['sell_hoga' + str(i) + '_stack'] = int(self.GetFieldData('OutBlock', 'offerrem' + str(i)))
            print(hoga_data)

        if code == 'JC0':
            """
            주식 선물 체결
            """
            pass

        if code == 'JH0':
            """
            주식 선물 호가
            """
            pass