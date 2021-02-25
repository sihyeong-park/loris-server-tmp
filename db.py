from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, BigInteger, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Info(Base):
    __tablename__ = 'info'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    stk_kind = Column(String(50)) # 산업 구분
    mkt_gb = Column(String(50)) # 코스피, 코스닥 시장
    mkt_cap = Column(BigInteger)
    mkt_cap_size = Column(String(50)) # 대형주, 중형주, 소형주, 제외
    frg_hlg = Column(Float)
    mgt_gb = Column(String(50)) # 정상, 정지, 관리 등


class Price(Base):
    __tablename__ = 'price'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    strt_prc = Column(Integer)
    high_prc = Column(Integer)
    low_prc = Column(Integer)
    cls_prc = Column(Integer)
    adj_prc = Column(BigInteger)
    trd_qty = Column(Float) # (x1000)
    trd_amt = Column(Float) # (x1000000)
    shtsale_trd_qty = Column(Float) # (x1000)


class MinutePrice(Base):
    __tablename__ = 'minute_price'

    id = Column(Integer, primary_key=True)
    date = Column(String(20))
    code = Column(String(20))
    strt_prc = Column(Integer)
    high_prc = Column(Integer)
    low_prc = Column(Integer)
    cls_prc = Column(Integer)
    trd_qty = Column(Integer)


class Index(Base):
    __tablename__ = 'index_data'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    strt_prc = Column(Float) # 시가
    high_prc = Column(Float) # 고가
    low_prc = Column(Float) # 저가
    cls_prc = Column(Float) # 종가
    trd_qty = Column(Float) # 거래량 (x1000)
    trd_amt = Column(Float) # 거래대금 (x1000000)


class MarketCapital(Base):
    __tablename__ = 'market_capital'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    comm_stk_qty = Column(Integer) # (x1000)
    pref_stk_qty = Column(Integer) # (x1000)


class BuySell(Base):
    __tablename__ = 'buy_sell'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    forgn_b = Column(Integer)
    forgn_s = Column(Integer)
    forgn_n = Column(Integer)
    private_b = Column(Integer)
    private_s = Column(Integer)
    private_n = Column(Integer)
    inst_sum_b = Column(Integer)
    inst_sum_s = Column(Integer)
    inst_sum_n = Column(Integer)
    trust_b = Column(Integer) # 투자신탁
    trust_s = Column(Integer)
    trust_n = Column(Integer)
    pension_b = Column(Integer) # 연기금
    pension_s = Column(Integer)
    pension_n = Column(Integer)
    etc_inst_b = Column(Integer)
    etc_inst_s = Column(Integer)
    etc_inst_n = Column(Integer)


class Factor(Base):
    __tablename__ = 'factor'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    per = Column(Float)
    pbr = Column(Float)
    pcr = Column(Float)
    psr = Column(Float)
    divid_yield = Column(Float)


class ETF(Base):
    __tablename__ = 'etf'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    code = Column(String(20))
    name = Column(String(50))
    cls_prc = Column(Integer)
    trd_qty = Column(Integer)
    trd_amt = Column(BigInteger)
    etf_nav = Column(Float)
    spread = Column(Float)


class Future(Base):
    __tablename__ = 'future'

    id = Column(Integer, primary_key=True)
    date = Column(String(10))
    name = Column(String(50))
    cls_prc = Column(Float)
    theo_prc_account = Column(Float)
    basis_mkt = Column(Float)
    strd_index = Column(Integer)
    trd_qty = Column(Integer)
    trd_amt = Column(BigInteger)
    remain_day_cnt = Column(Integer)
    unsetl_qty = Column(Integer)


# class BalanceSheet(Base):
#     __tablename__ = 'balance_sheet'
#
#
# class CashFlow(Base):
#     __tablename__ = 'cash_flow'
#
#
# class IncomeStatement(Base):
#     __tablename__ = 'income_statement'


if __name__ == '__main__':
    db = create_engine('mysql://root:qraft@10.0.1.245:3306/fnguide?charset=utf8mb4')
    # session = sessionmaker(bind=db)
    # s = session()

    # Base.metadata.create_all(db)


    Info.__table__.create(bind=db, checkfirst=True)
    Price.__table__.create(bind=db, checkfirst=True)
    MinutePrice.__table__.create(bind=db, checkfirst=True)
    Index.__table__.create(bind=db, checkfirst=True)
    MarketCapital.__table__.create(bind=db, checkfirst=True)
    BuySell.__table__.create(bind=db, checkfirst=True)
    Factor.__table__.create(bind=db, checkfirst=True)
    ETF.__table__.create(bind=db, checkfirst=True)
    Future.__table__.create(bind=db, checkfirst=True)