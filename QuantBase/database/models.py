from sqlalchemy import Column, String, Float, Date, DateTime, Integer, DECIMAL, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class StockBasic(Base):
    __tablename__ = 'meta_stock_basic'
    ts_code = Column(String(10), primary_key=True)
    symbol = Column(String(10))
    name = Column(String(50))
    area = Column(String(50))
    industry = Column(String(50))
    list_date = Column(String(8))
    market = Column(String(20))
    is_hs = Column(String(10)) # 是否沪深港通标的
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class TradeCal(Base):
    __tablename__ = 'meta_trade_cal'
    exchange = Column(String(10), primary_key=True)
    cal_date = Column(String(8), primary_key=True)
    is_open = Column(Integer)

class DailyQuote(Base):
    __tablename__ = 'data_daily_quote'
    trade_date = Column(String(8), primary_key=True)
    ts_code = Column(String(10), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    pre_close = Column(Float)
    change = Column(Float)
    pct_chg = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    # 复权因子
    adj_factor = Column(Float)
    
class IndexDaily(Base):
    __tablename__ = 'data_index_daily'
    trade_date = Column(String(8), primary_key=True)
    ts_code = Column(String(20), primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    # === 新增以下三个字段 ===
    pre_close = Column(Float)  # 昨收价
    change = Column(Float)     # 涨跌额
    pct_chg = Column(Float)    # 涨跌幅
    # =======================
    vol = Column(Float)
    amount = Column(Float)

class IndexWeight(Base):
    __tablename__ = 'data_index_weight'
    index_code = Column(String(20), primary_key=True)
    con_code = Column(String(20), primary_key=True)
    trade_date = Column(String(8), primary_key=True)
    weight = Column(Float)