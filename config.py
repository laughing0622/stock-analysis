import os

# Tushare Token
TS_TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'

# 数据库路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'stock_data.db')

# 监控指数列表
INDEX_MAP = {
    '沪深300': '000300.SH',
    '创业板指': '399006.SZ',
    '中证2000': '932000.CSI', 
    '上证指数': '000001.SH'
}