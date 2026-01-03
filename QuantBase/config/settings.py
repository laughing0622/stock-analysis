import os

# Tushare Token (请替换为你自己的)
TUSHARE_TOKEN = "5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e"

# 数据库连接配置
# 示例 SQLite: 'sqlite:///quant_data.db'
# 示例 MySQL: 'mysql+pymysql://user:password@localhost:3306/quant_db?charset=utf8mb4'
DB_URI = 'sqlite:///quant_data.db'

# 核心关注的指数列表
CORE_INDICES = {
    '000001.SH': '上证指数',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
    '399006.SZ': '创业板指',
    '000688.SH': '科创50'
}

# 数据起始时间（如果是全量初始化，从何时开始）
START_DATE_DEFAULT = '20180101'