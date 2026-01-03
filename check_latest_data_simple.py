import sqlite3
import pandas as pd

conn = sqlite3.connect('data/stock_data.db')

print("=== 20251212各指数详细数据 ===")
query = "SELECT index_name, trade_date, close, pct_above_ma20, pct_down_3days, crowd_index FROM market_breadth WHERE trade_date = '20251212'"
df_latest = pd.read_sql(query, conn)
print(df_latest)

print("\n=== 检查中证2000市场宽度计算逻辑 ===")
# 检查中证2000的有效股票数量
from data_engine import engine
from datetime import datetime, timedelta

index_name = "中证2000"
index_code = "932000.CSI"
trade_date = "20251212"

print(f"\n检查 {index_name} {trade_date} 的计算逻辑:")

# 获取成分股
stock_list = engine._get_index_constituents(index_code, trade_date)
print(f"成分股数量: {len(stock_list)}")

# 计算需要的日期范围
end_date = trade_date
start_date = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=20)).strftime('%Y%m%d')
print(f"需要的日期范围: {start_date} ~ {end_date}")

# 获取成分股数据
from config import TS_TOKEN
import tushare as ts
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 只获取前10只成分股的数据进行测试
test_stocks = stock_list[:10]
print(f"测试使用前 {len(test_stocks)} 只成分股")

df_d = pro.daily(ts_code=','.join(test_stocks), start_date=start_date, end_date=end_date, fields='ts_code,trade_date,close,pct_chg')
print(f"获取到 {len(df_d)} 条个股数据")
print("样本数据:")
print(df_d.head())

conn.close()
