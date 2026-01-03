"""只修复最近250天换手率"""
import sqlite3, pandas as pd
from data_engine import DataEngine

engine = DataEngine()
conn = sqlite3.connect('data/stock_data.db')

# 获取最近250个交易日
dates = engine.get_trade_cal(days=250)

print(f"修复最近250天: {dates[0]} ~ {dates[-1]}")

for i, date in enumerate(dates):
    if i % 10 == 0:
        print(f"进度: {i+1}/{len(dates)}")
    engine.update_market_breadth_for_date(date)

conn.close()

# 验证
conn = sqlite3.connect('data/stock_data.db')
df = pd.read_sql("SELECT AVG(pct_turnover_lt_3) avg_lt3, AVG(pct_turnover_gt_5) avg_gt5 FROM market_breadth", conn)
print(f"\n✅ 完成! 平均换手率<3%: {df.iloc[0]['avg_lt3']:.2f}%, >5%: {df.iloc[0]['avg_gt5']:.2f}%")
conn.close()
