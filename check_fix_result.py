import sqlite3
import pandas as pd

conn = sqlite3.connect('data/stock_data.db')

print("=== 20251212各指数详细数据 ===")
df_latest = pd.read_sql('SELECT index_name, trade_date, close, pct_above_ma20, pct_down_3days, crowd_index FROM market_breadth WHERE trade_date = "20251212"', conn)
print(df_latest)

print("\n=== 上证指数2025年完整月度数据分布 ===")
df_sh = pd.read_sql('SELECT trade_date, close, pct_above_ma20, pct_down_3days FROM market_breadth WHERE index_name = "上证指数" AND trade_date >= "20250101"', conn)
df_sh['year_month'] = df_sh['trade_date'].astype(str).str[:6]  # YYYYMM
month_counts = df_sh['year_month'].value_counts().sort_index()
print(month_counts)

# 检查其他指数的月度数据分布
for index_name in ['沪深300', '创业板指', '中证2000']:
    print(f"\n=== {index_name} 2025年月度数据分布 ===")
    df_idx = pd.read_sql(f'SELECT trade_date FROM market_breadth WHERE index_name = "{index_name}" AND trade_date >= "20250101"', conn)
    df_idx['year_month'] = df_idx['trade_date'].astype(str).str[:6]
    month_counts_idx = df_idx['year_month'].value_counts().sort_index()
    print(month_counts_idx)

# 检查每个指数的最新10天数据
print("\n=== 各指数最新10天数据样本 ===")
for index_name in ['沪深300', '创业板指', '中证2000', '上证指数']:
    print(f"\n{index_name} 最新10天数据:")
    df_recent = pd.read_sql(f'SELECT trade_date, pct_above_ma20, pct_down_3days FROM market_breadth WHERE index_name = "{index_name}" ORDER BY trade_date DESC LIMIT 10', conn)
    print(df_recent[['trade_date', 'pct_above_ma20', 'pct_down_3days']])

conn.close()
