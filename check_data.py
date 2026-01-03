import sqlite3
import pandas as pd

# 连接数据库
conn = sqlite3.connect('data/stock_data.db')

print("=== 各指数数据条数 ===")
df_counts = pd.read_sql('SELECT index_name, COUNT(*) as count FROM market_breadth GROUP BY index_name', conn)
print(df_counts)

print("\n=== 上证指数数据时间范围 ===")
df_sh = pd.read_sql('SELECT MIN(trade_date) as start_date, MAX(trade_date) as end_date FROM market_breadth WHERE index_name = "上证指数"', conn)
print(df_sh)

print("\n=== 最新一天(20251212)各指数数据 ===")
df_latest = pd.read_sql('SELECT * FROM market_breadth WHERE trade_date = "20251212"', conn)
print(df_latest)

print("\n=== 上证指数2024-2025年数据统计 ===")
df_sh_2024_2025 = pd.read_sql('SELECT trade_date, close, pct_above_ma20, pct_down_3days FROM market_breadth WHERE index_name = "上证指数" AND trade_date >= "20240101"', conn)
print(f"2024-2025年共 {len(df_sh_2024_2025)} 条数据")

# 统计2024年和2025年的数据条数
df_sh_2024_2025['year'] = df_sh_2024_2025['trade_date'].astype(str).str[:4]
year_counts = df_sh_2024_2025['year'].value_counts().sort_index()
print(year_counts)

print("\n=== 2025年数据样本(最近5条) ===")
df_2025 = df_sh_2024_2025[df_sh_2024_2025['year'] == '2025']
print(df_2025.tail())

# 检查市场宽度和市场情绪为0的情况
print("\n=== 2025年市场宽度或市场情绪为0的天数 ===")
df_zero = df_sh_2024_2025[(df_sh_2024_2025['pct_above_ma20'] == 0) | (df_sh_2024_2025['pct_down_3days'] == 0)]
print(f"2024-2025年共 {len(df_zero)} 天市场宽度或市场情绪为0")

# 关闭数据库连接
conn.close()
