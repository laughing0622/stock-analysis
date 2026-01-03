"""检查修复脚本中间数据"""
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/stock_data.db')

# 1. 检查异常数据分布
print("="*60)
print("当前数据库状态检查")
print("="*60)

# 检查换手率=100的记录
query1 = """
SELECT trade_date, index_name, pct_turnover_lt_3, pct_turnover_gt_5 
FROM market_breadth 
WHERE pct_turnover_lt_3 = 100 
ORDER BY trade_date DESC 
LIMIT 5
"""
df1 = pd.read_sql(query1, conn)
print("\n换手率<3%为100.0的最新记录（异常数据）:")
print(df1.to_string())

# 检查正常数据
query2 = """
SELECT trade_date, index_name, pct_turnover_lt_3, pct_turnover_gt_5 
FROM market_breadth 
WHERE pct_turnover_lt_3 > 0 AND pct_turnover_lt_3 < 100 
ORDER BY trade_date DESC 
LIMIT 5
"""
df2 = pd.read_sql(query2, conn)
print("\n正常数据（换手率<3%占比在0-100之间）:")
print(df2.to_string())

# 统计
query3 = """
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN pct_turnover_lt_3 = 100 THEN 1 ELSE 0 END) as abnormal_100,
    SUM(CASE WHEN pct_turnover_lt_3 = 0 THEN 1 ELSE 0 END) as abnormal_0,
    SUM(CASE WHEN pct_turnover_lt_3 > 0 AND pct_turnover_lt_3 < 100 THEN 1 ELSE 0 END) as normal
FROM market_breadth
"""
stats = pd.read_sql(query3, conn).iloc[0]
print("\n数据统计:")
print(f"总记录数: {stats['total']}")
print(f"异常数据(=100): {stats['abnormal_100']}")
print(f"异常数据(=0): {stats['abnormal_0']}")
print(f"正常数据: {stats['normal']}")

conn.close()
