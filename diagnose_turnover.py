"""
诊断换手率数据异常问题
"""
import sqlite3
import pandas as pd

DB_PATH = 'data/stock_data.db'

print("="*60)
print("诊断换手率数据问题")
print("="*60)

conn = sqlite3.connect(DB_PATH)

# 1. 检查表结构
print("\n1. 检查 market_breadth 表结构:")
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(market_breadth)")
columns = cursor.fetchall()
print(f"表字段数: {len(columns)}")
for col in columns:
    print(f"  - {col[1]} ({col[2]})")

# 2. 检查换手率字段是否存在
col_names = [col[1] for col in columns]
has_lt3 = 'pct_turnover_lt_3' in col_names
has_gt5 = 'pct_turnover_gt_5' in col_names
print(f"\n2. 换手率字段检查:")
print(f"  - pct_turnover_lt_3: {'✅ 存在' if has_lt3 else '❌ 缺失'}")
print(f"  - pct_turnover_gt_5: {'✅ 存在' if has_gt5 else '❌ 缺失'}")

# 3. 查询最近10天的数据
print(f"\n3. 查询最近10天的换手率数据:")
query = """
SELECT trade_date, index_name, 
       pct_turnover_lt_3, pct_turnover_gt_5,
       pct_above_ma20, pct_down_3days
FROM market_breadth 
ORDER BY trade_date DESC 
LIMIT 20
"""
df = pd.read_sql(query, conn)
print(df.to_string())

# 4. 统计换手率数据为0的比例
print(f"\n4. 换手率数据统计:")
total_count = len(df)
if has_lt3:
    zero_lt3 = len(df[df['pct_turnover_lt_3'] == 0])
    print(f"  - pct_turnover_lt_3 为0的记录: {zero_lt3}/{total_count} ({zero_lt3/total_count*100:.1f}%)")
    print(f"  - pct_turnover_lt_3 平均值: {df['pct_turnover_lt_3'].mean():.2f}")
    print(f"  - pct_turnover_lt_3 最大值: {df['pct_turnover_lt_3'].max():.2f}")
    
if has_gt5:
    zero_gt5 = len(df[df['pct_turnover_gt_5'] == 0])
    print(f"  - pct_turnover_gt_5 为0的记录: {zero_gt5}/{total_count} ({zero_gt5/total_count*100:.1f}%)")
    print(f"  - pct_turnover_gt_5 平均值: {df['pct_turnover_gt_5'].mean():.2f}")
    print(f"  - pct_turnover_gt_5 最大值: {df['pct_turnover_gt_5'].max():.2f}")

# 5. 检查某一天的详细数据
print(f"\n5. 检查最新交易日的详细数据:")
if not df.empty:
    latest_date = df.iloc[0]['trade_date']
    query_detail = f"""
    SELECT * FROM market_breadth 
    WHERE trade_date = '{latest_date}'
    """
    df_detail = pd.read_sql(query_detail, conn)
    print(f"\n最新日期: {latest_date}")
    print(df_detail.to_string())

conn.close()

print("\n" + "="*60)
print("诊断完成")
print("="*60)
