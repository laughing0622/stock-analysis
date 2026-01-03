"""
修复换手率数据问题并重新更新
"""
from data_engine import engine
from datetime import datetime
import sqlite3

print("="*60)
print("修复换手率数据问题")
print("="*60)

# 1. 显示当前问题
print("\n1. 当前数据状态检查:")
conn = sqlite3.connect('data/stock_data.db')
query = """
SELECT trade_date, index_name, pct_turnover_lt_3, pct_turnover_gt_5
FROM market_breadth 
ORDER BY trade_date DESC 
LIMIT 4
"""
import pandas as pd
df_before = pd.read_sql(query, conn)
print(df_before.to_string())
conn.close()

# 2. 重新更新今日数据
print("\n2. 重新更新今日数据（含换手率）...")
print("-" * 60)
try:
    engine.update_today_breadth()
    print("\n✅ 数据更新完成")
except Exception as e:
    print(f"\n❌ 更新失败: {e}")
    import traceback
    traceback.print_exc()

# 3. 验证修复结果
print("\n3. 验证修复后的数据:")
conn = sqlite3.connect('data/stock_data.db')
df_after = pd.read_sql(query, conn)
print(df_after.to_string())

# 4. 对比修复前后
print("\n4. 修复效果统计:")
total = len(df_after)
zero_lt3 = len(df_after[df_after['pct_turnover_lt_3'] == 0])
zero_gt5 = len(df_after[df_after['pct_turnover_gt_5'] == 0])

print(f"  - 总记录数: {total}")
print(f"  - pct_turnover_lt_3 为0的记录: {zero_lt3}/{total} ({zero_lt3/total*100:.1f}%)")
print(f"  - pct_turnover_gt_5 为0的记录: {zero_gt5}/{total} ({zero_gt5/total*100:.1f}%)")

if zero_lt3 < total or zero_gt5 < total:
    print("\n✅ 换手率数据修复成功！")
    print(f"  - pct_turnover_lt_3 平均值: {df_after['pct_turnover_lt_3'].mean():.2f}%")
    print(f"  - pct_turnover_gt_5 平均值: {df_after['pct_turnover_gt_5'].mean():.2f}%")
else:
    print("\n⚠️  换手率数据仍为0，可能需要检查：")
    print("  1. Tushare API 是否正常返回 turnover_rate 字段")
    print("  2. daily_basic 接口是否有数据")
    print("  3. 网络连接是否正常")

conn.close()

print("\n" + "="*60)
print("修复脚本执行完成")
print("="*60)
