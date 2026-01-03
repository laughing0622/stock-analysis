import tushare as ts
from config import TS_TOKEN

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 检查创业板最近的成分股调整日期
print("检查创业板成分股最近调整日期...")
df = pro.index_weight(index_code='399006.SZ', start_date='20240101', end_date='20251216')

if df.empty:
    print("❌ 无法获取创业板成分股数据")
else:
    print(f"✅ 获取到 {len(df)} 条记录")
    print(f"最新调整日期: {df['trade_date'].max()}")
    print(f"最新日期成分股数量: {len(df[df['trade_date'] == df['trade_date'].max()])}")
    print(f"\n最近的调整日期:")
    print(df['trade_date'].unique()[:10])
