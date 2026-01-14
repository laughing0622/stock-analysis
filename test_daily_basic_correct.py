"""
测试 daily_basic 接口的正确用法
根据官方文档验证：
1. ts_code='' 获取全部股票
2. 单个股票查询
3. 批量查询（逗号分隔）- 文档未明确支持
"""
import tushare as ts
import pandas as pd
from config import TS_TOKEN

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

print("=" * 60)
print("测试 daily_basic 接口的不同用法")
print("=" * 60)

# 测试日期
test_date = '20250109'

# ==================== 测试1: ts_code='' 获取全部股票 ====================
print("\n【测试1】ts_code='' 获取全部股票的当日数据")
print("-" * 60)
try:
    df_all = pro.daily_basic(ts_code='', trade_date=test_date,
                             fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_all)}")
    if not df_all.empty:
        print(f"换手率列统计:")
        print(df_all['turnover_rate'].describe())
        print(f"\n前5条样例:")
        print(df_all.head())
except Exception as e:
    print(f"查询异常: {e}")


# ==================== 测试2: 单个股票查询 ====================
print("\n【测试2】单个股票查询: 000001.SZ")
print("-" * 60)
try:
    df_single = pro.daily_basic(ts_code='000001.SZ', trade_date=test_date,
                                 fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_single)}")
    if not df_single.empty:
        print(f"换手率: {df_single['turnover_rate'].values[0]}")
except Exception as e:
    print(f"查询异常: {e}")


# ==================== 测试3: 批量查询（逗号分隔） ====================
print("\n【测试3】批量查询（逗号分隔）: 000001.SZ,600000.SH")
print("-" * 60)
codes_str = '000001.SZ,600000.SH,600036.SH,000002.SZ'
try:
    df_batch = pro.daily_basic(ts_code=codes_str, trade_date=test_date,
                                fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_batch)}")
    if not df_batch.empty:
        print(f"结果:")
        print(df_batch)
    else:
        print("【警告】返回空DataFrame！说明批量查询不支持")
except Exception as e:
    print(f"查询异常: {e}")


# ==================== 测试4: 使用 start_date/end_date ====================
print("\n【测试4】使用日期范围: ts_code='' + start_date/end_date")
print("-" * 60)
try:
    df_range = pro.daily_basic(ts_code='', start_date='20250108', end_date='20250109',
                                fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_range)}")
    if not df_range.empty:
        print(f"涵盖股票数: {df_range['ts_code'].nunique()}")
        print(f"涵盖日期数: {df_range['trade_date'].nunique()}")
        print(f"日期: {sorted(df_range['trade_date'].unique())}")
except Exception as e:
    print(f"查询异常: {e}")


# ==================== 测试5: 历史日期测试 ====================
print("\n【测试5】历史日期测试: 20200102（2020年初）")
print("-" * 60)
try:
    df_history = pro.daily_basic(ts_code='', trade_date='20200102',
                                  fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_history)}")
    if not df_history.empty:
        print(f"换手率列统计:")
        print(df_history['turnover_rate'].describe())
        print(f"\n前5条样例:")
        print(df_history.head())
        # 检查有多少股票换手率>0
        valid_count = (df_history['turnover_rate'] > 0).sum()
        print(f"\n换手率>0的股票数: {valid_count}/{len(df_history)}")
except Exception as e:
    print(f"查询异常: {e}")


print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
