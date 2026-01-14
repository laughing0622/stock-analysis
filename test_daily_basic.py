"""
测试tushare daily_basic接口批量查询问题
"""
import tushare as ts
import pandas as pd

# 使用你的token
TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'
pro = ts.pro_api(TOKEN)

print("="*60)
print("测试tushare daily_basic接口批量查询问题")
print("="*60)

# 测试数据
test_date = '20260109'
test_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']

print(f"\n测试日期: {test_date}")
print(f"测试股票: {test_codes}")
print()

# ========== 测试1: 单个股票查询 ==========
print("-"*60)
print("测试1: 单个股票查询（逐个查询）")
print("-"*60)
for code in test_codes:
    try:
        df = pro.daily_basic(ts_code=code, trade_date=test_date, fields='ts_code,trade_date,turnover_rate')
        if not df.empty:
            print(f"{code}: 换手率 = {df['turnover_rate'].values[0]}")
        else:
            print(f"{code}: 无数据")
    except Exception as e:
        print(f"{code}: 错误 - {e}")

print()

# ========== 测试2: 批量查询（逗号分隔） ==========
print("-"*60)
print("测试2: 批量查询（逗号分隔字符串）")
print("-"*60)
codes_str = ','.join(test_codes)
print(f"查询代码: {codes_str}")

try:
    df_batch = pro.daily_basic(ts_code=codes_str, trade_date=test_date, fields='ts_code,trade_date,turnover_rate')
    print(f"返回条数: {len(df_batch)}")
    if not df_batch.empty:
        print(df_batch)
    else:
        print("返回空DataFrame！")
except Exception as e:
    print(f"查询异常: {e}")

print()

# ========== 测试3: 批量查询（不指定fields） ==========
print("-"*60)
print("测试3: 批量查询（不指定fields参数）")
print("-"*60)
try:
    df_batch2 = pro.daily_basic(ts_code=codes_str, trade_date=test_date)
    print(f"返回条数: {len(df_batch2)}")
    if not df_batch2.empty:
        print(df_batch2[['ts_code', 'trade_date', 'turnover_rate']])
    else:
        print("返回空DataFrame！")
except Exception as e:
    print(f"查询异常: {e}")

print()

# ========== 测试4: 检查用户权限 ==========
print("-"*60)
print("测试4: 检查tushare用户积分/权限")
print("-"*60)
try:
    df_user = pro.query('user')
    print(df_user)
except Exception as e:
    print(f"无法获取用户信息: {e}")

print()
print("="*60)
print("测试结论:")
print("如果测试2和测试3都返回空数据，说明daily_basic接口")
print("不支持批量查询（可能是tushare的bug或积分权限限制）")
print("="*60)
