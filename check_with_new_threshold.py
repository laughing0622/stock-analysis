"""检查放量倍数=1时的筛选结果"""
import tushare as ts
import pandas as pd
from datetime import datetime

# 初始化 Tushare
TS_TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

print("=== 检查放量倍数=1的筛选条件 ===\n")

# 从测试结果看到的两只股票
stocks = [
    {'代码': '688802', '名称': 'N沐曦-U', '实时成交额': 38.57},
    {'代码': '002837', '名称': '英维克', '实时成交额': 24.18}
]

# 获取最近5个交易日
def get_trade_cal(days=5):
    today = datetime.now().strftime('%Y%m%d')
    from datetime import timedelta
    start_d = (datetime.now() - timedelta(days=days*2 + 100)).strftime('%Y%m%d')
    cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_d, end_date=today)
    dates = cal['cal_date'].tolist()
    return dates[-days:] if len(dates) >= days else dates

trade_dates = get_trade_cal(5)
print(f"过去5个交易日: {trade_dates}\n")

# 获取过去5日成交数据
past_data = []
for date in trade_dates:
    df_day = pro.daily(trade_date=date, fields='ts_code,amount')
    if not df_day.empty:
        past_data.append(df_day)

df_past = pd.concat(past_data, ignore_index=True)
df_past_avg = df_past.groupby('ts_code')['amount'].mean().reset_index()
df_past_avg.columns = ['ts_code', 'avg_amount_5d']

print("【新筛选条件】")
print("  ✓ 成交额 ≥ 25亿")
print("  ✓ 放量倍数 ≥ 1.0\n")

for stock in stocks:
    code = stock['代码']
    name = stock['名称']
    realtime_amount = stock['实时成交额']
    
    # 转换为 ts_code
    if code.startswith('6'):
        ts_code = f"{code}.SH"
    elif code.startswith(('0', '3')):
        ts_code = f"{code}.SZ"
    else:
        ts_code = f"{code}.SH"
    
    print(f"【{name} ({ts_code})】")
    print(f"  实时成交额: {realtime_amount:.2f} 亿")
    
    # 查找5日均值
    avg_row = df_past_avg[df_past_avg['ts_code'] == ts_code]
    
    if not avg_row.empty:
        avg_amount_5d = avg_row.iloc[0]['avg_amount_5d'] / 10000  # 万元转亿元
        ratio = realtime_amount / avg_amount_5d if avg_amount_5d > 0 else 0
        
        print(f"  5日均成交额: {avg_amount_5d:.2f} 亿")
        print(f"  放量倍数: {ratio:.2f}")
        
        # 判断是否符合条件（新阈值：1.0）
        meets_threshold = realtime_amount >= 25.0
        meets_ratio = ratio >= 1.0
        
        print(f"  ✓ 成交额≥25亿: {'✅' if meets_threshold else '❌'}")
        print(f"  ✓ 放量倍数≥1.0: {'✅' if meets_ratio else '❌'}")
        
        if meets_threshold and meets_ratio:
            print(f"  🎯 【符合条件】应该被选出")
        else:
            print(f"  ❌ 【不符合条件】")
            if not meets_threshold:
                print(f"     原因: 成交额{realtime_amount:.2f}亿 < 25亿")
            if not meets_ratio:
                print(f"     原因: 放量倍数{ratio:.2f} < 1.0倍")
    else:
        print(f"  ⚠️  未找到5日均值（可能是新股）")
        if realtime_amount >= 25.0:
            print(f"  ❌ 【不符合条件】无法计算放量倍数")
    
    print()
