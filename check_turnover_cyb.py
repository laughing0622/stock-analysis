import tushare as ts
from config import TS_TOKEN

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 检查12月16日创业板全部股票的换手率分布
df = pro.daily_basic(trade_date='20251216', fields='ts_code,turnover_rate')
cyb = df[df['ts_code'].str.startswith('300')]

print(f"创业板总数: {len(cyb)}")
print(f"换手率<3%: {len(cyb[cyb['turnover_rate']<3])} ({len(cyb[cyb['turnover_rate']<3])/len(cyb)*100:.1f}%)")
print(f"换手率>5%: {len(cyb[cyb['turnover_rate']>5])} ({len(cyb[cyb['turnover_rate']>5])/len(cyb)*100:.1f}%)")
print(f"\n指数成分股100只的换手率分布:")
# 获取11月28日的100只成分股
df_w = pro.index_weight(index_code='399006.SZ', start_date='20241128', end_date='20241128')
constituents = set(df_w['con_code'].tolist())
print(f"成分股数量: {len(constituents)}")

# 检查这100只股票的换手率
cyb_100 = df[df['ts_code'].isin(constituents)]
print(f"换手率<3%: {len(cyb_100[cyb_100['turnover_rate']<3])} ({len(cyb_100[cyb_100['turnover_rate']<3])/len(cyb_100)*100:.1f}%)")
print(f"换手率>5%: {len(cyb_100[cyb_100['turnover_rate']>5])} ({len(cyb_100[cyb_100['turnover_rate']>5])/len(cyb_100)*100:.1f}%)")
