"""
测试回填修复效果 - 只处理最近1周数据
"""
import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP, DB_PATH

# 设置Tushare
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 测试日期范围：最近1周
from datetime import datetime, timedelta
START_DATE = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
END_DATE = datetime.now().strftime('%Y%m%d')

print(f"=== 测试回填修复效果 ===")
print(f"日期范围: {START_DATE} ~ {END_DATE}")
print()

conn = sqlite3.connect(DB_PATH)

# 获取全市场A股
print("获取全市场A股列表...")
df_all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
all_stocks_set = set(df_all_stocks['ts_code'].tolist())
print(f"全市场A股数量: {len(all_stocks_set)}")

# 处理沪深300作为测试
index_name = '沪深300'
index_code = '000300.SH'

print(f"\n====== 处理指数: {index_name} ======")

# 获取指数行情
print("拉取指数行情...")
df_idx_price = pro.index_daily(ts_code=index_code, start_date=START_DATE, end_date=END_DATE)
df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})
print(f"获取 {len(df_idx_price)} 条指数行情")

# 处理数据
print(f"\n处理区间: {START_DATE} ~ {END_DATE}")
print("开始拉取股票数据...")

real_start_date = (datetime.strptime(START_DATE, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
all_dfs = []
batch_size = 50

stock_list = list(all_stocks_set)
total_batches = (len(stock_list) + batch_size - 1) // batch_size

for i in range(0, len(stock_list), batch_size):
    batch = stock_list[i:i+batch_size]
    codes_str = ','.join(batch)

    if (i // batch_size + 1) % 20 == 0:
        print(f"  进度: {i//batch_size+1}/{total_batches} 批...")

    try:
        df_d = pro.daily(ts_code=codes_str, start_date=real_start_date, end_date=END_DATE)
        df_f = pro.adj_factor(ts_code=codes_str, start_date=real_start_date, end_date=END_DATE)

        if not df_d.empty and not df_f.empty:
            df_d = df_d[['ts_code', 'trade_date', 'close', 'pct_chg']]
            df_f = df_f[['ts_code', 'trade_date', 'adj_factor']]

            # 获取每日基础数据 (换手率) - 逐个查询
            all_basic = []
            for code in batch:
                try:
                    df_b = pro.daily_basic(ts_code=code, start_date=real_start_date, end_date=END_DATE, fields='ts_code,trade_date,turnover_rate')
                    if not df_b.empty:
                        all_basic.append(df_b)
                except:
                    pass

            df_m = pd.merge(df_d, df_f, on=['ts_code', 'trade_date'], how='inner')
            if all_basic:
                df_b_all = pd.concat(all_basic)
                df_m = pd.merge(df_m, df_b_all, on=['ts_code', 'trade_date'], how='left')
                df_m['turnover_rate'] = df_m['turnover_rate'].fillna(0)
            else:
                df_m['turnover_rate'] = 0.0

            all_dfs.append(df_m)
    except:
        time.sleep(0.1)
        pass

print(f"数据拉取完成，合并计算...")

if not all_dfs:
    print("没有获取到数据！")
    exit(1)

# 合并计算
df_all = pd.concat(all_dfs, ignore_index=True)
df_all = df_all.sort_values(['ts_code', 'trade_date'])

# 计算复权价
df_all['hfq_close'] = df_all['close'] * df_all['adj_factor']

# 计算 MA20
df_all['ma20'] = df_all.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20).mean())
df_all['is_above_ma20'] = (df_all['hfq_close'] > df_all['ma20']).astype(int)

# 计算连跌3日
df_all['is_down'] = (df_all['pct_chg'] < 0)
df_all['down_1'] = df_all.groupby('ts_code')['is_down'].shift(1)
df_all['down_2'] = df_all.groupby('ts_code')['is_down'].shift(2)
df_all['is_down_3days'] = (df_all['is_down'] & df_all['down_1'] & df_all['down_2']).astype(int)

# 计算换手率状态
df_all['is_turnover_lt_3'] = (df_all['turnover_rate'] < 3.0).astype(int)
df_all['is_turnover_gt_5'] = (df_all['turnover_rate'] > 5.0).astype(int)

# 截取有效时间段
df_valid = df_all[df_all['trade_date'] >= START_DATE].copy()

# 聚合统计
df_stats = df_valid.groupby('trade_date').agg(
    total_count=('ts_code', 'count'),
    ma20_count=('is_above_ma20', 'sum'),
    down3_count=('is_down_3days', 'sum'),
    turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
    turnover_gt_5_count=('is_turnover_gt_5', 'sum')
).reset_index()

print(f"\n计算结果:")
print(df_stats[['trade_date', 'total_count', 'turnover_lt_3_count', 'turnover_gt_5_count']])

# 计算百分比
df_final = pd.merge(df_stats, df_idx_price, on='trade_date', how='inner')
df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
df_final['pct_turnover_lt_3'] = (df_final['turnover_lt_3_count'] / df_final['total_count']) * 100
df_final['pct_turnover_gt_5'] = (df_final['turnover_gt_5_count'] / df_final['total_count']) * 100

print(f"\n最终数据（百分比）:")
print(df_final[['trade_date', 'pct_turnover_lt_3', 'pct_turnover_gt_5']])

# 验证：检查是否有正常的换手率数据
normal_count = (df_final['pct_turnover_gt_5'] > 0).sum()
print(f"\n验证结果:")
print(f"总天数: {len(df_final)}")
print(f"正常数据天数（换手率>5%有值）: {normal_count}")

if normal_count > 0:
    print("\n✅ 修复成功！换手率数据正常！")

    # 写入数据库
    print("\n写入数据库...")
    crowd_index = 0.0  # 简化处理
    data_tuples = []
    for _, row in df_final.iterrows():
        data_tuples.append((
            row['trade_date'], index_code, index_name,
            row['idx_close'], row['pct_above_ma20'], row['pct_down_3days'], crowd_index,
            row['pct_turnover_lt_3'], row['pct_turnover_gt_5']
        ))

    c = conn.cursor()
    c.executemany('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?,?,?)', data_tuples)
    conn.commit()
    print(f"已写入 {len(data_tuples)} 条数据")
else:
    print("\n❌ 修复失败！换手率数据仍然全是0")

conn.close()
print("\n测试完成！")
