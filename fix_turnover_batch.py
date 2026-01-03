"""批量修复换手率 - 优化版"""
import sqlite3
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP
import time

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

DB_PATH = 'data/stock_data.db'
conn = sqlite3.connect(DB_PATH)

print("="*60)
print("批量修复换手率（优化版）")
print("="*60)

# 1. 获取需要修复的日期（换手率为100或0都是异常）
query = "SELECT DISTINCT trade_date FROM market_breadth WHERE pct_turnover_lt_3 = 0 OR pct_turnover_lt_3 = 100 ORDER BY trade_date"
dates_to_fix = pd.read_sql(query, conn)['trade_date'].tolist()

if not dates_to_fix:
    print("✅ 无需修复")
    conn.close()
    exit(0)

print(f"\n需修复日期数: {len(dates_to_fix)}")
print(f"范围: {dates_to_fix[0]} ~ {dates_to_fix[-1]}")

# 2. 获取所有A股列表
df_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
all_stocks = df_stocks['ts_code'].tolist()
print(f"A股总数: {len(all_stocks)}")

# 3. 批量拉取所有日期的换手率数据
print("\n批量拉取换手率数据...")
all_turnover_data = []

for i in range(0, len(dates_to_fix), 50):  # 每50个日期一批
    batch_dates = dates_to_fix[i:i+50]
    print(f"  拉取日期 {i+1}-{min(i+50, len(dates_to_fix))}/{len(dates_to_fix)}")
    
    for date in batch_dates:
        retry = 0
        while retry < 3:
            try:
                df = pro.daily_basic(
                    trade_date=date,
                    fields='ts_code,trade_date,turnover_rate'
                )
                if not df.empty:
                    all_turnover_data.append(df)
                break
            except Exception as e:
                if "每分钟最多访问" in str(e):
                    retry += 1
                    if retry < 3:
                        time.sleep(61)
                        continue
                break
        time.sleep(0.3)  # 避免触发限制

if not all_turnover_data:
    print("❌ 数据拉取失败")
    conn.close()
    exit(1)

df_all = pd.concat(all_turnover_data, ignore_index=True)
print(f"✅ 拉取完成，共 {len(df_all)} 条记录")

# 4. 获取创业板成分股（按季度获取）
print("\n获取创业板成分股...")
cyb_stocks_by_quarter = {}  # {(year, quarter): set(stocks)}
quarters = sorted(set([f"{d[:4]}Q{(int(d[4:6])-1)//3 + 1}" for d in dates_to_fix]))  # 生成所有季度
api_calls = 0

try:
    for quarter_label in quarters:
        year = quarter_label[:4]
        quarter_num = int(quarter_label[-1])
        start_month = (quarter_num - 1) * 3 + 1
        end_month = quarter_num * 3
        start_date = f"{year}{start_month:02d}01"
        end_date = f"{year}{end_month:02d}31"
        
        df_w = pro.index_weight(
            index_code='399006.SZ',
            start_date=start_date,
            end_date=end_date
        )
        api_calls += 1
        
        if not df_w.empty:
            # 获取该季度最新的成分股
            latest_date = df_w['trade_date'].max()
            stocks = df_w[df_w['trade_date'] == latest_date]['con_code'].unique()
            cyb_stocks_by_quarter[(year, quarter_num)] = set(stocks)
        else:
            cyb_stocks_by_quarter[(year, quarter_num)] = all_stocks_set
        
        if api_calls % 20 == 0:  # 每20次调用休息一下
            time.sleep(1)
    
    print(f"✅ 创业板成分股: {len(cyb_stocks_by_quarter)}个季度 (API调用{api_calls}次)")
except Exception as e:
    print(f"⚠️ 获取失败: {e}，使用全市场数据")
    for q in quarters:
        year, quarter_num = q.split('Q')[0], int(q.split('Q')[1])
        cyb_stocks_by_quarter[(year, quarter_num)] = all_stocks_set

all_stocks_set = set(all_stocks)

# 5. 批量计算并更新
print("\n计算换手率指标...")
df_all['turnover_rate'] = pd.to_numeric(df_all['turnover_rate'], errors='coerce').fillna(0)

update_batch = []
for i, trade_date in enumerate(dates_to_fix):
    if i % 100 == 0:
        print(f"  进度: {i+1}/{len(dates_to_fix)}")
    
    # 获取季度信息
    year = trade_date[:4]
    quarter_num = (int(trade_date[4:6]) - 1) // 3 + 1
    df_date = df_all[df_all['trade_date'] == trade_date]
    
    for idx_name, idx_code in INDEX_MAP.items():
        # 根据指数选择股票池
        if idx_code == '399006.SZ':  # 创业板
            constituents = cyb_stocks_by_quarter.get((year, quarter_num), all_stocks_set)
        else:  # 其他指数用全市场
            constituents = all_stocks_set
        
        df_idx = df_date[df_date['ts_code'].isin(constituents)]
        if df_idx.empty:
            continue
        
        total = len(df_idx)
        lt3 = len(df_idx[df_idx['turnover_rate'] < 3.0])
        gt5 = len(df_idx[df_idx['turnover_rate'] > 5.0])
        
        pct_lt3 = (lt3 / total * 100) if total > 0 else 0
        pct_gt5 = (gt5 / total * 100) if total > 0 else 0
        
        update_batch.append((pct_lt3, pct_gt5, trade_date, idx_code))

# 批量提交
print("\n提交到数据库...")
conn.executemany("""
    UPDATE market_breadth 
    SET pct_turnover_lt_3 = ?, pct_turnover_gt_5 = ?
    WHERE trade_date = ? AND index_code = ?
""", update_batch)
conn.commit()
print(f"✅ 已更新{len(update_batch)}条记录")

# 6. 验证
query = "SELECT AVG(pct_turnover_lt_3) as avg_lt3, AVG(pct_turnover_gt_5) as avg_gt5 FROM market_breadth"
result = pd.read_sql(query, conn).iloc[0]
conn.close()

print("\n" + "="*60)
print(f"✅ 修复完成！")
print(f"平均换手率<3%: {result['avg_lt3']:.2f}%")
print(f"平均换手率>5%: {result['avg_gt5']:.2f}%")
print("="*60)
