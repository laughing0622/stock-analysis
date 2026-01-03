"""
仅修复换手率数据（不重建其他数据）
快速修复脚本 - 只更新 pct_turnover_lt_3 和 pct_turnover_gt_5 字段
"""
import sqlite3
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP

# 设置 Tushare
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

print("="*60)
print("仅修复换手率数据（快速模式）")
print("="*60)

DB_PATH = 'data/stock_data.db'
conn = sqlite3.connect(DB_PATH)

# 1. 获取需要修复的日期列表（换手率为0的记录）
print("\n1. 检查需要修复的数据...")
query = """
SELECT DISTINCT trade_date 
FROM market_breadth 
WHERE pct_turnover_lt_3 = 0 OR pct_turnover_gt_5 = 0
ORDER BY trade_date
"""
df_dates = pd.read_sql(query, conn)
dates_to_fix = df_dates['trade_date'].tolist()

print(f"   需要修复的日期数: {len(dates_to_fix)}")
if len(dates_to_fix) == 0:
    print("   ✅ 所有数据正常，无需修复")
    conn.close()
    exit(0)

print(f"   日期范围: {dates_to_fix[0]} ~ {dates_to_fix[-1]}")

# 2. 获取成分股函数
def get_constituents(index_code, date_str):
    """获取指数成分股"""
    try:
        if index_code == '000001.SH':
            df = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,list_date')
            df = df[df['list_date'] <= date_str]
            return df['ts_code'].tolist()
        else:
            start_dt = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=31)).strftime('%Y%m%d')
            df = pro.index_weight(index_code=index_code, start_date=start_dt, end_date=date_str)
            if df.empty:
                start_dt = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=180)).strftime('%Y%m%d')
                df = pro.index_weight(index_code=index_code, start_date=start_dt, end_date=date_str)
            if df.empty:
                return []
            latest_date = df['trade_date'].max()
            return df[df['trade_date'] == latest_date]['con_code'].unique().tolist()
    except Exception as e:
        print(f"      获取成分股异常: {e}")
        return []

# 3. 计算换手率指标
def calculate_turnover(index_code, index_name, trade_date):
    """仅计算换手率指标"""
    import time
    try:
        stock_list = get_constituents(index_code, trade_date)
        if not stock_list:
            return 0.0, 0.0
        
        # 只需要当天的数据
        batch_size = 500
        all_basic_data = []
        
        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i:i+batch_size]
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    df_basic = pro.daily_basic(
                        ts_code=','.join(batch), 
                        trade_date=trade_date,
                        fields='ts_code,trade_date,turnover_rate'
                    )
                    if not df_basic.empty:
                        all_basic_data.append(df_basic)
                    break  # 成功则跳出重试循环
                    
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        retry_count += 1
                        if retry_count < max_retries:
                            time.sleep(61)  # 等待61秒后重试
                            continue
                    print(f"      批次拉取失败: {e}")
                    break
        
        if not all_basic_data:
            return 0.0, 0.0
        
        df = pd.concat(all_basic_data)
        df['turnover_rate'] = pd.to_numeric(df['turnover_rate'], errors='coerce').fillna(0)
        
        # 计算统计
        valid_stocks = len(df)
        if valid_stocks == 0:
            return 0.0, 0.0
        
        turnover_lt_3_count = len(df[df['turnover_rate'] < 3.0])
        turnover_gt_5_count = len(df[df['turnover_rate'] > 5.0])
        
        pct_lt_3 = (turnover_lt_3_count / valid_stocks) * 100
        pct_gt_5 = (turnover_gt_5_count / valid_stocks) * 100
        
        return pct_lt_3, pct_gt_5
        
    except Exception as e:
        print(f"      计算异常: {e}")
        return 0.0, 0.0

# 4. 修复数据
print("\n2. 开始修复换手率数据...")
total_fixed = 0
failed_dates = []

for i, trade_date in enumerate(dates_to_fix):
    if i % 10 == 0:
        print(f"   进度: {i+1}/{len(dates_to_fix)} - {trade_date}")
    
    # 获取该日期所有指数的记录
    query_date = f"SELECT index_code, index_name FROM market_breadth WHERE trade_date='{trade_date}'"
    df_indices = pd.read_sql(query_date, conn)
    
    for _, row in df_indices.iterrows():
        index_code = row['index_code']
        index_name = row['index_name']
        
        try:
            pct_lt_3, pct_gt_5 = calculate_turnover(index_code, index_name, trade_date)
            
            # 更新数据库
            update_sql = """
            UPDATE market_breadth 
            SET pct_turnover_lt_3 = ?, pct_turnover_gt_5 = ?
            WHERE trade_date = ? AND index_code = ?
            """
            conn.execute(update_sql, (pct_lt_3, pct_gt_5, trade_date, index_code))
            total_fixed += 1
            
        except Exception as e:
            print(f"   ❌ {trade_date} {index_name} 修复失败: {e}")
            failed_dates.append(f"{trade_date}_{index_name}")
    
    # 每10天提交一次
    if (i + 1) % 10 == 0:
        conn.commit()

# 最后提交
conn.commit()

# 5. 验证修复结果
print("\n3. 验证修复结果...")
query_verify = """
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN pct_turnover_lt_3 = 0 THEN 1 ELSE 0 END) as zero_lt3,
    SUM(CASE WHEN pct_turnover_gt_5 = 0 THEN 1 ELSE 0 END) as zero_gt5,
    AVG(pct_turnover_lt_3) as avg_lt3,
    AVG(pct_turnover_gt_5) as avg_gt5
FROM market_breadth
"""
df_verify = pd.read_sql(query_verify, conn)
result = df_verify.iloc[0]

print(f"   总记录数: {result['total']}")
print(f"   pct_turnover_lt_3 为0: {result['zero_lt3']} ({result['zero_lt3']/result['total']*100:.1f}%)")
print(f"   pct_turnover_gt_5 为0: {result['zero_gt5']} ({result['zero_gt5']/result['total']*100:.1f}%)")
print(f"   pct_turnover_lt_3 平均值: {result['avg_lt3']:.2f}%")
print(f"   pct_turnover_gt_5 平均值: {result['avg_gt5']:.2f}%")

conn.close()

print("\n" + "="*60)
if total_fixed > 0:
    print(f"✅ 换手率数据修复完成！共修复 {total_fixed} 条记录")
    if failed_dates:
        print(f"⚠️  失败 {len(failed_dates)} 条: {failed_dates[:5]}")
else:
    print("⚠️  未修复任何数据")
print("="*60)

print("\n后续操作：")
print("1. 刷新 Streamlit 网页")
print("2. 在宏观页面查看换手率指标")
print("3. 点击图例显示换手率曲线")
