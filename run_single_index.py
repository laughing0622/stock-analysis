import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
from datetime import datetime, timedelta
from config import TS_TOKEN, DB_PATH

# 设置 Tushare
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 定义要处理的指数
INDEX_NAME = '创业板指'
INDEX_CODE = '399006.SZ'
START_DATE = '20190101'
END_DATE = '20251009'

def get_constituents_safe(index_code, date_str):
    """
    智能获取成分股：
    - 上证指数(000001.SH): 获取当日上交所所有上市股票
    - 其他指数: 查询 index_weight
    """
    try:
        # 创业板指，查询 index_weight
        start_dt = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=31)).strftime('%Y%m%d')
        df = pro.index_weight(index_code=index_code, start_date=start_dt, end_date=date_str)
        
        # 如果没查到，尝试往前找半年（应对半年调仓的指数）
        if df.empty:
            start_dt_long = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=180)).strftime('%Y%m%d')
            df = pro.index_weight(index_code=index_code, start_date=start_dt_long, end_date=date_str)
        
        if df.empty:
            return []
            
        # 取离 target_date 最近的一天
        latest_date = df['trade_date'].max()
        codes = df[df['trade_date'] == latest_date]['con_code'].unique().tolist()
        return codes  # 返回所有成分股

    except Exception as e:
        print(f"      [Err] 获取成分股失败: {e}")
        return []

def get_quarters(start_date, end_date):
    """将大时间段切割为季度区间 [(start, end), ...]"""
    quarters = pd.date_range(start=start_date, end=end_date, freq='Q') # Quarter End
    intervals = []
    
    # 转换逻辑
    curr = datetime.strptime(start_date, '%Y%m%d')
    for q_end in quarters:
        q_end_str = q_end.strftime('%Y%m%d')
        if q_end_str > end_date:
            break
        intervals.append((curr.strftime('%Y%m%d'), q_end_str))
        curr = q_end + timedelta(days=1)
    
    # 加上最后一段 (如果 end_date 不是季度末)
    last_start = curr.strftime('%Y%m%d')
    if last_start <= end_date:
        intervals.append((last_start, end_date))
        
    return intervals

def process_chunk(index_name, index_code, start_date, end_date):
    """处理一个时间切片 (季度)"""
    print(f"   -> 正在处理区间: {start_date} ~ {end_date}")
    
    # 1. 获取该季度初的成分股 (Point-in-Time)
    stock_list = get_constituents_safe(index_code, start_date)
    
    if not stock_list:
        print(f"      [跳过] 无法获取成分股列表")
        return pd.DataFrame()

    print(f"      成分股数量: {len(stock_list)}")

    # 2. 拉取数据 (含40天 Buffer 算均线)
    real_start_date = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')
    
    all_dfs = []
    batch_size = 50 # 50只一批，稳定第一
    
    # 进度条显示
    total_batches = (len(stock_list) + batch_size - 1) // batch_size
    
    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        codes_str = ','.join(batch)
        try:
            # 这里的 batch_start/end 是为了节省内存，只取我们需要的时间段
            df_d = pro.daily(ts_code=codes_str, start_date=real_start_date, end_date=end_date)
            df_f = pro.adj_factor(ts_code=codes_str, start_date=real_start_date, end_date=end_date)
            
            if not df_d.empty and not df_f.empty:
                df_d = df_d[['ts_code', 'trade_date', 'close', 'pct_chg']]
                df_f = df_f[['ts_code', 'trade_date', 'adj_factor']]
                df_m = pd.merge(df_d, df_f, on=['ts_code', 'trade_date'], how='inner')
                all_dfs.append(df_m)
        except Exception as e:
            print(f"      [Err] 拉取数据失败: {e}")
            time.sleep(1) # 报错稍微歇一下
            pass
        
        # 简单防流控
        time.sleep(0.05)

    if not all_dfs: 
        return pd.DataFrame()
    
    # 3. 合并与计算
    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.sort_values(['ts_code', 'trade_date'])
    
    # 计算复权价
    df_all['hfq_close'] = df_all['close'] * df_all['adj_factor']
    
    # 计算 MA20
    df_all['ma20'] = df_all.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20).mean())
    df_all['is_above_ma20'] = (df_all['hfq_close'] > df_all['ma20']).astype(int)
    
    # 计算 连跌3日 (使用 pct_chg)
    df_all['is_down'] = (df_all['pct_chg'] < 0)
    # shift(1)是昨天
    df_all['down_1'] = df_all.groupby('ts_code')['is_down'].shift(1)
    df_all['down_2'] = df_all.groupby('ts_code')['is_down'].shift(2)
    df_all['is_down_3days'] = (df_all['is_down'] & df_all['down_1'] & df_all['down_2']).astype(int)
    
    # 4. 截取有效时间段 (去掉Buffer)
    df_valid = df_all[df_all['trade_date'] >= start_date].copy()
    
    # 5. 聚合统计
    df_stats = df_valid.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum')
    ).reset_index()
    
    return df_stats

def main():
    print(f"处理指数: {INDEX_NAME} ({INDEX_CODE})")
    print(f"日期范围: {START_DATE} ~ {END_DATE}")
    
    # 1. 获取指数自身价格 (用来做主图)
    try:
        print("   -> 拉取指数行情...")
        df_idx_price = pro.index_daily(ts_code=INDEX_CODE, start_date=START_DATE, end_date=END_DATE)
        df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})
        print(f"   -> 成功获取 {len(df_idx_price)} 条指数行情数据")
    except Exception as e:
        print(f"   [Error] 指数行情拉取失败: {e}")
        return

    # 2. 获取季度区间
    intervals = get_quarters(START_DATE, END_DATE)
    print(f"   -> 共 {len(intervals)} 个季度区间")

    # 3. 连接数据库
    conn = sqlite3.connect(DB_PATH)
    
    # 4. 按季度循环处理
    for (s_date, e_date) in intervals:
        try:
            df_breadth = process_chunk(INDEX_NAME, INDEX_CODE, s_date, e_date)
            
            if not df_breadth.empty:
                # 合并指数价格
                df_final = pd.merge(df_breadth, df_idx_price, on='trade_date', how='inner')
                
                # 算百分比
                df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
                df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
                
                # 入库
                data_tuples = []
                for _, row in df_final.iterrows():
                    # 初始化拥挤度为0.0，后续会通过其他方式计算或更新
                    data_tuples.append((
                        row['trade_date'], INDEX_CODE, INDEX_NAME,
                        row['idx_close'], row['pct_above_ma20'], row['pct_down_3days'], 0.0
                    ))
                
                c = conn.cursor()
                c.executemany('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?)', data_tuples)
                conn.commit()
                print(f"      [√] 已入库 {len(df_final)} 天数据")
            
        except Exception as e:
            print(f"      [!!!] 区间处理异常: {e}")
            time.sleep(5) # 出错多歇会

    conn.close()
    print(f"\n🎉 {INDEX_NAME} 指数数据处理完成！")

if __name__ == "__main__":
    main()
