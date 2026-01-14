"""
并行查询版回填脚本 - 加速换手率数据获取
使用多线程并行查询daily_basic接口
"""
import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP, DB_PATH
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 线程锁用于日志输出
log_lock = threading.Lock()

def fetch_turnover_for_stock(code, start_date, end_date):
    """获取单个股票的换手率数据"""
    try:
        df = pro.daily_basic(ts_code=code, start_date=start_date, end_date=end_date,
                            fields='ts_code,trade_date,turnover_rate')
        if not df.empty:
            return df
    except:
        pass
    return None

def fetch_turnover_batch(stock_list, start_date, end_date, max_workers=10):
    """并行获取一批股票的换手率数据"""
    all_data = []
    total = len(stock_list)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_code = {
            executor.submit(fetch_turnover_for_stock, code, start_date, end_date): code
            for code in stock_list
        }

        # 收集结果
        for i, future in enumerate(as_completed(future_to_code)):
            if future.result() is not None:
                all_data.append(future.result())

            # 进度显示
            if (i + 1) % 100 == 0:
                with log_lock:
                    print(f"      换手率查询进度: {i+1}/{total}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def process_chunk_parallel(index_name, index_code, start_date, end_date, all_stocks_set=None):
    """处理一个时间切片（并行版）"""
    print(f"   -> 正在处理区间: {start_date} ~ {end_date}")

    # 获取成分股
    if index_code == '399006.SZ':  # 创业板
        # 这里简化处理，使用全市场
        stock_list = list(all_stocks_set) if all_stocks_set else []
    else:
        stock_list = list(all_stocks_set) if all_stocks_set else []

    if not stock_list:
        return pd.DataFrame()

    print(f"      处理股票数量: {len(stock_list)}")

    # 拉取数据
    real_start_date = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')

    all_dfs = []
    batch_size = 50

    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        codes_str = ','.join(batch)

        try:
            # 拉取日线和复权因子（可以批量）
            df_d = pro.daily(ts_code=codes_str, start_date=real_start_date, end_date=end_date)
            df_f = pro.adj_factor(ts_code=codes_str, start_date=real_start_date, end_date=end_date)

            if not df_d.empty and not df_f.empty:
                df_d = df_d[['ts_code', 'trade_date', 'close', 'pct_chg']]
                df_f = df_f[['ts_code', 'trade_date', 'adj_factor']]

                df_m = pd.merge(df_d, df_f, on=['ts_code', 'trade_date'], how='inner')

                # 并行获取换手率数据
                df_turnover = fetch_turnover_batch(batch, real_start_date, end_date, max_workers=10)

                if not df_turnover.empty:
                    df_m = pd.merge(df_m, df_turnover, on=['ts_code', 'trade_date'], how='left')
                    df_m['turnover_rate'] = df_m['turnover_rate'].fillna(0)
                else:
                    df_m['turnover_rate'] = 0.0

                all_dfs.append(df_m)

            time.sleep(0.05)  # 防流控
        except Exception as e:
            print(f"      批处理异常: {e}")
            time.sleep(1)
            pass

    if not all_dfs:
        return pd.DataFrame()

    # 合并计算
    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.sort_values(['ts_code', 'trade_date'])

    # 计算复权价
    df_all['hfq_close'] = df_all['close'] * df_all['adj_factor']

    # 计算指标
    df_all['ma20'] = df_all.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20).mean())
    df_all['is_above_ma20'] = (df_all['hfq_close'] > df_all['ma20']).astype(int)

    df_all['is_down'] = (df_all['pct_chg'] < 0)
    df_all['down_1'] = df_all.groupby('ts_code')['is_down'].shift(1)
    df_all['down_2'] = df_all.groupby('ts_code')['is_down'].shift(2)
    df_all['is_down_3days'] = (df_all['is_down'] & df_all['down_1'] & df_all['down_2']).astype(int)

    df_all['is_turnover_lt_3'] = (df_all['turnover_rate'] < 3.0).astype(int)
    df_all['is_turnover_gt_5'] = (df_all['turnover_rate'] > 5.0).astype(int)

    # 截取有效时间段
    df_valid = df_all[df_all['trade_date'] >= start_date].copy()

    # 聚合统计
    df_stats = df_valid.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum'),
        turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
        turnover_gt_5_count=('is_turnover_gt_5', 'sum')
    ).reset_index()

    return df_stats

def run_parallel_backfill():
    # 配置：全量回填从2019年开始
    from datetime import datetime, timedelta
    START_DATE = '20190101'  # 全量回填
    END_DATE = datetime.now().strftime('%Y%m%d')

    print(f"=== 并行查询版全量回填 ===")
    print(f"日期范围: {START_DATE} ~ {END_DATE}")
    print(f"使用多线程并行查询换手率")
    print(f"警告：全量回填需要10-15小时，请确保网络稳定")

    conn = sqlite3.connect(DB_PATH)

    # 获取全市场股票
    print("\n获取全市场A股列表...")
    df_all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
    all_stocks_set = set(df_all_stocks['ts_code'].tolist())
    print(f"全市场A股数量: {len(all_stocks_set)}")

    # 处理所有指数
    for index_name, index_code in INDEX_MAP.items():
        print(f"\n====== 处理指数: {index_name} ======")

        # 获取指数行情
        print("拉取指数行情...")
        try:
            df_idx_price = pro.index_daily(ts_code=index_code, start_date=START_DATE, end_date=END_DATE)
            df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})
            print(f"获取 {len(df_idx_price)} 条指数行情")
        except Exception as e:
            print(f"获取指数行情失败: {e}")
            continue

        # 处理数据
        print(f"\n开始处理数据...")
        df_breadth = process_chunk_parallel(index_name, index_code, START_DATE, END_DATE, all_stocks_set)

        if df_breadth.empty:
            print("没有获取到数据！")
            continue

        # 合并计算
        df_final = pd.merge(df_breadth, df_idx_price, on='trade_date', how='inner')
        df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
        df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
        df_final['pct_turnover_lt_3'] = (df_final['turnover_lt_3_count'] / df_final['total_count']) * 100
        df_final['pct_turnover_gt_5'] = (df_final['turnover_gt_5_count'] / df_final['total_count']) * 100

        print(f"\n计算结果:")
        print(df_final[['trade_date', 'pct_turnover_lt_3', 'pct_turnover_gt_5']])

        # 验证
        normal_count = (df_final['pct_turnover_gt_5'] > 0).sum()
        print(f"\n验证: 正常数据天数 {normal_count}/{len(df_final)}")

        if normal_count > 0:
            # 入库
            print("\n写入数据库...")
            crowd_index = 0.0
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
            print("\n数据异常，跳过")

    print("\n[OK] 所有指数回填完成！")
    conn.close()

if __name__ == "__main__":
    run_parallel_backfill()
