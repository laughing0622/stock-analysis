"""
回填脚本 V3 增量版 - 只回填异常数据
只回填 pct_turnover_gt_5 = 0 的日期
"""
import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
import sys
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP, DB_PATH

# 禁用输出缓冲，实时显示日志
sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

def get_trading_dates(start_date, end_date):
    """获取交易日历"""
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    return df[df['is_open']==1]['cal_date'].tolist()

def fetch_turnover_by_date(trade_date):
    """获取指定日期全市场的换手率数据"""
    try:
        df = pro.daily_basic(
            ts_code='',  # 空字符串表示全市场
            trade_date=trade_date,
            fields='ts_code,trade_date,turnover_rate'
        )
        return df
    except Exception as e:
        print(f"      获取{trade_date}换手率失败: {e}")
        return pd.DataFrame()

def get_abnormal_dates(index_code):
    """获取需要回填的异常日期列表"""
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql('''
        SELECT trade_date
        FROM market_breadth
        WHERE index_code = ? AND pct_turnover_gt_5 = 0
        ORDER BY trade_date
    ''', conn, params=[index_code])

    conn.close()
    return df['trade_date'].tolist()

def process_index_data(index_name, index_code, dates_to_fill):
    """处理单个指数的数据回填（只处理指定日期）"""
    print(f"   -> 处理指数: {index_name} ({index_code})")
    print(f"      需要回填的日期数: {len(dates_to_fill)}")

    if not dates_to_fill:
        print(f"      无需回填")
        return pd.DataFrame()

    # 获取交易日历（为了MA20计算，需要额外30天）
    first_date = dates_to_fill[0]
    last_date = dates_to_fill[-1]
    real_start_date = (datetime.strptime(first_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')

    # 获取需要处理的交易日历（包括前后缓冲）
    trading_dates_all = get_trading_dates(real_start_date, last_date)
    # 只处理需要回填的日期
    trading_dates = [d for d in trading_dates_all if d in dates_to_fill]

    print(f"      交易日数量: {len(trading_dates)}")

    # 1. 获取换手率数据（只获取需要回填的日期）
    print(f"      开始获取换手率数据...")
    all_turnover_data = []
    for i, trade_date in enumerate(trading_dates):
        df_t = fetch_turnover_by_date(trade_date)
        if not df_t.empty:
            all_turnover_data.append(df_t)

        # 进度显示
        if (i + 1) % 50 == 0:
            print(f"        换手率获取进度: {i+1}/{len(trading_dates)}")

        # 防流控
        time.sleep(0.1)

    if not all_turnover_data:
        print(f"      换手率数据获取失败！")
        return pd.DataFrame()

    df_all_turnover = pd.concat(all_turnover_data, ignore_index=True)
    print(f"      换手率数据总量: {len(df_all_turnover)} 条")

    # 2. 获取日线和复权数据（按日期分批，不限制股票列表）
    print(f"      开始获取日线和复权数据...")
    all_price_data = []

    # 按日期循环获取数据，每次获取全市场数据
    for i, trade_date in enumerate(trading_dates):
        try:
            # 获取该日期的全市场日线数据
            df_d = pro.daily(trade_date=trade_date, fields='ts_code,trade_date,close,pct_chg')

            # 获取该日期的全市场复权数据
            df_f = pro.adj_factor(trade_date=trade_date, fields='ts_code,trade_date,adj_factor')

            if not df_d.empty and not df_f.empty:
                df_m = pd.merge(df_d, df_f, on=['ts_code', 'trade_date'], how='inner')
                all_price_data.append(df_m)

            # 进度显示
            if (i + 1) % 50 == 0:
                print(f"        日线获取进度: {i+1}/{len(trading_dates)}")

            # 防流控
            time.sleep(0.05)
        except Exception as e:
            print(f"      日期{trade_date}处理异常: {e}")
            time.sleep(1)
            pass

    if not all_price_data:
        return pd.DataFrame()

    df_all_prices = pd.concat(all_price_data, ignore_index=True)
    print(f"      日线数据总量: {len(df_all_prices)} 条")

    # 3. 合并换手率数据
    df_all = pd.merge(df_all_prices, df_all_turnover, on=['ts_code', 'trade_date'], how='left')
    df_all['turnover_rate'] = df_all['turnover_rate'].fillna(0)

    # 验证
    with_turnover = (df_all['turnover_rate'] > 0).sum()
    print(f"      合并后记录总数: {len(df_all)} 条，有换手率数据: {with_turnover} 条 ({with_turnover/len(df_all)*100:.1f}%)")

    # 4. 计算指标
    df_all = df_all.sort_values(['ts_code', 'trade_date'])
    df_all['hfq_close'] = df_all['close'] * df_all['adj_factor']
    df_all['ma20'] = df_all.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20).mean())
    df_all['is_above_ma20'] = (df_all['hfq_close'] > df_all['ma20']).astype(int)

    df_all['is_down'] = (df_all['pct_chg'] < 0)
    df_all['down_1'] = df_all.groupby('ts_code')['is_down'].shift(1)
    df_all['down_2'] = df_all.groupby('ts_code')['is_down'].shift(2)
    df_all['is_down_3days'] = (df_all['is_down'] & df_all['down_1'] & df_all['down_2']).astype(int)

    df_all['is_turnover_lt_3'] = (df_all['turnover_rate'] < 3.0).astype(int)
    df_all['is_turnover_gt_5'] = (df_all['turnover_rate'] > 5.0).astype(int)

    # 5. 聚合统计
    df_stats = df_all.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum'),
        turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
        turnover_gt_5_count=('is_turnover_gt_5', 'sum')
    ).reset_index()

    return df_stats

def run_backfill_v3_incremental():
    """运行增量回填V3 - 只回填异常数据"""
    END_DATE = datetime.now().strftime('%Y%m%d')

    print(f"=== 增量回填V3 - 只回填异常数据 ===")
    print(f"改进：按日期获取全市场数据，跳过已有正常数据")

    conn = sqlite3.connect(DB_PATH)

    # 处理所有指数
    for index_name, index_code in INDEX_MAP.items():
        print(f"\n====== 处理指数: {index_name} ======")

        # 获取需要回填的异常日期
        abnormal_dates = get_abnormal_dates(index_code)

        if not abnormal_dates:
            print(f"      无需回填，所有数据都正常")
            continue

        print(f"      需要回填的日期数: {len(abnormal_dates)}")
        print(f"      最早异常日期: {abnormal_dates[0]}")
        print(f"      最晚异常日期: {abnormal_dates[-1]}")

        # 处理数据
        df_breadth = process_index_data(index_name, index_code, abnormal_dates)

        if df_breadth.empty:
            print("没有获取到数据！")
            continue

        # 读取指数行情用于合并
        df_idx_price = pro.index_daily(ts_code=index_code, start_date=abnormal_dates[0], end_date=END_DATE)
        df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})

        # 合并计算
        df_final = pd.merge(df_breadth, df_idx_price, on='trade_date', how='inner')
        df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
        df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
        df_final['pct_turnover_lt_3'] = (df_final['turnover_lt_3_count'] / df_final['total_count']) * 100
        df_final['pct_turnover_gt_5'] = (df_final['turnover_gt_5_count'] / df_final['total_count']) * 100

        print(f"\n计算结果（前10条）:")
        print(df_final[['trade_date', 'total_count', 'pct_turnover_lt_3', 'pct_turnover_gt_5']].head(10))

        # 验证
        normal_count = (df_final['pct_turnover_gt_5'] > 0).sum()
        print(f"\n验证: 正常数据天数 {normal_count}/{len(df_final)} ({normal_count/len(df_final)*100:.1f}%)")

        if normal_count > 0:
            # 入库（使用INSERT OR REPLACE更新已有数据）
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
            print(f"已更新 {len(data_tuples)} 条数据")

    print("\n=== 增量回填完成 ===")
    conn.close()

if __name__ == "__main__":
    run_backfill_v3_incremental()
