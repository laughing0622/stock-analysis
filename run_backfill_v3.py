"""
回填脚本 V3 - 修复历史数据问题
关键改进：不使用当前股票列表限制历史数据查询
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

def process_index_data(index_name, index_code, start_date, end_date):
    """处理单个指数的数据回填"""
    print(f"   -> 处理指数: {index_name} ({index_code})")

    # 计算实际开始日期（为了MA20计算）
    real_start_date = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')

    # 获取交易日历
    trading_dates = get_trading_dates(real_start_date, end_date)
    print(f"      交易日数量: {len(trading_dates)}")

    # ==================== 关键改进：不使用stock_list限制 ====================
    # 直接按日期获取所有数据，而不是按股票列表

    # 1. 获取换手率数据
    print(f"      开始获取换手率数据（按日期批量）...")
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

    # 5. 截取有效时间段
    df_valid = df_all[df_all['trade_date'] >= start_date].copy()

    # 6. 聚合统计
    df_stats = df_valid.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum'),
        turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
        turnover_gt_5_count=('is_turnover_gt_5', 'sum')
    ).reset_index()

    return df_stats

def run_backfill_v3():
    """运行回填V3"""
    START_DATE = '20190101'
    END_DATE = datetime.now().strftime('%Y%m%d')

    print(f"=== 回填V3 - 修复历史数据问题 ===")
    print(f"日期范围: {START_DATE} ~ {END_DATE}")
    print(f"改进：按日期获取全市场数据，不使用当前股票列表")

    conn = sqlite3.connect(DB_PATH)

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
        df_breadth = process_index_data(index_name, index_code, START_DATE, END_DATE)

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
        print(df_final[['trade_date', 'pct_turnover_lt_3', 'pct_turnover_gt_5']].tail(10))

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
    run_backfill_v3()
