import tushare as ts
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP
from db_manager import db

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

def calculate_breadth(index_name, trade_date):
    index_code = INDEX_MAP[index_name]
    print(f"   正在计算 {index_name} ({index_code}) 在 {trade_date} 的数据...")

    # 1. 检查是否已存在
    if db.check_breadth_date_exists(trade_date, index_code):
        print(f"   -> {trade_date} 数据已存在，跳过。")
        return

    # 2. 获取指数收盘价
    df_idx = pro.index_daily(ts_code=index_code, trade_date=trade_date)
    if df_idx.empty:
        print("   -> 无指数行情数据")
        return
    idx_close = float(df_idx.iloc[0]['close'])

    # 3. 获取成分股 (权重接口比较耗积分，如果失败则尝试备用方案)
    # 为了演示稳定，我们这里取指数最新的成分股，近似作为当天的成分股
    try:
        df_w = pro.index_weight(index_code=index_code, start_date='20240101', end_date=trade_date)
        if df_w.empty:
             # 如果当天没权重数据，取最近的一个月
             start_dt = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=30)).strftime('%Y%m%d')
             df_w = pro.index_weight(index_code=index_code, start_date=start_dt, end_date=trade_date)
        
        if df_w.empty:
            print("   -> 无法获取成分股，跳过")
            return
            
        # 取最新一日的权重列表
        latest_d = df_w['trade_date'].max()
        stock_list = df_w[df_w['trade_date'] == latest_d]['con_code'].unique().tolist()
        print(f"   -> 成分股数量: {len(stock_list)}")
        
    except Exception as e:
        print(f"   -> 获取成分股出错: {e}")
        return

    # 4. 核心计算：MA20 & 连跌
    # 需要拉取过去30天的日线来计算MA20
    # 为了节省IO，我们分批处理，每批50只
    
    start_lookback = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')
    
    cnt_total = 0
    cnt_ma20 = 0
    cnt_down3 = 0
    
    batch_size = 50
    for i in range(0, len(stock_list), batch_size):
        batch = stock_list[i:i+batch_size]
        try:
            # 拉取这批股票的历史数据
            df_daily = pro.daily(ts_code=','.join(batch), start_date=start_lookback, end_date=trade_date)
            if df_daily.empty: continue
            
            # 按股票分组计算
            for code, group in df_daily.groupby('ts_code'):
                group = group.sort_values('trade_date')
                if len(group) < 20: continue
                
                # 确认最后一条是目标日期
                last_row = group.iloc[-1]
                if last_row['trade_date'] != trade_date: continue
                
                closes = group['close'].values
                
                # A. MA20判定
                ma20 = np.mean(closes[-20:])
                if closes[-1] > ma20:
                    cnt_ma20 += 1
                    
                # B. 连跌3日判定 (今天跌，昨天跌，前天跌)
                # Close[t] < Close[t-1] < Close[t-2] < Close[t-3]
                # 简化逻辑：连跌3日即连续3天的涨跌幅<0
                if len(group) >= 3:
                    # 检查最后3天
                    changes = group['change'].values[-3:]
                    if np.all(changes < 0):
                        cnt_down3 += 1
                        
                cnt_total += 1
                
            time.sleep(0.2) # 稍微防一下流控
            
        except Exception as e:
            print(f"   -> 批处理出错: {e}")
            continue

    # 5. 存库
    if cnt_total > 0:
        pct_ma20 = (cnt_ma20 / cnt_total) * 100
        pct_down3 = (cnt_down3 / cnt_total) * 100
        
        db.save_breadth(trade_date, index_code, index_name, idx_close, pct_ma20, pct_down3)
        print(f"   -> [成功] {index_name}: MA20={pct_ma20:.1f}%, 连跌={pct_down3:.1f}%")
    else:
        print("   -> 有效股票数为0，未保存")

def run():
    today = datetime.now().strftime('%Y%m%d')
    # 如果现在是下午3点半以后，计算今天；否则计算昨天
    if datetime.now().hour < 15:
        print("当前未收盘，不执行更新。")
        return

    print(f"=== 开始执行宏观择时任务 {today} ===")
    for name in INDEX_MAP.keys():
        try:
            calculate_breadth(name, today)
        except Exception as e:
            print(f"ERROR {name}: {e}")