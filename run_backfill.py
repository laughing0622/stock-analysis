import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
from datetime import datetime, timedelta
from config import TS_TOKEN, INDEX_MAP, DB_PATH

# 1. 设置 Tushare
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 2. 辅助函数：获取区间
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

def get_constituents_safe(index_code, date_str):
    """
    智能获取成分股：
    - 上证指数(000001.SH): 获取当日上交所所有上市股票
    - 其他指数: 查询 index_weight
    """
    try:
        # === 特殊处理：上证指数 ===
        if index_code == '000001.SH':
            # 获取该日期仍在上市的 SSE 股票
            # list_status='L' (上市), exchange='SSE'
            df = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,list_date,delist_date')
            # 筛选：上市日期 <= date_str
            df = df[df['list_date'] <= date_str]
            return df['ts_code'].tolist()
        
        # === 常规指数：沪深300/中证500/创业板等 ===
        # 找最近的一个月内的权重数据
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
        return codes

    except Exception as e:
        print(f"      [Err] 获取成分股失败: {e}")
        return []

def process_chunk(index_name, index_code, start_date, end_date, all_stocks_set=None):
    """处理一个时间切片 (季度)
    
    Args:
        index_name: 指数名称
        index_code: 指数代码
        start_date: 开始日期
        end_date: 结束日期
        all_stocks_set: 全市场股票集合（用于非创业板指数）
    """
    print(f"   -> 正在处理区间: {start_date} ~ {end_date}")
    
    # 1. 获取成分股：只有创业板需要获取成分股，其他用全市场
    if index_code == '399006.SZ':  # 创业板
        stock_list = get_constituents_safe(index_code, start_date)
        if not stock_list:
            print(f"      [跳过] 无法获取成分股列表")
            return pd.DataFrame()
        print(f"      创业板成分股数量: {len(stock_list)}")
    else:
        # 其他指数：使用全市场股票
        if all_stocks_set is None:
            print(f"      [跳过] 全市场股票集合未提供")
            return pd.DataFrame()
        stock_list = list(all_stocks_set)
        print(f"      使用全市场股票数量: {len(stock_list)}")

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
                
                # 获取每日基础数据 (换手率)
                # 注意：daily_basic接口批量查询会返回空数据，需要逐个查询
                all_basic = []
                for code in batch:
                    try:
                        df_b = pro.daily_basic(ts_code=code, start_date=real_start_date, end_date=end_date, fields='ts_code,trade_date,turnover_rate')
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
    
    # Calculate turnover status
    df_all['is_turnover_lt_3'] = (df_all['turnover_rate'] < 3.0).astype(int)
    df_all['is_turnover_gt_5'] = (df_all['turnover_rate'] > 5.0).astype(int)
    
    # 4. 截取有效时间段 (去掉Buffer)
    df_valid = df_all[df_all['trade_date'] >= start_date].copy()
    
    # 5. 聚合统计
    df_stats = df_valid.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum'),
        turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
        turnover_gt_5_count=('is_turnover_gt_5', 'sum')
    ).reset_index()
    
    return df_stats

def calculate_crowd_index(pro, trade_date):
    """计算真实的拥挤度指标：成交额排名前5%的个股成交额占全部A成交额的比例"""
    try:
        # 获取全部A股的成交额数据
        df_daily = pro.daily(trade_date=trade_date, fields='ts_code,amount')
        if df_daily.empty:
            return 0
        
        # 计算成交额前5%的个股
        total_stocks = len(df_daily)
        top_5_pct_count = max(1, int(total_stocks * 0.05))  # 至少1只
        
        # 按成交额降序排序
        df_sorted = df_daily.sort_values('amount', ascending=False)
        
        # 获取前5%的个股
        top_5_pct = df_sorted.head(top_5_pct_count)
        
        # 计算前5%成交额总和
        top_5_pct_amount = top_5_pct['amount'].sum()
        
        # 计算全部A股成交额总和
        total_amount = df_daily['amount'].sum()
        
        # 计算拥挤度
        if total_amount > 0:
            crowd_index = (top_5_pct_amount / total_amount) * 100
            return round(crowd_index, 2)
        else:
            return 0
    except Exception as e:
        print(f"计算拥挤度异常 ({trade_date}): {e}")
        return 0

def run_full_backfill():
    # === 配置区域 ===
    # 临时修改：只回填最近3个月数据（验证修复效果）
    from datetime import datetime, timedelta
    START_DATE = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')  # 最近3个月
    END_DATE   = datetime.now().strftime('%Y%m%d')
    # 全量回填时使用: START_DATE = '20190101'
    # =============

    print(f" 启动回填任务: {START_DATE} ~ {END_DATE} (最近3个月验证)")
    
    conn = sqlite3.connect(DB_PATH)
    intervals = get_quarters(START_DATE, END_DATE)
    
    # 获取全市场A股股票列表（用于非创业板指数）
    print(f"\n====== 获取全市场A股列表 ======")
    try:
        df_all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
        all_stocks_set = set(df_all_stocks['ts_code'].tolist())
        print(f"   -> 全市场A股数量: {len(all_stocks_set)}")
    except Exception as e:
        print(f"   [Error] 获取全市场股票列表失败: {e}")
        all_stocks_set = set()
    
    # 先获取所有需要计算拥挤度的日期
    all_dates = []
    for (s_date, e_date) in intervals:
        try:
            df_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=s_date, end_date=e_date)
            all_dates.extend(df_cal['cal_date'].values)
        except:
            pass
    
    # 去重并排序
    all_dates = sorted(list(set(all_dates)))
    
    # 先计算所有日期的拥挤度，存储到字典中，避免重复计算
    crowd_index_dict = {}
    print(f"\n====== 计算所有日期的拥挤度 ======")
    for i, trade_date in enumerate(all_dates):
        if i % 10 == 0:  # 每10天显示一次进度
            print(f"   计算拥挤度: {i+1}/{len(all_dates)} - {trade_date}")
        crowd_index = calculate_crowd_index(pro, trade_date)
        crowd_index_dict[trade_date] = crowd_index
    
    for index_name, index_code in INDEX_MAP.items():
        print(f"\n====== 处理指数: {index_name} ======")
        
        # 1. 获取指数自身价格 (用来做主图)
        try:
            print("   -> 拉取指数行情...")
            # 特殊处理中证2000指数
            if index_code == '932000.CSI':
                # 中证2000指数可能没有足够的历史数据，尝试获取它实际存在的时间范围
                print(f"   -> 特殊处理 {index_name} 指数...")
                # 先尝试获取最新数据，确定该指数是否存在
                df_latest = pro.index_daily(ts_code=index_code, start_date='20200101', end_date=END_DATE, limit=1)
                if df_latest.empty:
                    print(f"   [Skip] {index_name} 指数数据不存在，跳过处理")
                    continue
                # 如果存在，只获取它实际存在的时间范围
                df_idx_price = pro.index_daily(ts_code=index_code, start_date='20200101', end_date=END_DATE)
                if df_idx_price.empty:
                    print(f"   [Skip] {index_name} 指数无行情数据，跳过处理")
                    continue
            else:
                # 其他指数正常处理
                df_idx_price = pro.index_daily(ts_code=index_code, start_date=START_DATE, end_date=END_DATE)
            
            df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})
            print(f"   -> 成功获取 {len(df_idx_price)} 条指数行情数据")
        except Exception as e:
            print(f"   [Error] 指数行情拉取失败: {e}")
            continue

        # 2. 按季度循环处理
        for (s_date, e_date) in intervals:
            try:
                df_breadth = process_chunk(index_name, index_code, s_date, e_date, all_stocks_set)
                
                if not df_breadth.empty:
                    # 合并指数价格
                    df_final = pd.merge(df_breadth, df_idx_price, on='trade_date', how='inner')
                    
                    # 算百分比
                    df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
                    df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
                    df_final['pct_turnover_lt_3'] = (df_final['turnover_lt_3_count'] / df_final['total_count']) * 100
                    df_final['pct_turnover_gt_5'] = (df_final['turnover_gt_5_count'] / df_final['total_count']) * 100
                    
                    # 入库
                    data_tuples = []
                    for _, row in df_final.iterrows():
                        trade_date = row['trade_date']
                        # 获取预先计算好的拥挤度，如果没有则为0.0
                        crowd_index = crowd_index_dict.get(trade_date, 0.0)
                        data_tuples.append((
                            trade_date, index_code, index_name,
                            row['idx_close'], row['pct_above_ma20'], row['pct_down_3days'], crowd_index,
                            row['pct_turnover_lt_3'], row['pct_turnover_gt_5']
                        ))
                    
                    c = conn.cursor()
                    c.executemany('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?,?,?)', data_tuples)
                    conn.commit()
                    print(f"      [√] 已入库 {len(df_final)} 天数据")
                
            except Exception as e:
                print(f"      [!!!] 区间处理异常: {e}")
                time.sleep(5) # 出错多歇会

    conn.close()
    print("\n🎉 全量历史回填完成！")

if __name__ == "__main__":
    run_full_backfill()