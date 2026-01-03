import tushare as ts
import pandas as pd
import datetime
import warnings
import time
import numpy as np
import os
import webbrowser

# 引入 plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ==========================================
# 1. 配置部分
# ==========================================
ts.set_token('5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e') 
pro = ts.pro_api()
warnings.filterwarnings('ignore')

# ==========================================
# 2. 板块定义
# ==========================================
SECTOR_LOW_RISK = ['银行', '石油石化', '煤炭', '钢铁', '公用事业']
SECTOR_HIGH_RISK = ['国防军工', '计算机', '通信', '电力设备', '食品饮料', '医药生物', '电子', '汽车']
INDEX_BROAD = {
    '上证50': ['000016.SH'],
    '创业板指': ['399006.SZ'],
    '科创50': ['000688.SH']
}
INDEX_STYLE = {
    '微盘股': ['DYNAMIC_CALC', '399303.SZ'], 
    '高股息': ['000922.CSI', '000015.SH']
}
DISPLAY_ORDER = SECTOR_LOW_RISK + SECTOR_HIGH_RISK + list(INDEX_BROAD.keys()) + list(INDEX_STYLE.keys())

# ==========================================
# 3. 数据获取逻辑
# ==========================================

def get_real_micro_cap_stocks(ref_date_str=None):
    """动态计算微盘股"""
    if not ref_date_str: ref_date_str = datetime.date.today().strftime('%Y%m%d')
    print(f"   [计算] 正在动态计算微盘股 (市值最小Top400)...")
    try:
        df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,list_date')
        df_daily = pd.DataFrame()
        # 往回找最近的一个有数据的交易日
        for i in range(10): 
            d = (pd.to_datetime(ref_date_str) - pd.Timedelta(days=i)).strftime('%Y%m%d')
            try:
                df_daily = pro.daily_basic(ts_code='', trade_date=d, fields='ts_code,total_mv')
                if not df_daily.empty: break
            except: continue
        
        if df_daily.empty: return []
        df_merge = pd.merge(df_basic, df_daily, on='ts_code')
        df_merge = df_merge[~df_merge['name'].str.contains('ST')]
        cutoff = (pd.to_datetime(ref_date_str) - pd.Timedelta(days=180)).strftime('%Y%m%d')
        df_merge = df_merge[df_merge['list_date'] < cutoff]
        df_sorted = df_merge.sort_values(by='total_mv', ascending=True)
        return df_sorted.head(400)['ts_code'].tolist()
    except: return []

def get_stocks_from_index_smart(codes, name):
    """智能获取指数成分"""
    for code in codes:
        if code == 'DYNAMIC_CALC': continue
        try:
            df = pro.index_member(index_code=code, is_new='Y')
            if not df.empty: return df['con_code'].tolist()
        except: pass
        try:
            today = datetime.date.today()
            start_dt = (today - datetime.timedelta(days=60)).strftime('%Y%m%d')
            df_w = pro.index_weight(index_code=code, start_date=start_dt, end_date=today.strftime('%Y%m%d'))
            if not df_w.empty:
                latest = df_w['trade_date'].max()
                return df_w[df_w['trade_date'] == latest]['con_code'].tolist()
        except: pass
    return []

def get_sector_members_map():
    """构建板块字典"""
    members_map = {}
    print("\n====== 步骤1：构建板块成分股列表 ======")
    
    # 申万
    try:
        df_ind_list = pro.index_classify(level='L1', src='SW2021')
        target_industries = set(SECTOR_LOW_RISK + SECTOR_HIGH_RISK)
        for i, row in df_ind_list.iterrows():
            if row['industry_name'] in target_industries:
                try:
                    df_m = pro.index_member(index_code=row['index_code'], is_new='Y')
                    if not df_m.empty: members_map[row['industry_name']] = df_m['con_code'].tolist()
                except: pass
    except: pass

    # 指数
    all_indices = {**INDEX_BROAD, **INDEX_STYLE}
    for name, codes in all_indices.items():
        if name == '微盘股':
            stocks = get_real_micro_cap_stocks()
            if stocks: members_map[name] = stocks
            continue
        
        stocks = get_stocks_from_index_smart(codes, name)
        if stocks: members_map[name] = stocks
            
    return members_map

def get_market_data_and_calc(target_end_date, count):
    today = datetime.date.today()
    if target_end_date > today: target_end_date = today
    end_str = target_end_date.strftime('%Y%m%d')
    
    # 【关键修复1】大幅增加回溯时间
    # 假设 count=180, 我们往回取 180*2.5 + 60 = 510 天
    # 这样能确保即使在非交易日很多的年份，也有足够的数据计算最开始那几天的MA20
    lookback_days = int((count * 2.5) + 60)
    start_lookback = target_end_date - datetime.timedelta(days=lookback_days)
    start_str = start_lookback.strftime('%Y%m%d')
    
    print(f"\n====== 步骤2：获取行情 ({start_str} - {end_str}) ======")
    print(f"   目标统计 {count} 天，实际拉取了 {lookback_days} 天的数据以保证精度...")
    
    df_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_str, end_date=end_str)
    if df_cal.empty: return None
    trade_days = sorted(df_cal['cal_date'].tolist())
    real_start_date, real_end_date = trade_days[0], trade_days[-1]

    sector_map = get_sector_members_map()
    all_target_stocks = set()
    for stocks in sector_map.values(): all_target_stocks.update(stocks)
    stock_list = list(all_target_stocks)
    
    if not stock_list: return None

    # 【关键修复2】减小 chunk_size
    # 因为时间跨度变长了，为了防止单次请求超过5000行限制，必须把每批次的股票数量减少
    chunk_size = 25 
    all_data = []
    print(f"\n   开始拉取 {len(stock_list)} 只股票行情 (分批)...")
    
    for i in range(0, len(stock_list), chunk_size):
        subset = stock_list[i:i+chunk_size]
        ts_codes = ",".join(subset)
        try:
            df_daily = pro.daily(ts_code=ts_codes, start_date=real_start_date, end_date=real_end_date, fields='ts_code,trade_date,close')
            df_adj = pro.adj_factor(ts_code=ts_codes, start_date=real_start_date, end_date=real_end_date, fields='ts_code,trade_date,adj_factor')
            
            if not df_daily.empty:
                # 修复复权因子缺失
                if not df_adj.empty:
                    df_merge = pd.merge(df_daily, df_adj, on=['ts_code', 'trade_date'], how='left')
                else:
                    df_merge = df_daily.copy()
                    df_merge['adj_factor'] = 1.0 
                
                df_merge.sort_values(by=['ts_code', 'trade_date'], inplace=True)
                df_merge['adj_factor'] = df_merge.groupby('ts_code')['adj_factor'].fillna(method='ffill')
                df_merge['adj_factor'] = df_merge['adj_factor'].fillna(1.0)
                
                df_merge['hfq_close'] = df_merge['close'] * df_merge['adj_factor']
                all_data.append(df_merge[['ts_code', 'trade_date', 'hfq_close']])
        except Exception as e: 
            pass
            
        if i > 0 and i % 500 == 0: print(f"   已处理 {i} 只...")

    if not all_data: return None
    df_total = pd.concat(all_data)
    
    print("\n====== 步骤3：计算宽度 ======")
    df_close = df_total.pivot(index='ts_code', columns='trade_date', values='hfq_close')
    
    # 计算 MA20
    df_ma20 = df_close.rolling(window=20, axis=1).mean()
    
    # 截取需要显示的日期
    available_days = sorted(df_close.columns)
    # 确保 target_days 完全在数据范围内，避免因数据不足导致的NaN
    if len(available_days) < count:
        print(f"警告：获取的数据天数 ({len(available_days)}) 小于请求天数 ({count})")
        target_days = available_days
    else:
        target_days = available_days[-count:]
    
    # 再次检查：确保 target_days 的第一天在 df_ma20 里不是全 NaN
    # 如果回溯不够，df_ma20的前19天是NaN。
    # 我们上面增加了2.5倍回溯，理论上 target_days 肯定在有效区间内
    
    df_bias_all = (df_close[target_days] > df_ma20[target_days])
    
    result_dict = {}
    for name in DISPLAY_ORDER:
        members = sector_map.get(name, [])
        valid = [s for s in members if s in df_bias_all.index]
        if valid:
            # 计算百分比并进行四舍五入 (Standard Rounding)
            # 原始：mean是 0~1 的小数 -> * 100 -> + 0.5 -> int截断
            val_series = df_bias_all.loc[valid].mean() * 100
            result_dict[name] = (val_series + 0.5).astype(int)
            
    return pd.DataFrame(result_dict)

# ==========================================
# 4. 绘图函数 (视觉优化版)
# ==========================================

def show_interactive_heatmap(df):
    if df is None or df.empty:
        print("无数据绘图")
        return

    # 1. 数据预处理
    df = df.iloc[::-1] # 日期倒序
    
    try:
        date_labels = [pd.to_datetime(d).strftime('%Y-%m-%d') for d in df.index]
    except:
        date_labels = df.index.tolist()
        
    s_total = df.sum(axis=1) 
    
    cols_to_plot = [c for c in DISPLAY_ORDER if c in df.columns]
    df_main = df[cols_to_plot]

    # 2. 创建图表
    fig = make_subplots(rows=1, cols=2, 
                        column_widths=[0.94, 0.06], 
                        shared_yaxes=True,
                        horizontal_spacing=0.01,
                        # 【视觉修复】移除子图标题，改用主标题，防止重叠
                        subplot_titles=("", "")) 

    # 颜色
    custom_colorscale = [
        [0.0, 'rgb(80, 140, 140)'], 
        [0.5, 'rgb(240, 240, 240)'], 
        [1.0, 'rgb(200, 60, 60)']
    ]

    # 左图
    fig.add_trace(
        go.Heatmap(
            z=df_main.values,
            x=cols_to_plot,
            y=date_labels,
            colorscale=custom_colorscale,
            zmin=0, zmax=100,
            text=df_main.values, 
            texttemplate="%{text}", 
            textfont={"size": 10},
            hoverongaps=False,
            hovertemplate="日期: %{y}<br>板块: %{x}<br>宽度: %{z}%<extra></extra>"
        ),
        row=1, col=1
    )

    # 右图 (合计)
    fig.add_trace(
        go.Heatmap(
            z=s_total.values.reshape(-1, 1), 
            x=['合计'],
            y=date_labels,
            colorscale=custom_colorscale, 
            text=s_total.values.reshape(-1, 1),
            texttemplate="%{text}",
            # 【视觉修复】强制文字为黑色，防止背景浅色时看不见
            textfont={"size": 11, "color": "black"}, 
            showscale=False, 
            hovertemplate="日期: %{y}<br>合计: %{z}<extra></extra>"
        ),
        row=1, col=2
    )

    # 3. 布局调整
    row_count = len(df)
    dynamic_height = max(800, row_count * 25)

    fig.update_layout(
        title={
            'text': f"全市场宽度扫描 (近 {row_count} 天)",
            'y': 0.98, # 标题位置微调
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top'
        },
        height=dynamic_height, 
        width=1300, 
        
        # 【视觉修复】大幅增加 Top Margin，防止标题和X轴标签重叠
        margin=dict(t=180, b=50, l=80, r=50),
        
        xaxis=dict(
            side='top', 
            tickangle=-45,
            title_text="" # 去除 "variable" 字样
        ), 
        xaxis2=dict(
            side='top',
            title_text="" # 去除 "variable" 字样
        ), 
        
        yaxis=dict(
            autorange="reversed", 
            type='category',     
            dtick=1              
        ),
    )
    
    print(">> 正在生成本地 HTML 文件...")
    file_name = "market_width_heatmap.html"
    
    # 1. 保存为静态 HTML 文件
    fig.write_html(file_name)
    
    # 2. 获取绝对路径并用浏览器打开
    file_path = os.path.abspath(file_name)
    webbrowser.open('file://' + file_path)
    
    print(f">> 图表已保存并打开: {file_path}")

# ==========================================
# 5. 执行入口
# ==========================================

if __name__ == '__main__':
    target_date = datetime.date.today()
    # 统计 180 天
    count_days = 180
    
    data = get_market_data_and_calc(target_date, count_days)
    show_interactive_heatmap(data)