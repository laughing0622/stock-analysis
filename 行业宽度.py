import tushare as ts
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import pandas as pd
import datetime
import warnings
import time

import sys
print("当前使用的 Python 版本: ", sys.version)
print("当前使用的 Python 路径: ", sys.executable)
# ---------------- 配置部分 ----------------
ts.set_token('5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e') # 替换Token
pro = ts.pro_api()

warnings.filterwarnings('ignore')
plt.rcParams['font.sans-serif'] = ['SimHei']  
plt.rcParams['axes.unicode_minus'] = False  

# ---------------- 数据获取部分 (保持稳健版) ----------------

def get_sw_industry_map():
    print("正在获取申万2021一级行业列表...")
    try:
        # 必须使用 SW2021 才能匹配图2的行业
        df_ind_list = pro.index_classify(level='L1', src='SW2021')
    except Exception as e:
        print(f"获取行业列表失败: {e}")
        return pd.Series()
        
    stock_industry_dict = {}
    for i, row in df_ind_list.iterrows():
        ind_code = row['index_code']
        ind_name = row['industry_name']
        for attempt in range(3):
            try:
                members = pro.index_member(index_code=ind_code, is_new='Y')
                if not members.empty:
                    for _, m_row in members.iterrows():
                        stock_industry_dict[m_row['con_code']] = ind_name
                break 
            except:
                time.sleep(0.3)
    print(f"行业映射构建完成，覆盖 {len(stock_industry_dict)} 只股票。")
    return pd.Series(stock_industry_dict)

def get_market_width_data(target_end_date, count):
    # 日期修正
    today = datetime.date.today()
    if target_end_date > today:
        target_end_date = today
        
    end_str = target_end_date.strftime('%Y%m%d')
    start_lookback = target_end_date - datetime.timedelta(days=120)
    start_str = start_lookback.strftime('%Y%m%d')
    
    print(f"1. 获取日历: {start_str} - {end_str}")
    df_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_str, end_date=end_str)
    if df_cal.empty: return None
    
    trade_days = sorted(df_cal['cal_date'].tolist())
    real_start_date = trade_days[0]
    real_end_date = trade_days[-1]

    # 获取股票
    df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
    stock_list = df_basic['ts_code'].tolist()
    
    # 分批获取行情 (chunk_size=40 防止数据丢失)
    chunk_size = 40 
    all_data = []
    print(f"2. 开始分批拉取数据，共 {len(stock_list)} 只股票...")
    
    for i in range(0, len(stock_list), chunk_size):
        subset = stock_list[i:i+chunk_size]
        ts_codes = ",".join(subset)
        try:
            df_daily = pro.daily(ts_code=ts_codes, start_date=real_start_date, end_date=real_end_date, fields='ts_code,trade_date,close')
            df_adj = pro.adj_factor(ts_code=ts_codes, start_date=real_start_date, end_date=real_end_date, fields='ts_code,trade_date,adj_factor')
            if not df_daily.empty and not df_adj.empty:
                df_merge = pd.merge(df_daily, df_adj, on=['ts_code', 'trade_date'])
                df_merge['hfq_close'] = df_merge['close'] * df_merge['adj_factor']
                all_data.append(df_merge[['ts_code', 'trade_date', 'hfq_close']])
        except:
            pass
        if i > 0 and i % 2000 == 0: print(f"   已处理 {i} 只...")

    if not all_data: return None
    df_total = pd.concat(all_data)
    
    # 计算
    print("3. 计算均线与行业宽度...")
    df_close = df_total.pivot(index='ts_code', columns='trade_date', values='hfq_close')
    df_ma20 = df_close.rolling(window=20, axis=1).mean()
    
    # 截取最后 count 天
    available_days = sorted(df_close.columns)
    target_days = available_days[-count:] if len(available_days) >= count else available_days
    
    df_bias = (df_close[target_days] > df_ma20[target_days])
    
    # 行业聚合
    s_stk_2_ind = get_sw_industry_map()
    df_bias['industry'] = df_bias.index.map(s_stk_2_ind)
    df_bias.dropna(subset=['industry'], inplace=True)
    
    # 算出百分比
    df_ratio = (df_bias.groupby('industry').mean() * 100).round().astype(int)
    
    # 注意：这里不计算全市场平均，而是稍后在绘图时计算“合计”
    return df_ratio.T # Index=Date, Cols=Industry

# ---------------- 绘图部分 (完全复刻图2布局) ----------------

def show_split_style_heatmap(df):
    """
    仿照图2风格：左侧行业热力图 + 右侧合计列
    """
    if df is None or df.empty:
        print("无数据")
        return

    # 1. 数据预处理
    # 日期倒序
    df = df.iloc[::-1]
    
    # 格式化日期：20251124 -> 11-24
    try:
        df.index = [pd.to_datetime(d).strftime('%m-%d') for d in df.index]
    except:
        pass

    # 计算右侧的“合计”列 (所有行业宽度的总和，模拟图2的大数值)
    # 或者如果你想要全市场平均值，就用 df.mean(axis=1)
    s_total = df.sum(axis=1) 
    
    # 排序：按最新一天(第一行)的数值，对行业列进行降序排列
    last_day_vals = df.iloc[0]
    sorted_cols = last_day_vals.sort_values(ascending=False).index.tolist()
    df_main = df[sorted_cols]

    # 2. 布局设置
    # 使用 GridSpec 将画布分为两部分：左边(主图)占 15 份，右边(合计)占 1 份
    row_count = len(df)
    # 动态高度，保证字不挤
    fig_height = max(8, row_count * 0.5) 
    fig = plt.figure(figsize=(22, fig_height)) # 宽度设大一点
    
    gs = gridspec.GridSpec(1, 16, figure=fig, wspace=0.05) # wspace控制中间缝隙
    ax_main = fig.add_subplot(gs[0, 0:15])
    ax_total = fig.add_subplot(gs[0, 15])

    # 3. 颜色方案
    # 图2风格：青色(低) -> 红色(高)。这里用 RdBu_r (红蓝反转)，Red是高，Blue是低
    # 或者用 sns.diverging_palette(220, 10, as_cmap=True) 模拟青红
    cmap_style = sns.diverging_palette(220, 10, as_cmap=True) 

    # 4. 绘制左侧主图 (行业)
    sns.heatmap(df_main, 
                annot=True, fmt="d", 
                cmap=cmap_style, vmin=0, vmax=100,
                cbar=False, 
                linewidths=0.5, linecolor='white',
                ax=ax_main)
    
    # 主图 X 轴调整 (放在顶部，垂直显示)
    ax_main.xaxis.tick_top()
    ax_main.xaxis.set_label_position('top')
    # 关键：rotation=90 垂直显示，解决重叠问题
    ax_main.set_xticklabels(ax_main.get_xticklabels(), rotation=90, fontsize=12)
    ax_main.set_xlabel("")
    ax_main.set_yticklabels(ax_main.get_yticklabels(), rotation=0, fontsize=11)

    # 5. 绘制右侧合计图
    # 将 Series 转为 DataFrame 才能画热力图
    df_total_plot = s_total.to_frame(name='合计')
    
    # 合计列数值很大(0~3100)，需要单独的色阶，或者直接用单色
    sns.heatmap(df_total_plot, 
                annot=True, fmt="d", 
                cmap=cmap_style, # 使用相同的色系，但数值范围不同，会自动归一化
                cbar=False,
                linewidths=0.5, linecolor='white',
                yticklabels=False, # 右侧不显示日期，对齐左侧即可
                ax=ax_total)
    
    # 合计列 X 轴调整
    ax_total.xaxis.tick_top()
    ax_total.set_xticklabels(['合计'], rotation=0, fontsize=12, fontweight='bold')
    ax_total.set_xlabel("")

    # 6. 最终调整
    # 调整顶部边距，留出空间给垂直的文字
    plt.subplots_adjust(top=0.85, bottom=0.05, left=0.08, right=0.95)
    
    # 增加一个总标题
    fig.suptitle("申万一级行业宽度", y=0.98, fontsize=18)
    
    plt.show()

if __name__ == '__main__':
    # 设置为今天 (代码会自动获取过去120天数据来计算)
    # 如果是盘中，Tushare可能有延迟，建议盘后运行
    target_date = datetime.date.today()
    
    # 只需要最近 20 天的结果
    data = get_market_width_data(target_date, 50)
    
    show_split_style_heatmap(data)