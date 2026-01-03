import tushare as ts
import pandas as pd
import datetime

# --- 1. 配置 ---
ts.set_token('5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e')
pro = ts.pro_api()

NAME_MAP = {
    'IF': 'IF(沪深300)',
    'IC': 'IC(中证500)',
    'IM': 'IM(中证1000)',
    'IH': 'IH(上证50)'
}

# --- 2. 基础函数 ---

def get_smart_date():
    """获取日期逻辑"""
    now = datetime.datetime.now()
    current_hour = now.hour
    today_str = now.strftime('%Y%m%d')
    start_search = (now - datetime.timedelta(days=60)).strftime('%Y%m%d')
    
    print(f"系统时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        cal_df = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_search, end_date=today_str, fields='cal_date')
        cal_df = cal_df.sort_values('cal_date')
        trade_dates = cal_df['cal_date'].tolist()
        if len(trade_dates) < 5: return None, None
        
        if today_str in trade_dates:
            target_idx = -1 if current_hour >= 17 else -2
        else:
            target_idx = -1
        return trade_dates[target_idx], trade_dates[target_idx - 1]
    except:
        return None, None

def get_contract_info(variety_list):
    """
    获取合约的基础信息（主要是交割日）
    返回字典: {'IF2512': '20251220', ...}
    """
    info_map = {}
    try:
        # 获取所有正在交易或刚退市的合约基础信息
        # 这里的逻辑稍微简化，拉取CFFEX所有合约信息，然后本地匹配
        df = pro.fut_basic(exchange='CFFEX', fut_type='1', fields='symbol,delist_date')
        for index, row in df.iterrows():
            info_map[row['symbol']] = row['delist_date']
    except Exception as e:
        print(f"获取合约信息失败: {e}")
    return info_map

def get_full_structure(date_str, variety_list):
    """
    获取某日的全结构持仓数据
    返回结构: { 'IF': { 'total': -100, 'contracts': [{'symbol': 'IF2512', 'net': -50}, ...] } }
    """
    data = {}
    try:
        df = pro.fut_holding(trade_date=date_str, exchange='CFFEX')
        if df.empty: return {}

        for v in variety_list:
            df_v = df[df['symbol'].str.contains(f'^{v}')]
            if df_v.empty: continue
            
            # 1. 筛选中信
            citic = df_v[df_v['broker'].str.contains('中信', na=False)]
            
            # 2. 计算各合约净单
            contract_details = []
            
            # 既然df_v包含了该品种所有合约，我们直接遍历其中的 unique symbol
            all_symbols = df_v['symbol'].unique()
            
            total_net = 0
            
            for sym in all_symbols:
                # 找到中信在这个合约上的记录
                record = citic[citic['symbol'] == sym]
                net = 0
                if not record.empty:
                    net = record['long_hld'].sum() - record['short_hld'].sum()
                
                contract_details.append({
                    'symbol': sym,
                    'net': int(net)
                })
                total_net += net
            
            data[v] = {
                'total_net': int(total_net),
                'contracts': contract_details
            }
    except:
        pass
    return data

# --- 3. 主程序 ---

if __name__ == '__main__':
    target_date, prev_date = get_smart_date()
    if not target_date: exit()
    
    print(f"\n 深度透视: {target_date} (对比: {prev_date})")
    print("正在拉取合约交割日信息...")
    varieties = ['IF', 'IC', 'IM', 'IH']
    delist_map = get_contract_info(varieties)
    
    data_now = get_full_structure(target_date, varieties)
    data_prev = get_full_structure(prev_date, varieties)
    
    print("\n" + "="*80)
    print(f"【中信期指·全期限结构监控】")
    print("="*80)

    for v in varieties:
        if v not in data_now or v not in data_prev: continue
        
        # 准备打印表头
        v_name = NAME_MAP.get(v, v)
        total_change = data_now[v]['total_net'] - data_prev[v]['total_net']
        
        print(f"\n {v_name} | 总净单: {data_now[v]['total_net']} | 总变动: {total_change:+d}")
        print("-" * 65)
        print(f"{'合约代码':<10} {'交割日期':<12} {'今日净单':<10} {'较昨日变动':<10} {'状态'}")
        print("-" * 65)
        
        # 获取所有涉及的合约代码（并集），并按交割日排序
        now_contracts = {item['symbol']: item['net'] for item in data_now[v]['contracts']}
        prev_contracts = {item['symbol']: item['net'] for item in data_prev[v]['contracts']}
        all_symbols = list(set(now_contracts.keys()) | set(prev_contracts.keys()))
        
        # 排序：按合约代码里的数字排序 (IF2512 < IF2603)
        all_symbols.sort()
        
        for sym in all_symbols:
            delist_date = delist_map.get(sym, "未知")
            net_now = now_contracts.get(sym, 0)
            net_prev = prev_contracts.get(sym, 0)
            change = net_now - net_prev
            
            # 判断状态
            if change == 0 and net_now == 0: continue # 没仓位也没变动的不显示
            
            status = ""
            if abs(change) > 500:
                if change > 0: status = "大幅平空/加多"
                else: status = "大幅加空"
            elif abs(change) > 100:
                 if change > 0: status = "平空"
                 else: status = "加空"
            
            print(f"{sym:<10} {delist_date:<12} {net_now:<10} {change:<+10d} {status}")

    print("\n" + "="*80)
    print("解读提示：")
    print("1. 观察 [旧合约] 是否大幅平仓（变动为正）。")
    print("2. 观察 [新合约] 是否等量加仓（变动为负）。")
    print("3. 如果 [总变动] 为正，说明移仓过程中【丢弃】了部分空单 -> 真实看多。")