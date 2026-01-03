import sqlite3
import pandas as pd

conn = sqlite3.connect('data/stock_data.db')

# 检查最新一天(20251212)的详细数据
print("=== 20251212各指数详细数据 ===")
df_latest = pd.read_sql('SELECT * FROM market_breadth WHERE trade_date = "20251212"', conn)
print(df_latest[['index_name', 'trade_date', 'pct_above_ma20', 'pct_down_3days']])

# 检查上证指数2024-2025年的数据分布
print("\n=== 上证指数2024-2025年月度数据分布 ===")
df_sh = pd.read_sql('SELECT trade_date, close, pct_above_ma20, pct_down_3days FROM market_breadth WHERE index_name = "上证指数" AND trade_date >= "20240101"', conn)
df_sh['year_month'] = df_sh['trade_date'].astype(str).str[:6]  # YYYYMM
month_counts = df_sh['year_month'].value_counts().sort_index()
print(month_counts)

# 检查计算市场宽度和市场情绪的逻辑
print("\n=== 检查计算逻辑 ===")
from datetime import datetime, timedelta

def check_calculation_logic(index_name, trade_date):
    """检查市场宽度和市场情绪的计算逻辑"""
    print(f"\n检查 {index_name} {trade_date} 的计算逻辑:")
    
    # 计算需要的日期范围
    end_date = trade_date
    start_date = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=20)).strftime('%Y%m%d')
    print(f"需要的日期范围: {start_date} ~ {end_date}")
    
    # 检查成分股数据
    from data_engine import engine
    stock_list = engine._get_index_constituents(INDEX_MAP[index_name], trade_date)
    print(f"成分股数量: {len(stock_list)}")
    
    if len(stock_list) > 0:
        # 检查是否能获取到个股数据
        try:
            import tushare as ts
            from config import TS_TOKEN
            ts.set_token(TS_TOKEN)
            pro = ts.pro_api()
            
            # 只检查前10只成分股
            test_stocks = stock_list[:10]
            df_d = pro.daily(ts_code=','.join(test_stocks), start_date=start_date, end_date=end_date, fields='ts_code,trade_date,close,pct_chg')
            print(f"获取到 {len(df_d)} 条个股数据")
            
            if not df_d.empty:
                print("样本数据:")
                print(df_d.head())
        except Exception as e:
            print(f"获取个股数据失败: {e}")

# 从config导入INDEX_MAP
from config import INDEX_MAP

# 检查沪深300的计算逻辑
check_calculation_logic('沪深300', '20251212')

conn.close()
