import sqlite3
import pandas as pd
from data_engine import engine
from datetime import datetime, timedelta

# 测试日期：使用今天或最近的交易日
test_date = datetime.now().strftime('%Y%m%d')
print(f"\n====== 测试修复结果 ======")
print(f"测试日期: {test_date}")

# 测试1：验证calculate_market_indicators函数
print(f"\n1. 测试calculate_market_indicators函数")
print(f"   测试指数: 中证2000")
index_name = "中证2000"
index_code = "932000.CSI"

pct_above_ma20, pct_down_3days = engine.calculate_market_indicators(index_code, index_name, test_date)
print(f"   市场宽度 (pct_above_ma20): {pct_above_ma20:.2f}%")
print(f"   恐慌情绪 (pct_down_3days): {pct_down_3days:.2f}%")

# 测试2：验证calculate_crowd_index函数
print(f"\n2. 测试calculate_crowd_index函数")
crowd_index = engine.calculate_crowd_index(test_date)
print(f"   拥挤度: {crowd_index:.2f}%")

# 测试3：验证update_today_breadth函数
print(f"\n3. 测试update_today_breadth函数")
engine.update_today_breadth()

# 测试4：检查数据库中的数据
print(f"\n4. 检查数据库中的修复后数据")
conn = sqlite3.connect('data/stock_data.db')

# 检查最新一天的数据
query = f"SELECT index_name, trade_date, close, pct_above_ma20, pct_down_3days, crowd_index FROM market_breadth WHERE trade_date = '{test_date}'"
df_latest = pd.read_sql(query, conn)
print(f"   最新一天 ({test_date}) 数据:")
print(df_latest)

# 检查中证2000的市场宽度是否正常
print(f"\n5. 检查中证2000的市场宽度数据")
query_csi2000 = f"SELECT trade_date, pct_above_ma20, pct_down_3days, crowd_index FROM market_breadth WHERE index_name = '中证2000' ORDER BY trade_date DESC LIMIT 5"
df_csi2000 = pd.read_sql(query_csi2000, conn)
print(f"   中证2000最近5天数据:")
print(df_csi2000)

# 检查其他指数的拥挤度数据
print(f"\n6. 检查各指数的拥挤度数据")
query_crowd = f"SELECT index_name, trade_date, crowd_index FROM market_breadth WHERE trade_date = '{test_date}'"
df_crowd = pd.read_sql(query_crowd, conn)
print(f"   各指数拥挤度数据 ({test_date}):")
print(df_crowd)

# 检查数据质量：是否有0值
print(f"\n7. 数据质量检查")
for index_name in df_latest['index_name'].unique():
    index_data = df_latest[df_latest['index_name'] == index_name].iloc[0]
    has_zero = False
    
    if index_data['pct_above_ma20'] == 0:
        print(f"   ⚠️ {index_name}: 市场宽度为0")
        has_zero = True
    if index_data['pct_down_3days'] == 0:
        print(f"   ⚠️ {index_name}: 恐慌情绪为0")
        has_zero = True
    if index_data['crowd_index'] == 0:
        print(f"   ⚠️ {index_name}: 拥挤度为0")
        has_zero = True
    
    if not has_zero:
        print(f"   ✅ {index_name}: 所有数据正常")

conn.close()
print(f"\n====== 测试完成 ======")
