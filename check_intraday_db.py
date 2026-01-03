import sqlite3
import pandas as pd

conn = sqlite3.connect('data/stock_data.db')

print("=== 检查日内节点表 ===")
df = pd.read_sql("SELECT * FROM daily_nodes ORDER BY trade_date DESC LIMIT 5", conn)
print(df)

print("\n=== 表结构 ===")
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(daily_nodes)")
print(pd.DataFrame(cursor.fetchall(), columns=['cid','name','type','notnull','dflt_value','pk']))

conn.close()
