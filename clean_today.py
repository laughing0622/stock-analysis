import sqlite3
import os

# 确保路径对
db_path = os.path.join('data', 'stock_data.db')
conn = sqlite3.connect(db_path)
c = conn.cursor()

# 删除今天的宽基数据
target_date = '20251210' # 根据你日志里的日期
c.execute(f"DELETE FROM market_breadth WHERE trade_date='{target_date}'")
conn.commit()

print(f"已删除 {target_date} 的数据，现在可以重新运行 run_daily.py 了。")
conn.close()