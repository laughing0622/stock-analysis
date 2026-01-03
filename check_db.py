import sqlite3
import os

# 获取数据库文件的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'stock_data.db')

print(f"数据库路径: {DB_PATH}")
print(f"数据库是否存在: {os.path.exists(DB_PATH)}")

# 连接数据库
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

try:
    # 检查market_breadth表结构
    cursor.execute("PRAGMA table_info(market_breadth)")
    columns = cursor.fetchall()
    print("\n表结构:")
    for col in columns:
        print(f"  {col[1]} ({col[2]})")
    
    # 检查数据行数
    cursor.execute("SELECT COUNT(*) FROM market_breadth")
    row_count = cursor.fetchone()[0]
    print(f"\n数据行数: {row_count}")
    
    # 检查最新5条数据
    print("\n最新5条数据:")
    cursor.execute("SELECT trade_date, index_name, pct_down_3days, pct_above_ma20, crowd_index FROM market_breadth ORDER BY trade_date DESC LIMIT 5")
    rows = cursor.fetchall()
    for row in rows:
        print(f"  日期: {row[0]}, 指数: {row[1]}, 恐慌情绪: {row[2]:.1f}%, 市场宽度: {row[3]:.1f}%, 拥挤度: {row[4]:.1f}%")
    
    # 检查沪深300的最新数据
    print("\n沪深300最新数据:")
    cursor.execute("SELECT trade_date, close, pct_down_3days, pct_above_ma20, crowd_index FROM market_breadth WHERE index_name='沪深300' ORDER BY trade_date DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        print(f"  日期: {row[0]}, 点位: {row[1]:.2f}, 恐慌情绪: {row[2]:.1f}%, 市场宽度: {row[3]:.1f}%, 拥挤度: {row[4]:.1f}%")
    else:
        print("  无沪深300数据")
        
finally:
    # 关闭连接
    conn.close()