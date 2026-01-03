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
    print("\n1. 删除现有数据...")
    cursor.execute("DELETE FROM market_breadth")
    conn.commit()
    print("   ✅ 现有数据已删除")
    
    print("\n2. 重新运行数据回填...")
    # 运行全量回填
    from run_backfill import run_full_backfill
    run_full_backfill()
    
    print("\n3. 检查回填结果...")
    cursor.execute("SELECT COUNT(*) FROM market_breadth")
    row_count = cursor.fetchone()[0]
    print(f"   ✅ 回填完成，共 {row_count} 条数据")
    
    # 检查各指数的数据数量
    print("\n4. 各指数数据统计:")
    cursor.execute("SELECT index_name, COUNT(*) FROM market_breadth GROUP BY index_name")
    for row in cursor.fetchall():
        print(f"   - {row[0]}: {row[1]} 条")
    
    # 检查最新数据
    print("\n5. 最新数据:")
    cursor.execute("SELECT trade_date, index_name, pct_above_ma20, pct_down_3days, crowd_index FROM market_breadth ORDER BY trade_date DESC LIMIT 10")
    for row in cursor.fetchall():
        print(f"   - {row[0]} {row[1]}: 市场宽度={row[2]:.1f}% 恐慌情绪={row[3]:.1f}% 拥挤度={row[4]:.1f}%")
    
finally:
    conn.close()
    print("\n6. 操作完成")