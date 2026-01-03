import sqlite3
import pandas as pd
from config import DB_PATH

try:
    conn = sqlite3.connect(DB_PATH)
    
    # Check if columns exist
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(market_breadth)")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"Columns in market_breadth: {columns}")
    
    if 'pct_turnover_lt_3' not in columns or 'pct_turnover_gt_5' not in columns:
        print("[X] Columns pct_turnover_lt_3 or pct_turnover_gt_5 NOT FOUND!")
    else:
        print("[V] Columns found.")
        
        # Check data coverage
        query = """
        SELECT 
            index_name, 
            COUNT(*) as total_rows,
            COUNT(pct_turnover_lt_3) as lt_3_count,
            COUNT(pct_turnover_gt_5) as gt_5_count,
            MIN(trade_date) as start_date,
            MAX(trade_date) as end_date
        FROM market_breadth
        GROUP BY index_name
        """
        df = pd.read_sql_query(query, conn)
        print("\nData Coverage per Index:")
        print(df)
        
        # Check for zeros (might indicate default values if not updated)
        query_zeros = """
        SELECT 
            index_name,
            SUM(CASE WHEN pct_turnover_lt_3 = 0 THEN 1 ELSE 0 END) as lt_3_zeros,
            SUM(CASE WHEN pct_turnover_gt_5 = 0 THEN 1 ELSE 0 END) as gt_5_zeros
        FROM market_breadth
        GROUP BY index_name
        """
        df_zeros = pd.read_sql_query(query_zeros, conn)
        print("\nZero Values Count (Potential default values):")
        print(df_zeros)

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
