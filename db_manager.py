import sqlite3
import pandas as pd
import sys
import os

# 直接定义DB_PATH，避免导入冲突
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'stock_data.db')

class DBManager:
    def __init__(self):
        self.db_path = DB_PATH
        self._init_tables()

    def get_conn(self):
        return sqlite3.connect(self.db_path, timeout=30.0)

    def _init_tables(self):
        conn = self.get_conn()
        # 开启 WAL 模式 (大幅提升并发性能，允许同时读写)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
        except:
            pass
            
        c = conn.cursor()
        
        # 1. 日内节点 (原有)
        c.execute('''CREATE TABLE IF NOT EXISTS daily_nodes (
            trade_date TEXT PRIMARY KEY,
            node_open REAL, node_15 REAL, node_30 REAL, 
            node_60 REAL, node_lunch REAL, node_close REAL
        )''')
        
        # 2. 宏观宽度 (原有-用于大盘指数)
        c.execute('''CREATE TABLE IF NOT EXISTS market_breadth (
            trade_date TEXT,
            index_code TEXT,
            index_name TEXT,
            close REAL,
            pct_above_ma20 REAL,
            pct_down_3days REAL,
            PRIMARY KEY (trade_date, index_code)
        )''')
        
        # 检查是否需要添加crowd_index字段
        try:
            c.execute("PRAGMA table_info(market_breadth)")
            columns = [col[1] for col in c.fetchall()]
            if 'crowd_index' not in columns:
                c.execute("ALTER TABLE market_breadth ADD COLUMN crowd_index REAL DEFAULT 0.0")
            if 'pct_turnover_lt_3' not in columns:
                c.execute("ALTER TABLE market_breadth ADD COLUMN pct_turnover_lt_3 REAL DEFAULT 0.0")
            if 'pct_turnover_gt_5' not in columns:
                c.execute("ALTER TABLE market_breadth ADD COLUMN pct_turnover_gt_5 REAL DEFAULT 0.0")
        except Exception as e:
            print(f"添加字段时出错: {e}")

        # 3. [新增] 板块宽度表 (用于申万行业)
        # level: 'L1' or 'L2'
        c.execute('''CREATE TABLE IF NOT EXISTS sector_breadth (
            trade_date TEXT,
            sector_name TEXT,
            level TEXT, 
            pct_above_ma20 REAL,
            PRIMARY KEY (trade_date, sector_name)
        )''')

        # 4. [新增] ETF 日线表 (用于策略计算，支持增量)
        c.execute('''CREATE TABLE IF NOT EXISTS etf_daily (
            ts_code TEXT,
            trade_date TEXT,
            open REAL, high REAL, low REAL, close REAL, vol REAL, adj_factor REAL,
            PRIMARY KEY (ts_code, trade_date)
        )''')

        # 检查是否需要添加adj_factor字段
        try:
            c.execute("PRAGMA table_info(etf_daily)")
            columns = [col[1] for col in c.fetchall()]
            if 'adj_factor' not in columns:
                c.execute("ALTER TABLE etf_daily ADD COLUMN adj_factor REAL DEFAULT 1.0")
        except Exception as e:
            print(f"添加adj_factor字段时出错: {e}")

        # 5. [新增] 期指持仓历史表 (用于期指监控历史分析)
        c.execute('''CREATE TABLE IF NOT EXISTS futures_holdings_history (
            trade_date TEXT,
            variety TEXT,
            symbol TEXT,
            net_long INTEGER,
            net_short INTEGER,
            change_net INTEGER,
            PRIMARY KEY (trade_date, variety, symbol)
        )''')

        conn.commit()
        conn.close()

    # --- 通用保存方法 ---
    def save_df(self, df, table_name, if_exists='append'):
        if df.empty: return
        conn = self.get_conn()
        try:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        except Exception as e:
            print(f"DB Save Error ({table_name}): {e}")
        finally:
            conn.close()

    def get_latest_date(self, table_name, code_col=None, code_val=None):
        """获取某表某代码的最新日期"""
        conn = self.get_conn()
        sql = f"SELECT MAX(trade_date) FROM {table_name}"
        if code_col and code_val:
            sql += f" WHERE {code_col}='{code_val}'"
        res = conn.execute(sql).fetchone()
        conn.close()
        return res[0] if res else None

db = DBManager()