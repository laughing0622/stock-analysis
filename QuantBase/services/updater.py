import pandas as pd
import time
from datetime import datetime, timedelta
from database.connector import db_manager
from fetchers.tushare_client import TushareClient
from config.settings import START_DATE_DEFAULT, CORE_INDICES

class DataUpdater:
    def __init__(self):
        self.engine = db_manager.get_engine()
        self.ts_client = TushareClient()

    def get_last_date(self, table_name, date_col='trade_date'):
        """获取数据库中最新日期"""
        try:
            sql = f"SELECT MAX({date_col}) FROM {table_name}"
            df = pd.read_sql(sql, self.engine)
            return df.iloc[0, 0]
        except:
            return None

    def update_stock_basic(self):
        print(">>> 更新股票基础信息...")
        df = self.ts_client.get_stock_basic()
        if not df.empty:
            # 基础信息推荐全量替换
            # 注意：SQLite 不支持 Upsert 语法简单的替换，这里简单处理为先删后插或Replace
            # 生产环境建议使用 Upsert
            df.to_sql('meta_stock_basic', self.engine, if_exists='replace', index=False)
            print(f"完成，共 {len(df)} 条。")

    def update_daily_quotes(self):
        print(">>> 开始增量更新日线行情...")
        last_date = self.get_last_date('data_daily_quote') or START_DATE_DEFAULT
        today = datetime.now().strftime('%Y%m%d')
        
        # 获取交易日历
        cal_df = self.ts_client.get_trade_cal(last_date, today)
        if cal_df.empty: return

        dates = cal_df['cal_date'].tolist()
        # 如果数据库已有 last_date，从下一天开始，避免重复
        if last_date in dates:
            dates = [d for d in dates if d > last_date]

        for date in dates:
            print(f"正在下载: {date} ...")
            # 1. 获取行情
            df_daily = self.ts_client.get_daily(date)
            # 2. 获取复权因子
            df_adj = self.ts_client.get_adj_factor(date)
            
            if not df_daily.empty:
                # 合并复权因子
                if not df_adj.empty:
                    df_merge = pd.merge(df_daily, df_adj[['ts_code', 'adj_factor']], on='ts_code', how='left')
                    df_merge['adj_factor'] = df_merge['adj_factor'].fillna(1.0) # 防止新股无因子
                else:
                    df_merge = df_daily
                    df_merge['adj_factor'] = 1.0

                # 写入数据库
                df_merge.to_sql('data_daily_quote', self.engine, if_exists='append', index=False, chunksize=2000)
            
	    # 3. 顺便更新指数日线 (按代码循环获取)
            for idx_code in CORE_INDICES.keys():
                # 修正点：这里必须传入 ts_code=idx_code
                df_idx = self.ts_client.get_index_daily(date=date, ts_code=idx_code)
                
                if not df_idx.empty:
                    # 直接写入，不需要再做 isin 筛选，因为请求的就是指定指数
                    df_idx.to_sql('data_index_daily', self.engine, if_exists='append', index=False)
            
            # 频控
            time.sleep(0.3)
    
# 替换 services/updater.py 中的 update_index_weights 方法
    def update_index_weights(self):
        print(">>> 更新指数成分股权重...")
        today_year = datetime.now().year
        today_str = datetime.now().strftime('%Y%m%d')

        for idx_code, idx_name in CORE_INDICES.items():
            # 1. 获取该指数在库里的最新时间，确定开始年份
            sql = f"SELECT MAX(trade_date) FROM data_index_weight WHERE index_code='{idx_code}'"
            last = pd.read_sql(sql, self.engine).iloc[0, 0]
            
            start_year = 2018 # 默认起始年份
            if last:
                # 如果有数据，从最新数据的年份开始检查（或者下一季度），为了简单起见，从最后年份开始
                start_year = int(last[:4])
            
            print(f"检查 {idx_name} ({idx_code})，从 {start_year} 年开始...")

            # 2. 按年循环下载，防止超过 Tushare 单次行数限制
            for year in range(start_year, today_year + 1):
                y_start = f"{year}0101"
                y_end = f"{year}1231"
                
                # 如果是最后一年，结束日期不能超过今天
                if y_end > today_str:
                    y_end = today_str
                
                # 如果开始时间比结束时间晚（比如已经是最新），跳过
                if last and y_start <= last:
                    # 如果这一年已经部分更新过，从 last 的下一天开始
                    if f"{year}" == last[:4]:
                         # 重新构造这一年的 start
                         dt_last = datetime.strptime(last, '%Y%m%d') + timedelta(days=1)
                         y_start = dt_last.strftime('%Y%m%d')
                    else:
                        # 还没到需要更新的年份（理论上 range 保证了不会进入这里，但为了保险）
                        pass

                if y_start > y_end:
                    continue

                try:
                    # print(f"  -> 下载 {year} 年数据 ({y_start}-{y_end})...")
                    df = self.ts_client.get_index_weight(idx_code, y_start, y_end)
                    
                    if not df.empty:
                        # 剔除重复数据（以防万一数据库里已有部分当月数据）
                        # 最好是利用数据库的主键约束，这里简单处理，直接 append，依赖数据库去重或忽略
                        # 如果是 SQLite/MySQL 且没设置 IGNORE，重复插入会报错。
                        # 建议：生产环境用 Upsert。这里我们假设是增量，直接插入。
                        
                        # 小技巧：为了避免主键冲突报错（如果重复跑），可以先判断
                        df.to_sql('data_index_weight', self.engine, if_exists='append', index=False, chunksize=2000)
                        print(f"  -> {year} 年写入 {len(df)} 条")
                    else:
                        print(f"  -> {year} 年无数据")
                        
                except Exception as e:
                    # 如果是重复主键错误，说明这一年数据已存在，可以忽略
                    if "IntegrityError" in str(e) or "UNIQUE constraint" in str(e):
                        print(f"  -> {year} 年数据已存在，跳过。")
                    else:
                        print(f"  -> {year} 年出错: {e}")
                
                # 稍微休息一下，保护接口
                time.sleep(0.5)
    def run_all(self):
        self.update_stock_basic()
        self.update_daily_quotes()
        self.update_index_weights()
        print("=== 所有更新完成 ===")