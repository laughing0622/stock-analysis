import pandas as pd
from database.connector import db_manager

class QuantDataAPI:
    def __init__(self):
        self.engine = db_manager.get_engine()

    def get_stock_history(self, ts_code, start_date, end_date, adj='qfq'):
        """
        获取复权后的个股历史数据
        :param adj: 'qfq' (前复权), 'hfq' (后复权), None (不复权)
        """
        sql = f"""
        SELECT * FROM data_daily_quote 
        WHERE ts_code='{ts_code}' AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """
        df = pd.read_sql(sql, self.engine)
        if df.empty: return df

        if adj:
            factor = df['adj_factor']
            latest_factor = factor.iloc[-1]
            
            cols = ['open', 'high', 'low', 'close']
            if adj == 'qfq':
                for col in cols:
                    df[col] = df[col] * factor / latest_factor
            elif adj == 'hfq':
                for col in cols:
                    df[col] = df[col] * factor
                    
        return df

    def get_index_components(self, index_code, date):
        """获取某日指数成分股（Point-in-Time）"""
        # 寻找 <= date 的最新成分股日期
        sql = f"""
        SELECT * FROM data_index_weight 
        WHERE index_code='{index_code}' 
        AND trade_date = (
            SELECT MAX(trade_date) FROM data_index_weight 
            WHERE index_code='{index_code}' AND trade_date <= '{date}'
        )
        """
        return pd.read_sql(sql, self.engine)