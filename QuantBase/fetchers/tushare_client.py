import tushare as ts
import time
import pandas as pd
from config.settings import TUSHARE_TOKEN

class TushareClient:
    def __init__(self):
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api()
        self.max_retries = 3

    def fetch_with_retry(self, api_name, **kwargs):
        """通用重试装饰器逻辑"""
        for i in range(self.max_retries):
            try:
                # 动态调用 API
                func = getattr(self.pro, api_name)
                df = func(**kwargs)
                return df
            except Exception as e:
                print(f"[Warning] {api_name} failed ({i+1}/{self.max_retries}): {e}")
                time.sleep(1 * (i + 1))
        return pd.DataFrame()

    def get_trade_cal(self, start, end):
        return self.fetch_with_retry('trade_cal', start_date=start, end_date=end, is_open='1')

    def get_stock_basic(self):
        # 列表必须分段取或者取全量，这里取全量上市的
        return self.fetch_with_retry('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date,market,is_hs')

    def get_daily(self, date):
        return self.fetch_with_retry('daily', trade_date=date)

    def get_adj_factor(self, date):
        return self.fetch_with_retry('adj_factor', trade_date=date)

    def get_index_daily(self, date, ts_code=''):
        """
        获取指数日线
        注意：index_daily 接口通常强制要求 ts_code
        """
        # 将 ts_code 传入 fetch_with_retry
        return self.fetch_with_retry('index_daily', trade_date=date, ts_code=ts_code)

    def get_index_weight(self, index_code, start, end):
        return self.fetch_with_retry('index_weight', index_code=index_code, start_date=start, end_date=end)