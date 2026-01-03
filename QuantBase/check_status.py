# from database.connector import db_manager
# import pandas as pd

# def check_db_status():
#     engine = db_manager.get_engine()
    
#     print("=== 检查数据库最新数据日期 ===")
    
#     # 1. 检查日线行情最新日期
#     try:
#         daily_date = pd.read_sql("SELECT MAX(trade_date) FROM data_daily_quote", engine).iloc[0,0]
#         print(f"个股日线最新日期: {daily_date}")
#     except:
#         print("个股日线: 无数据")

#     # 2. 检查指数成分股最新日期
#     print("\n=== 指数成分股状态 ===")
#     indices = {
#         '000001.SH': '上证指数',
#         '000300.SH': '沪深300',
#         '000905.SH': '中证500',
#         '399006.SZ': '创业板指',
#         '000688.SH': '科创50'
#     }
    
#     for code, name in indices.items():
#         try:
#             sql = f"SELECT MAX(trade_date) FROM data_index_weight WHERE index_code='{code}'"
#             last_date = pd.read_sql(sql, engine).iloc[0,0]
#             print(f"{name} ({code}): 最新权重日期 -> {last_date}")
#         except:
#             print(f"{name}: 无数据")

# if __name__ == "__main__":
#     check_db_status()

# 文件名: clean_weights.py
from database.connector import db_manager
from sqlalchemy import text

def clean_index_weights():
    engine = db_manager.get_engine()
    print("正在清空指数成分股数据 (data_index_weight)...")
    
    with engine.connect() as conn:
        # 清空表中的所有数据
        conn.execute(text("DELETE FROM data_index_weight"))
        # 提交事务
        conn.commit()
        
    print("清空完成！现在请重新运行 python main.py update")

if __name__ == "__main__":
    clean_index_weights()