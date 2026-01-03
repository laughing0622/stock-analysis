import sys
from database.connector import db_manager
from services.updater import DataUpdater
from services.data_api import QuantDataAPI

def init_system():
    print("正在初始化数据库表结构...")
    db_manager.init_db()
    print("初始化完成。")

def update_data():
    updater = DataUpdater()
    updater.run_all()

def test_query():
    api = QuantDataAPI()
    print("测试查询：获取 000001.SZ 2023年数据(前复权)")
    df = api.get_stock_history('000001.SZ', '20230101', '20230201', adj='qfq')
    print(df.head())
    
    print("\n测试查询：获取 沪深300 在 2022-05-01 的成分股")
    df_idx = api.get_index_components('000300.SH', '20220501')
    print(df_idx.head())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py [init|update|test]")
    else:
        cmd = sys.argv[1]
        if cmd == 'init':
            init_system()
        elif cmd == 'update':
            update_data()
        elif cmd == 'test':
            test_query()