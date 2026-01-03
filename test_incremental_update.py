"""
测试增量更新功能
用于验证板块宽度和ETF策略的增量更新是否正常工作
"""
import time
from datetime import datetime
from data_engine import engine
import sqlite3

def test_sector_incremental():
    """测试板块宽度增量更新"""
    print("\n" + "="*60)
    print("测试 1: 板块宽度增量更新")
    print("="*60)
    
    # 连接数据库检查初始状态
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    
    # 获取更新前的数据量
    cursor.execute("SELECT COUNT(*) FROM sector_breadth")
    count_before = cursor.fetchone()[0]
    
    cursor.execute("SELECT MAX(trade_date) as max_date FROM sector_breadth")
    max_date_before = cursor.fetchone()[0]
    
    print(f"\n更新前状态:")
    print(f"  - 数据总量: {count_before} 条")
    print(f"  - 最新日期: {max_date_before}")
    
    conn.close()
    
    # 执行增量更新
    print(f"\n开始增量更新...")
    start_time = time.time()
    engine.update_sector_breadth(lookback_days=250, incremental=True)
    elapsed = time.time() - start_time
    
    # 检查更新后状态
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM sector_breadth")
    count_after = cursor.fetchone()[0]
    
    cursor.execute("SELECT MAX(trade_date) as max_date FROM sector_breadth")
    max_date_after = cursor.fetchone()[0]
    
    print(f"\n更新后状态:")
    print(f"  - 数据总量: {count_after} 条")
    print(f"  - 最新日期: {max_date_after}")
    print(f"  - 新增数据: {count_after - count_before} 条")
    print(f"  - 耗时: {elapsed:.2f} 秒")
    
    conn.close()
    
    # 验证结果
    if max_date_after and (max_date_after >= max_date_before or count_after > count_before):
        print("\n✅ 板块宽度增量更新测试通过")
    else:
        print("\n❌ 板块宽度增量更新测试失败")
    
    return True

def test_etf_incremental():
    """测试ETF策略增量更新"""
    print("\n" + "="*60)
    print("测试 2: ETF策略增量更新")
    print("="*60)
    
    # 连接数据库检查初始状态
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    
    # 获取更新前的数据量
    cursor.execute("SELECT COUNT(*) FROM etf_daily")
    count_before = cursor.fetchone()[0]
    
    cursor.execute("SELECT MAX(trade_date) as max_date FROM etf_daily")
    max_date_before = cursor.fetchone()[0]
    
    # 检查是否有adj_factor字段
    cursor.execute("PRAGMA table_info(etf_daily)")
    columns = [col[1] for col in cursor.fetchall()]
    has_adj_factor = 'adj_factor' in columns
    
    print(f"\n更新前状态:")
    print(f"  - 数据总量: {count_before} 条")
    print(f"  - 最新日期: {max_date_before}")
    print(f"  - adj_factor字段: {'存在' if has_adj_factor else '缺失'}")
    
    conn.close()
    
    # 执行增量更新
    print(f"\n开始增量更新...")
    start_time = time.time()
    engine.update_strategy_data(incremental=True)
    elapsed = time.time() - start_time
    
    # 检查更新后状态
    conn = sqlite3.connect('data/stock_data.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM etf_daily")
    count_after = cursor.fetchone()[0]
    
    cursor.execute("SELECT MAX(trade_date) as max_date FROM etf_daily")
    max_date_after = cursor.fetchone()[0]
    
    # 再次检查adj_factor字段
    cursor.execute("PRAGMA table_info(etf_daily)")
    columns = [col[1] for col in cursor.fetchall()]
    has_adj_factor_after = 'adj_factor' in columns
    
    print(f"\n更新后状态:")
    print(f"  - 数据总量: {count_after} 条")
    print(f"  - 最新日期: {max_date_after}")
    print(f"  - adj_factor字段: {'存在' if has_adj_factor_after else '缺失'}")
    print(f"  - 新增数据: {count_after - count_before} 条")
    print(f"  - 耗时: {elapsed:.2f} 秒")
    
    conn.close()
    
    # 验证结果
    if has_adj_factor_after and (max_date_after >= max_date_before or count_after >= count_before):
        print("\n✅ ETF策略增量更新测试通过")
    else:
        print("\n❌ ETF策略增量更新测试失败")
    
    return True

def test_full_vs_incremental():
    """对比全量和增量更新的性能差异"""
    print("\n" + "="*60)
    print("测试 3: 全量 vs 增量性能对比")
    print("="*60)
    
    print("\n提示: 此测试仅作参考，实际性能取决于网络和数据量")
    print("建议在数据库已有数据的情况下运行此测试")
    
    user_input = input("\n是否执行性能对比测试？(y/n): ")
    if user_input.lower() != 'y':
        print("跳过性能对比测试")
        return False
    
    # 测试增量更新速度
    print("\n--- 增量更新测试 ---")
    start = time.time()
    engine.update_sector_breadth(lookback_days=250, incremental=True)
    incremental_time = time.time() - start
    print(f"增量更新耗时: {incremental_time:.2f} 秒")
    
    # 提示用户全量更新会清空数据
    print("\n⚠️  警告: 全量更新将重新计算所有数据")
    confirm = input("是否继续执行全量更新测试？(y/n): ")
    if confirm.lower() != 'y':
        print("跳过全量更新测试")
        return False
    
    # 测试全量更新速度
    print("\n--- 全量更新测试 ---")
    start = time.time()
    engine.update_sector_breadth(lookback_days=250, incremental=False)
    full_time = time.time() - start
    print(f"全量更新耗时: {full_time:.2f} 秒")
    
    # 性能对比
    print("\n📊 性能对比结果:")
    print(f"  - 增量更新: {incremental_time:.2f} 秒")
    print(f"  - 全量更新: {full_time:.2f} 秒")
    print(f"  - 速度提升: {full_time / incremental_time:.2f}x")
    
    return True

def main():
    """主测试函数"""
    print("\n" + "="*60)
    print("增量更新功能测试套件")
    print("="*60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 测试1: 板块宽度增量更新
        test_sector_incremental()
        
        # 测试2: ETF策略增量更新  
        test_etf_incremental()
        
        # 测试3: 性能对比（可选）
        # test_full_vs_incremental()
        
        print("\n" + "="*60)
        print("所有测试完成！")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
