#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试板块增量更新是否正常
"""
import sqlite3
import pandas as pd
from data_engine import DataEngine

def test_sector_incremental():
    print("=== 测试板块增量更新 ===\n")
    
    engine = DataEngine()
    conn = sqlite3.connect('data/stock_data.db')
    
    # 1. 查询更新前的最新日期
    df_before = pd.read_sql("SELECT MAX(trade_date) as max_date FROM sector_breadth", conn)
    latest_before = df_before.iloc[0]['max_date']
    print(f"1. 更新前数据库最新日期: {latest_before}")
    
    # 2. 查询最新日期的数据样本
    if latest_before:
        df_sample = pd.read_sql(
            f"SELECT * FROM sector_breadth WHERE trade_date='{latest_before}' AND level='level1' LIMIT 5", 
            conn
        )
        print(f"\n2. 更新前最新日期样本数据:")
        print(df_sample[['trade_date', 'sector_name', 'pct_above_ma20']])
        print(f"   样本均值: {df_sample['pct_above_ma20'].mean():.1f}%")
        print(f"   样本最大值: {df_sample['pct_above_ma20'].max()}%")
    
    # 3. 执行增量更新
    print(f"\n3. 开始执行增量更新...")
    print("=" * 50)
    engine.update_sector_breadth(incremental=True)
    print("=" * 50)
    
    # 4. 查询更新后的最新日期
    df_after = pd.read_sql("SELECT MAX(trade_date) as max_date FROM sector_breadth", conn)
    latest_after = df_after.iloc[0]['max_date']
    print(f"\n4. 更新后数据库最新日期: {latest_after}")
    
    # 5. 查询新日期的数据样本
    if latest_after:
        df_new = pd.read_sql(
            f"SELECT * FROM sector_breadth WHERE trade_date='{latest_after}' AND level='level1' LIMIT 5", 
            conn
        )
        print(f"\n5. 更新后最新日期样本数据:")
        print(df_new[['trade_date', 'sector_name', 'pct_above_ma20']])
        print(f"   样本均值: {df_new['pct_above_ma20'].mean():.1f}%")
        print(f"   样本最大值: {df_new['pct_above_ma20'].max()}%")
        
        # 6. 检查是否有0值问题
        zero_count = (df_new['pct_above_ma20'] == 0).sum()
        total_count = len(df_new)
        print(f"\n6. 数据质量检查:")
        print(f"   0值行业数: {zero_count}/{total_count}")
        
        if zero_count == total_count:
            print("   ❌ 警告: 所有行业宽度都是0, 数据异常!")
        elif df_new['pct_above_ma20'].mean() > 0:
            print(f"   ✅ 数据正常, 平均宽度 {df_new['pct_above_ma20'].mean():.1f}%")
    
    conn.close()

if __name__ == "__main__":
    test_sector_incremental()
