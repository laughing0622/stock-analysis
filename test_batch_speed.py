"""
测试批量查询vs逐个查询的速度对比
"""
import tushare as ts
import time
from concurrent.futures import ThreadPoolExecutor

TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'
pro = ts.pro_api(TOKEN)

# 测试股票列表（取100只作为样本）
test_stocks = [f'{num:06d}.SZ' for num in range(1, 101)]
test_date = '20260108'

print("="*60)
print("速度对比测试：100只股票")
print("="*60)

# ========== 方法1：逐个查询（当前方式） ==========
print("\n方法1：逐个查询 + 多线程并行")
print("-"*60)

start = time.time()
results1 = []

def fetch_single(code):
    try:
        df = pro.daily_basic(ts_code=code, trade_date=test_date, fields='ts_code,trade_date,turnover_rate')
        return len(df)
    except:
        return 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_single, code) for code in test_stocks]
    for future in futures:
        results1.append(future.result())

time1 = time.time() - start
success1 = sum(1 for r in results1 if r > 0)

print(f"耗时: {time1:.2f}秒")
print(f"成功获取: {success1}/100")
print(f"平均速度: {100/time1:.1f} 只/秒")

# 等待频率限制恢复
print("\n等待60秒让频率限制恢复...")
time.sleep(60)

# ========== 方法2：批量查询 ==========
print("\n方法2：批量查询 + 频率控制")
print("-"*60)

# 测试不同的批量大小
batch_sizes = [5, 10, 20, 50]
batch_results = []

for batch_size in batch_sizes:
    print(f"\n测试批量大小: {batch_size}只/次")

    start = time.time()
    results2 = []
    batches = [test_stocks[i:i+batch_size] for i in range(0, len(test_stocks), batch_size)]

    for batch in batches:
        codes_str = ','.join(batch)
        try:
            df = pro.daily_basic(ts_code=codes_str, trade_date=test_date, fields='ts_code,trade_date,turnover_rate')
            results2.append(len(df))
            # 频率控制：确保不超过200次/分钟
            time.sleep(60/200 * 1.1)  # 留10%余量
        except Exception as e:
            # 如果出错，可能是批量大小太大，尝试逐个查询这批
            if "每分钟最多访问" in str(e):
                time.sleep(60)
                results2.append(0)
            else:
                results2.append(0)

    time_batch = time.time() - start
    success_batch = sum(1 for r in results2 if r > 0)

    print(f"  耗时: {time_batch:.2f}秒")
    print(f"  成功获取: {success_batch}/{len(test_stocks)}")
    print(f"  平均速度: {len(test_stocks)/time_batch:.1f} 只/秒")

    batch_results.append({
        'batch_size': batch_size,
        'time': time_batch,
        'success': success_batch,
        'speed': len(test_stocks)/time_batch
    })

# 等待频率限制恢复
print("\n等待60秒...")
time.sleep(60)

# ========== 方法3：优化的批量查询 ==========
print("\n方法3：批量查询 + 并行 + 频率控制")
print("-"*60)

def fetch_batch(batch):
    codes_str = ','.join(batch)
    try:
        df = pro.daily_basic(ts_code=codes_str, trade_date=test_date, fields='ts_code,trade_date,turnover_rate')
        return len(df)
    except:
        return 0

# 使用最佳批量大小
best_batch_size = 10  # 先假设10
batches = [test_stocks[i:i+best_batch_size] for i in range(0, len(test_stocks), best_batch_size)]

start = time.time()
results3 = []

# 使用2个线程并行查询（避免触发频率限制）
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(fetch_batch, batch) for batch in batches]
    for future in futures:
        results3.append(future.result())
        # 每次查询后稍作延迟
        time.sleep(0.1)

time3 = time.time() - start
success3 = sum(1 for r in results3 if r > 0)

print(f"耗时: {time3:.2f}秒")
print(f"成功获取: {success3}/100")
print(f"平均速度: {100/time3:.1f} 只/秒")

# ========== 总结 ==========
print("\n" + "="*60)
print("速度对比总结")
print("="*60)
print(f"方法1（逐个+多线程）: {100/time1:.1f} 只/秒, 成功率 {success1}%")

for br in batch_results:
    print(f"方法2（批量{br['batch_size']}只）: {br['speed']:.1f} 只/秒, 成功率 {br['success']}%")

print(f"方法3（批量+并行）: {100/time3:.1f} 只/秒, 成功率 {success3}%")

# 计算全量回填时间预估
total_stocks = 5470
print(f"\n全量回填时间预估（{total_stocks}只股票，60个交易日）:")
print(f"方法1: 约 {(total_stocks/100/time1)*60/60:.1f} 小时")
print(f"方法2（批量10）: 约 {(total_stocks/100/batch_results[1]['speed'])*60/60:.1f} 小时")
print(f"方法3: 约 {(total_stocks/100/(100/time3))*60/60:.1f} 小时")
