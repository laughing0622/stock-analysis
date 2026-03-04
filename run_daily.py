import logging
import time
from datetime import datetime, timedelta
from tasks import job_macro
from data_engine import engine

# 配置日志 - 保存到文件和控制台
logging.basicConfig(
    level=logging INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_task.log', encoding='utf-8'),  # 保存到文件
        logging.StreamHandler()  # 输出到控制台
    ]
)

# ==========================================
# 模块1: 量能分析
# ==========================================
def run_intraday_save():
    """运行日内数据保存任务"""
    logging.info("📊 [模块1] 量能分析 - 日内节点数据保存...")
    try:
        engine.save_intraday_data()
        logging.info("   ✅ 日内节点数据保存完成")
    except Exception as e:
        logging.error(f"   ❌ 日内数据保存异常: {e}")

# ==========================================
# 模块2: 宏观择时
# ==========================================
def run_macro_breadth():
    """运行市场宽度数据更新（含换手率）"""
    logging.info("📈 [模块2] 宏观择时 - 市场宽度数据更新...")
    try:
        # 使用 data_engine 的 update_today_breadth 方法（包含换手率）
        engine.update_today_breadth()
        logging.info("   ✅ 市场宽度数据更新完成（含4指数+4指标）")
    except Exception as e:
        logging.error(f"   ❌ 市场宽度数据更新异常: {e}")

# ==========================================
# 模块3: 板块宽度
# ==========================================
def run_sector_breadth():
    """运行板块宽度数据更新"""
    logging.info("🏢 [模块3] 板块宽度 - 申万行业热力图更新...")
    try:
        engine.update_sector_breadth(incremental=True)
        logging.info("   ✅ 板块宽度数据更新完成")
    except Exception as e:
        logging.error(f"   ❌ 板块宽度数据更新异常: {e}")

# ==========================================
# 模块4: ETF策略
# ==========================================
def run_etf_strategy():
    """运行ETF策略数据更新"""
    logging.info("💰 [模块4] ETF策略 - ETF动量数据更新...")
    try:
        engine.update_strategy_data(incremental=True)
        logging.info("   ✅ ETF策略数据更新完成")
    except Exception as e:
        logging.error(f"   ❌ ETF策略数据更新异常: {e}")

# ==========================================
# 模块5-7: 策略数据（可选，非交易时段可跳过）
# ==========================================
def run_strategies():
    """运行策略数据更新（配债+可转债+期指）"""
    logging.info("🎯 [模块5-7] 策略数据更新...")

    # 配债策略
    try:
        result = engine.update_convertible_strategy()
        if result.get('timing_safe', False):
            logging.info(f"   ✅ 配债策略更新完成")
        else:
            logging.info(f"   ⏭️  配债策略跳过: {result.get('timing_msg', '')}")
    except Exception as e:
        logging.error(f"   ❌ 配债策略更新异常: {e}")

    time.sleep(1)

    # 可转债低估策略
    try:
        result = engine.update_bond_low_strategy()
        counts = result.get('counts', {})
        logging.info(f"   ✅ 可转债低估策略更新完成 (可用:{counts.get('usable',0)})")
    except Exception as e:
        logging.error(f"   ❌ 可转债低估策略更新异常: {e}")

    time.sleep(1)

    # 期指持仓数据
    try:
        target_date, prev_date = engine.get_futures_smart_date()
        if target_date:
            result = engine.analyze_futures_position_change(target_date, prev_date)
            if result:
                logging.info(f"   ✅ 期指持仓数据更新完成 ({target_date})")
            else:
                logging.info(f"   ⏭️  期指持仓数据跳过")
        else:
            logging.info(f"   ⏭️  期指持仓数据跳过（非交易时间）")
    except Exception as e:
        logging.error(f"   ❌ 期指持仓数据更新异常: {e}")

# ==========================================
# 主函数
# ==========================================
def main(skip_strategies=False):
    """
    运行每日数据更新

    Args:
        skip_strategies: 是否跳过策略数据（用于非交易时段）
    """
    start_time = datetime.now()
    logging.info("🚀 启动 AlphaMonitor 每日数据作业...")

    # 核心数据（每次都运行）
    run_intraday_save()
    time.sleep(1)

    run_macro_breadth()      # 使用新方法替代 job_macro.run()
    time.sleep(1)

    run_sector_breadth()
    time.sleep(1)

    run_etf_strategy()

    # 策略数据（交易时段运行）
    if not skip_strategies:
        run_strategies()
    else:
        logging.info("⏭️  跳过策略数据更新（skip_strategies=True）")

    # 汇总
    elapsed = (datetime.now() - start_time).total_seconds()
    logging.info(f"✅ 所有任务执行完毕，耗时 {elapsed:.1f} 秒。")

if __name__ == "__main__":
    import sys
    # 支持命令行参数
    skip_strategies = '--skip-strategies' in sys.argv or '-s' in sys.argv
    main(skip_strategies=skip_strategies)