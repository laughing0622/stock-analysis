import logging
import time
from datetime import datetime, timedelta
from tasks import job_macro
from data_engine import engine

# 配置日志 - 保存到文件和控制台
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_task.log', encoding='utf-8'),  # 保存到文件
        logging.StreamHandler()  # 输出到控制台
    ]
)

def run_intraday_save():
    """运行日内数据保存任务"""
    logging.info("📊 启动日内数据保存任务...")
    try:
        engine.save_intraday_data()
        logging.info("   ✅ 日内数据保存完成")
    except Exception as e:
        logging.error(f"   ❌ 日内数据保存异常: {e}")

def main():
    logging.info("🚀 启动 AlphaMonitor 每日数据作业...")
    
    # 1. 日内数据保存
    # 无论是否收盘，都保存当前日内数据
    run_intraday_save()
    
    # 2. 宏观择时数据更新
    # 建议在每天 16:00 以后运行
    try:
        job_macro.run()
        logging.info("✅ 宏观任务执行完成")
    except Exception as e:
        logging.error(f"❌ 宏观任务异常: {e}")
        
    logging.info("✅ 所有任务执行完毕。")

if __name__ == "__main__":
    main()