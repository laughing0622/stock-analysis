import schedule
import time
import logging
from datetime import datetime
from data_engine import engine

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='intraday_schedule.log'  # 保存日志到文件
)

# 控制台输出
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def job():
    """定时任务：保存日内数据"""
    logging.info("=== 执行日内数据保存任务 ===")
    try:
        engine.save_intraday_data()
        logging.info("=== 日内数据保存任务完成 ===")
    except Exception as e:
        logging.error(f"=== 日内数据保存任务失败: {e} ===")

def main():
    """主函数：设置定时任务"""
    logging.info("🚀 启动日内数据定时保存服务...")
    
    # 设置运行时间点
    # 交易日时间：9:30-15:00
    # 保存频率：每15分钟保存一次
    schedule.every(15).minutes.do(job)
    
    # 初始运行一次
    job()
    
    # 持续运行
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    main()