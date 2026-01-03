import schedule
import time
import logging
import subprocess
from datetime import datetime
import os

# 配置日志 - 保存到文件和控制台
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('auto_run_daily.log', encoding='utf-8'),  # 保存到文件
        logging.StreamHandler()  # 输出到控制台
    ]
)

# 获取脚本所在目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 设置要运行的脚本路径
DAILY_SCRIPT_PATH = os.path.join(BASE_DIR, 'run_daily.py')

# 设置Python解释器路径（如果需要特定环境，可以修改这里）
PYTHON_EXECUTABLE = 'python'

def run_daily_script():
    """运行每日脚本"""
    logging.info("=== 开始执行每日自动任务 ===")
    try:
        # 运行run_daily.py脚本
        result = subprocess.run(
            [PYTHON_EXECUTABLE, DAILY_SCRIPT_PATH],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=3600  # 设置1小时超时
        )
        
        # 记录标准输出
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                logging.info(f"   OUTPUT: {line}")
        
        # 记录标准错误
        if result.stderr:
            for line in result.stderr.strip().split('\n'):
                logging.error(f"   ERROR: {line}")
        
        # 记录返回码
        if result.returncode == 0:
            logging.info("=== 每日自动任务执行成功 ===")
        else:
            logging.error(f"=== 每日自动任务执行失败，返回码: {result.returncode} ===")
            
    except subprocess.TimeoutExpired:
        logging.error("=== 每日自动任务执行超时 ===")
    except Exception as e:
        logging.error(f"=== 每日自动任务执行异常: {e} ===")

def main():
    """主函数：设置定时任务"""
    logging.info("🚀 启动每日任务自动运行服务...")
    
    # 设置运行时间：每天21:00
    schedule.every().day.at("21:00").do(run_daily_script)
    
    # 初始运行一次，用于测试
    logging.info("🔧 初始运行一次，用于测试...")
    run_daily_script()
    
    # 持续运行
    logging.info("⏰ 每日任务自动运行服务已启动，每天21:00执行")
    logging.info("按 Ctrl+C 停止服务")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("🛑 每日任务自动运行服务已停止")
    except Exception as e:
        logging.error(f"💥 服务意外终止: {e}")
