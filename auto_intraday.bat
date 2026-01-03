@echo off
chcp 65001 >nul
echo 启动日内数据自动保存服务...
python run_intraday_schedule.py
pause
