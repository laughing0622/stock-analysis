@echo off
chcp 65001 >nul
echo ========================================
echo    全量回填换手率数据（2019年起）
echo ========================================
echo.
echo 预计时间：6-10小时
echo 建议：晚上睡前运行
echo.
pause

echo.
echo 开始执行...
python run_backfill_batch.py

echo.
echo ========================================
echo    回填完成！
echo ========================================
pause
