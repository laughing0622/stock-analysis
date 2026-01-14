@echo off
echo ========================================
echo Streamlit 重启脚本
echo ========================================
echo.

echo [1/3] 正在查找 Streamlit 进程...
for /f "tokens=2" %%i in ('tasklist ^| findstr /i "streamlit"') do (
    echo 发现 Streamlit 进程: %%i
    taskkill /F /PID %%i >nul 2>&1
    if errorlevel 1 (
        echo 无法终止进程 %%i (可能需要管理员权限)
    ) else (
        echo 已终止进程 %%i
    )
)

echo.
echo [2/3] 正在查找 Python Streamlit 进程...
wmic process where "commandline like '%%streamlit%%' and name='python.exe'" delete >nul 2>&1

echo.
echo [3/3] 清理缓存...
rd /s /q .streamlit >nul 2>&1
rd /s /q __pycache__ >nul 2>&1
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

echo.
echo ========================================
echo 清理完成！
echo ========================================
echo.
echo 现在请运行 start_app.bat 重新启动 Streamlit
echo.
pause
