@echo off
REM 设置代理环境变量
set HTTP_PROXY=http://127.0.0.1:7890
set HTTPS_PROXY=http://127.0.0.1:7890
echo 已设置代理: %HTTP_PROXY%

REM 使用 Python 3.13 启动 Streamlit
python -m streamlit run app.py
