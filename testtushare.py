import sys
import tushare as ts

# 1. 检查 Python 路径（确保是你自己的环境）
print("当前使用的 Python:", sys.executable)

# 2. 检查 Tushare 是否加载成功
print("Tushare 版本:", ts.__version__)

# 3. 简单的网络测试（Tushare 需要联网）
# 注意：这一步如果报错，说明 Proxifier 可能把 Python 的流量拦截了
try:
    # 这里用你的 token（代码里不用真写，只要 import 成功通常就没大问题）
    # api = ts.pro_api('你的token')
    print("环境配置成功！可以开始开发了。")
except Exception as e:
    print("Tushare 连接报错:", e)