import os

# Tushare Token
TS_TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'

# 数据库路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'stock_data.db')

# 监控指数列表
INDEX_MAP = {
    '沪深300': '000300.SH',
    '创业板指': '399006.SZ',
    '中证2000': '932000.CSI', 
    '上证指数': '000001.SH'
}

# ==========================================
# Gemini AI 配置
# ==========================================
# 请在此处填入您的 Google Gemini API Key
GEMINI_API_KEY = "AIzaSyCG306ltsH4MFPJsAToIawsEBlWuwrBQhU" 

# 模型名称（推荐使用最新模型）
# - gemini-3-pro-preview: Gemini 3.0 Pro 预览版（最新）
# - gemini-2.5-pro: Gemini 2.5 Pro 稳定版
# - gemini-2.5-flash: Gemini 2.5 Flash 快速版
# - gemini-2.0-flash-exp: Gemini 2.0 实验版
# - gemini-exp-1206: 2024.12.6 实验模型
GEMINI_MODEL_NAME = "gemini-2.0-flash-exp"

# 代理配置（如果需要的话）
# 示例: GEMINI_PROXY = "http://127.0.0.1:7890"
# 示例: GEMINI_PROXY = "socks5://127.0.0.1:1080"
# 【已注释】代理配置 - 可能影响其他网络请求导致数据下载失败
# GEMINI_PROXY = "http://127.0.0.1:7890"  # Clash/V2Ray 默认代理端口
GEMINI_PROXY = None  # 禁用代理

# ==========================================
# 雪球量化策略数据库配置
# ==========================================
XUEQIU_DB_PATH = r"D:\stockproject\xueqiu_qmt_trader\data\xueqiu_qmt.db"
XUEQIU_PROJECT_PATH = r"D:\stockproject\xueqiu_qmt_trader"

XUEQIU_STRATEGIES = {
    '雪球组合': {
        'enabled': True,
        'portfolios': ['ZH3204652', 'ZH2349311', 'ZH2497943', 'ZH2863835']
    },
    'ETF套利': {
        'enabled': True,
        'strategy_name': 'ETF_Arbitrage_Strategy'
    },
    '可转债三低轮动': {
        'enabled': True,
        'strategy_name': 'CB_ThreeLow_Strategy'
    },
    '聚宽AH溢价': {
        'enabled': True,
        'strategy_name': 'JoinQuant_AH_Strategy'
    }
}