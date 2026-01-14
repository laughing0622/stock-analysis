"""
标的代码标准化工具 (Info项目版)
用于统一各类数据源（Tushare, Akshare, QMT, 雪球, 聚宽）的代码格式。
核心标准：QMT格式 (e.g., 000001.SZ, 113050.SH)
"""
import re

def normalize_to_std(code: str) -> str:
    """
    将任意格式代码转换为标准 QMT 格式 (Code.Suffix)
    
    支持输入: 
    - 纯数字: "000001", "123001"
    - 前缀: "SZ000001", "SH113050"
    - 后缀: "000001.XSHE", "600519.XSHG"
    - 标准: "000001.SZ"
    - 混合: " 000001 " (去空格)
    
    返回:
    - 标准格式字符串 (e.g. "000001.SZ")
    - 如果无法识别，返回原始字符串
    """
    if not code:
        return ""
    
    code = str(code).strip().upper()
    
    # 1. 处理带后缀的 (QMT/Tushare/JoinQuant)
    if "." in code:
        base, suffix = code.split(".")
        # 聚宽格式映射
        if suffix == "XSHE": return f"{base}.SZ"
        if suffix == "XSHG": return f"{base}.SH"
        if suffix == "XBSE": return f"{base}.BJ"
        # 标准格式直接返回
        if suffix in ["SZ", "SH", "BJ"]: return code
        # 其他未知后缀，尝试只取前面部分重新推断（防御性）
        if base.isdigit() and len(base) == 6:
            code = base # 降级为纯数字处理
        else:
            return code 

    # 2. 处理带前缀的 (雪球/富途)
    # 匹配 "SZ123456" 格式
    match = re.match(r"^(SZ|SH|BJ)(\d{6})$", code)
    if match:
        return f"{match.group(2)}.{match.group(1)}"

    # 3. 处理纯数字 (Akshare/集思录/手输)
    if code.isdigit() and len(code) == 6:
        market = _infer_market(code)
        return f"{code}.{market}" if market else code

    return code

def _infer_market(code: str) -> str:
    """
    根据首位数字推断市场 (修正版)
    """
    # 优先判断北交所 (8xx, 4xx, 92x)
    if code.startswith(("8", "4", "92")):
        return "BJ"
    
    # === 关键修正 ===
    # 12xxxx 全系均为 深交所转债 (创业板/主板)
    if code.startswith("12"): 
        return "SZ"
    
    # 沪市转债 (11)
    if code.startswith("11"):
        return "SH"
    
    # 沪市股票/ETF/基金 (6, 5)
    if code.startswith(("6", "5")):
        return "SH"
    
    # 深市股票/ETF/基金 (0, 3, 15, 16)
    if code.startswith(("0", "3", "15", "16")):
        return "SZ"
        
    return "SZ" # 默认兜底

def format_for_display(code: str) -> str:
    """
    用于前端展示，去除后缀，保持整洁 (可选)
    """
    if not code: return ""
    return code.split(".")[0] if "." in code else code

if __name__ == "__main__":
    # 测试用例
    test_cases = [
        ("123001", "123001.SZ"), # 创业板转债
        ("113050", "113050.SH"), # 沪市转债
        ("600519", "600519.SH"), # 沪市股票
        ("000001", "000001.SZ"), # 深市股票
        ("SZ000001", "000001.SZ"), # 雪球格式
        ("000001.XSHE", "000001.SZ"), # 聚宽格式
        ("830001", "830001.BJ"), # 北交所
        ("920001", "920001.BJ"), # 北交所新号段
        ("159915", "159915.SZ"), # 创业板ETF
        ("510300", "510300.SH"), # 沪深300ETF
    ]
    for inp, expected in test_cases:
        res = normalize_to_std(inp)
        print(f"{inp} -> {res} [{'✅' if res==expected else '❌'}]")
