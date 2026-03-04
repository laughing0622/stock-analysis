import sys
import os

import json
import pandas as pd

# 先导入配置，设置代理环境变量（必须在导入 google.genai 之前）
try:
    from config import GEMINI_API_KEY, GEMINI_MODEL_NAME, GEMINI_PROXY
    # 【已注释】立即设置代理 - 可能影响其他网络请求导致数据下载失败
    # if GEMINI_PROXY:
    #     os.environ['HTTP_PROXY'] = GEMINI_PROXY
    #     os.environ['HTTPS_PROXY'] = GEMINI_PROXY
    #     print(f"DEBUG: [模块级] 设置代理: {GEMINI_PROXY}")
except ImportError as e:
    print(f"配置导入失败: {e}")
    GEMINI_API_KEY = None
    GEMINI_MODEL_NAME = "gemini-2.0-flash-exp"
    GEMINI_PROXY = None

# 现在才导入 genai（此时代理已设置）
# 注意：google-genai 可能需要额外配置 httpx 代理
genai_http_client = None
try:
    import google.genai as genai
    # 【已注释】尝试配置 httpx 代理（google-genai 底层使用 httpx）- 可能影响其他网络请求
    # if GEMINI_PROXY:
    #     try:
    #         import httpx
    #         # httpx 的正确代理配置方式：使用 proxy 参数（单数）
    #         genai_http_client = httpx.Client(proxy=GEMINI_PROXY, timeout=60.0)
    #         print(f"DEBUG: [模块级] 创建 httpx 代理客户端: {GEMINI_PROXY}")
    #     except Exception as e:
    #         print(f"DEBUG: httpx 代理配置失败: {e}，将使用环境变量代理")
    #         genai_http_client = None
except ImportError:
    try:
        from google import genai
    except ImportError:
        genai = None

class StockLLMClient:
    def __init__(self, proxy=None):
        # 调试信息：如果导入失败，打印当前 Python 路径
        if not genai:
            print(f"DEBUG: google.genai 库未安装")
            print(f"DEBUG: sys.executable = {sys.executable}")
            print(f"DEBUG: sys.path = {sys.path}")
            self.api_available = False
            self.error_msg = "google.genai 库未安装"
            return

        if not GEMINI_API_KEY:
            print(f"DEBUG: GEMINI_API_KEY 未配置")
            self.api_available = False
            self.error_msg = "GEMINI_API_KEY 未配置"
            return
            
        try:
            # 【已注释】如果参数传入了额外的代理，也设置一下 - 可能影响其他网络请求
            # if proxy:
            #     os.environ['HTTP_PROXY'] = proxy
            #     os.environ['HTTPS_PROXY'] = proxy
            #     print(f"DEBUG: [参数] 覆盖代理: {proxy}")
            
            # 新版客户端初始化，尝试传入 http_client
            if genai_http_client:
                try:
                    # 使用自定义的 httpx 客户端（带代理）
                    self.client = genai.Client(api_key=GEMINI_API_KEY, http_options={'client': genai_http_client})
                    print(f"DEBUG: Gemini Client 初始化成功（使用 httpx 代理）")
                except Exception as e1:
                    print(f"DEBUG: 尝试1失败: {e1}")
                    try:
                        # 尝试其他参数名
                        self.client = genai.Client(api_key=GEMINI_API_KEY, client=genai_http_client)
                        print(f"DEBUG: Gemini Client 初始化成功（client参数）")
                    except Exception as e2:
                        print(f"DEBUG: 尝试2失败: {e2}")
                        # 回退到普通初始化
                        self.client = genai.Client(api_key=GEMINI_API_KEY)
                        print(f"DEBUG: Gemini Client 初始化成功（使用环境变量代理）")
            else:
                self.client = genai.Client(api_key=GEMINI_API_KEY)
                print(f"DEBUG: Gemini Client 初始化成功")
            
            self.api_available = True
            self.error_msg = None
        except Exception as e:
            print(f"DEBUG: Gemini Client 初始化失败: {e}")
            self.api_available = False
            self.error_msg = f"客户端初始化失败: {str(e)}"

    def _format_csv_for_prompt(self, df):
        """将DataFrame转换为精简的CSV字符串，节省Token"""
        # 复制并处理数据
        d = df.copy()
            
        # 优化：只取最近180天数据（约半年，足够分析且节省Token）
        if len(d) > 180:
            d = d.tail(180)
            print(f"DEBUG: 数据裁减至最近180天，原始{len(df)}天")
            
        # 成交量转为"万手"，保留整数
        d['vol'] = (d['vol'] / 10000).astype(int)
        d['VMA20'] = (d['VMA20'] / 10000).fillna(0).astype(int)
            
        # 价格保疙2位小数
        price_cols = [c for c in d.columns if c not in ['date', 'vol', 'VMA20']]
        for c in price_cols:
            d[c] = d[c].round(2)
                
        # 转CSV，无表头，无索引
        csv_str = d.to_csv(index=False, header=False)
            
        # 添加头部说明
        columns_desc = ",".join(d.columns)
        final_str = f"Data Columns: {columns_desc}\nData Values (Vol in 10k):\n{csv_str}"
            
        # 打印Token估算
        token_est = len(final_str) // 4  # 粗略估计：4字符约1 token
        print(f"DEBUG: 数据大小 {len(final_str)} 字符，估计 ~{token_est} tokens")
            
        return final_str

    def analyze_stock(self, stock_name, df, custom_prompt="", system_prompt=None):
        if not self.api_available:
            error_detail = getattr(self, 'error_msg', '未配置 GEMINI_API_KEY，或客户端初始化失败。')
            return {"error": f"未配置 GEMINI_API_KEY，或客户端初始化失败。\n\n详细信息: {error_detail}"}

        data_str = self._format_csv_for_prompt(df)
        
        # 1. 构建 System Prompt (设定)
        # 如果用户没在UI修改，就用默认的
        if not system_prompt:
            system_prompt = """
你现在是交易史上最伟大的人物理查德·D·威科夫（Richard D. Wyckoff）。
你需要对我提供的股票行情数据进行大师级的专业分析。
请严格遵循以下JSON格式输出你的分析结果，不要输出任何Markdown代码块标记（如 ```json），直接输出JSON字符串。

JSON输出格式要求：
{
    "analysis_text": "这里写你的威科夫语气分析报告，使用中文，包含对背景、阶段、关键行为的详细解读...",
    "market_phase": "当前所处阶段 (如 Phase A / Phase B / 吸筹 / 派发 / 上升趋势)",
    "phases": [
        {
            "name": "吸筹区/派发区/交易区间",
            "start_date": "YYYY-MM-DD",
            "end_date": "YYYY-MM-DD",
            "top_price": 15.5,
            "bottom_price": 12.0,
            "type": "accumulation" (或 distribution / neutral)
        }
    ],
    "events": [
        {
            "date": "YYYY-MM-DD",
            "type": "SC/ST/Spring/LPS/SOS/UTAD/SOW",
            "description": "简短说明理由"
        }
    ]
}
"""
        # 2. 构建 User Prompt (数据 + 指令)
        user_instruction = f"""
分析对象：{stock_name}
数据如下（已按日期排序）：
{data_str}

{custom_prompt}

请开始你的表演，威科夫先生。请务必返回合法的 JSON 格式。
"""

        try:
            # 新版 SDK 调用方式，增加超时和重试
            import time
            max_retries = 3
            retry_delay = 2  # 秒
            
            print(f"DEBUG: 开始调用 {GEMINI_MODEL_NAME} 模型...")
            start_time = time.time()
            
            for attempt in range(max_retries):
                try:
                    print(f"DEBUG: 第 {attempt + 1} 次请求发出，请耐心等待（首次通常需要 30-60 秒）...")
                    response = self.client.models.generate_content(
                        model=GEMINI_MODEL_NAME,
                        contents=f"{system_prompt}\n\n{user_instruction}",
                        config={"temperature": 0.2}  # 低温度保证格式稳定
                    )
                    elapsed = time.time() - start_time
                    print(f"DEBUG: API 调用成功，耗时 {elapsed:.1f} 秒")
                    break  # 成功则退出重试循环
                except Exception as e:
                    error_detail = str(e)
                    print(f"DEBUG: 第 {attempt + 1} 次调用失败: {error_detail[:200]}...")  # 打印前200字符
                    if attempt < max_retries - 1:
                        print(f"API 调用失败，{retry_delay}秒后重试... ({attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                    else:
                        raise  # 最后一次失败则抛出异常
            
            # 解析 JSON
            text = response.text
            # 清理可能的 markdown 标记
            text = text.replace("```json", "").replace("```", "").strip()
            
            try:
                result = json.loads(text)
                return result
            except json.JSONDecodeError:
                return {
                    "error": "解析 AI 返回的 JSON 失败",
                    "raw_response": text
                }
                
        except Exception as e:
            error_msg = str(e)
            # 网络错误特别提示
            if "10060" in error_msg or "timeout" in error_msg.lower():
                return {
                    "error": f"API 调用失败: 网络连接超时\n\n原因: {error_msg}\n\n解决方案:\n1. 检查网络连接\n2. 如需要代理，请在 config.py 中配置 GEMINI_PROXY\n3. 尝试使用 VPN 或其他网络环境"
                }
            return {"error": f"API 调用失败: {error_msg}"}
