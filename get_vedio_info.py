#!/usr/bin/env python3
"""
多账号批量处理+豆包AI总结测试脚本
功能：
1. 支持多个SEC_UID和对应账号名称
2. 统一时间筛选条件
3. 自动调用豆包API总结视频内容
4. 输出格式：账号名称, 视频地址, 视频标题, 豆包AI总结内容
"""

import requests
from datetime import datetime
import time

# ==================== 豆包AI客户端 ====================
#!/usr/bin/env python3
# ... 保持所有前面的代码不变 ...

class DoubaoAI:
    def __init__(self, api_key: str, model: str = "ep-20251108122940-dg6jq"):
        self.api_key = api_key
        # 修改这里：移除路径中的/api/v3，只在请求时添加
        self.base_url = "https://ark.cn-beijing.volces.com"
        self.model = model
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.timeout = 60
    
    def summarize_video(self, video_url: str, title: str, account_name: str) -> str:
        """
        优化后的提示词结构：
        - system: 定义AI身份
        - user: 明确任务、格式、重点
        """
        
        # system: 只定义角色（简洁）
        system_prompt = "你是一个专业的投资内容分析师，擅长从抖音视频中提取和总结与投资相关的观点、数据和分析逻辑。"
        
        # user: 详细任务指令（重点在这里）
        user_content = f"""请总结以下抖音视频的所有投资相关观点：
            【账号】{account_name}
            【视频标题】{title}
            【视频链接】{video_url}

            请按以下格式输出：
            1. **大盘观点**：视频对大盘走势的看法、判断依据，保留视频中涉及原文只是去掉没有实际意义的词汇
            2. **板块分析**：看好或者不看好的行业/板块及其逻辑，保留视频中涉及原文只是去掉没有实际意义的词汇
            3. **公司分析**：看好的公司或股票并给出详细逻辑，保留视频中涉及原文只是去掉没有实际意义的词汇
            4. **风险提示**：提到的风险因素和注意事项
            5. **明确类型**：视频如果是对之前行情的总结，请标明。同时标注上观点的看法时间，例如“下周”、“未来3个月”，“明天”

            要求：
            - 不要遗漏任何投资相关内容
            - 用简洁、专业的语言
            - 如果对应项没有相关内容，明确说明"略" """
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.7,  # 0.7保持一定创造性
            "max_tokens": 3000   # 投资分析需要更多字数
        }
        
        # 后续请求代码...
        
        try:
            # 关键修复：完整的endpoint路径
            endpoint = f"{self.base_url}/api/v3/chat/completions"
            
            # 打印调试信息
            print(f"\n[Debug] 正在请求: {endpoint}")
            print(f"[Debug] 模型: {self.model}")
            
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=payload,
                timeout=self.timeout
            )
            
            # 打印响应信息
            print(f"[Debug] 状态码: {response.status_code}")
            if response.status_code != 200:
                print(f"[Debug] 错误响应: {response.text[:500]}")
            
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
            
        except Exception as e:
            print(f"[!] 豆包API调用失败: {e}")
            return f"总结失败: {str(e)}"

# ... 保持所有后面的代码不变 ...
# ==================== 抖音API客户端 ====================
class DouyinAPI:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
    
    def get_user_videos(self, sec_uid: str, count: int = 30):
        endpoint = f"{self.base_url}/api/douyin/web/fetch_user_post_videos"
        params = {"sec_user_id": sec_uid, "count": count, "max_cursor": 0}
        
        try:
            response = requests.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") == 200:
                return data.get("data", {}).get("aweme_list", [])
            else:
                print(f"[!] API错误: {data.get('message')}")
                return []
        except Exception as e:
            print(f"[!] 网络请求失败: {e}")
            return []

# ==================== 参数配置（全部写死） ====================
# 豆包API密钥
DOUBAO_API_KEY = "3c039545-4532-4f17-a1dd-5b890e7b25c7"

# 多个SEC_UID和对应账号名称
ACCOUNTS = [
    {
        "sec_uid": "MS4wLjABAAAA6VKlJ8WJIVVnw1Lq6Aq0ydNVFz7Fw_FJwFBz69ofgvw",
        "name": "丹木大叔"
    },
    {
        "sec_uid": "MS4wLjABAAAAtMbrLdV2Zk-ATVxn98zg3bnur76IEWbK43UzDgHmwC8",
        "name": "机构福总"
    },
    {
        "sec_uid": "MS4wLjABAAAAACxDGphpj8hpUb0uNtKQMuwGwnnqzMWpBRuKov11hytIgCIOID41hAgwGOAhM1MC",
        "name": "福总调研"
    },
    {
        "sec_uid": "MS4wLjABAAAAxWvkJpL16BVeki4zgW2l_x-BoZM5k8L2W2Cn03yC760",
        "name": "爱丽丝财经笔记"
    },
    {
        "sec_uid": "MS4wLjABAAAAUX1S9gYjnwzHe9WWFF6LjlneOULCSWIj0nYLoPHtHkmr3PwHjPInf5byan8MvKR5",
        "name": "刘满仓"
    }
]

# 统一时间筛选条件：2025年11月7日12:00:00
# 注意：如果用户未发布该时间之后的视频，结果为空
CUTOFF_TIME = datetime(2025, 11, 22, 23, 0, 0)

# 每个用户获取的视频数量
FETCH_COUNT = 20  # 多获取一些用于筛选

# 豆包API调用间隔（秒），避免限流
API_DELAY = 2.0
# ========================================================

print("=" * 90)
print("多账号视频+豆包AI批量处理")
print("=" * 90)
print(f"时间筛选: {CUTOFF_TIME.strftime('%Y-%m-%d %H:%M:%S')} 之后的视频")
print(f"处理账号: {len(ACCOUNTS)} 个")
print("-" * 90)

# 初始化API客户端
douyin = DouyinAPI()
doubao = DoubaoAI(DOUBAO_API_KEY)

# 结果存储
final_results = []

# 遍历每个账号
for account in ACCOUNTS:
    account_name = account["name"]
    sec_uid = account["sec_uid"]
    
    print(f"\n[{'='*70}]")
    print(f"正在处理账号: {account_name} (UID: {sec_uid[:30]}...)")
    print(f"[{'='*70}]")
    
    # 1. 获取视频列表
    print(f"\n[Step 1] 获取视频列表...")
    videos = douyin.get_user_videos(sec_uid, FETCH_COUNT)
    
    if not videos:
        print(f"[!] 账号 {account_name} 未获取到视频，跳过")
        continue
    
    print(f"[OK] 获取到 {len(videos)} 个视频")
    
    # 2. 时间筛选
    print(f"\n[Step 2] 时间筛选...")
    filtered_videos = []
    
    for video in videos:
        create_timestamp = video.get("create_time", 0)
        create_datetime = datetime.fromtimestamp(create_timestamp)
        
        # 只保留指定时间之后的视频
        if create_datetime > CUTOFF_TIME:
            filtered_videos.append({
                "aweme_id": video.get("aweme_id"),
                "title": video.get("desc", "")[:100],  # 视频标题/描述（前100字）
                "create_time": create_datetime,
                "doubao_url": f"https://www.douyin.com/video/{video.get('aweme_id')}",
                "original_video": video
            })
    
    print(f"[OK] 筛选后剩余 {len(filtered_videos)} 个视频")
    
    if not filtered_videos:
        print(f"[!] 账号 {account_name} 没有符合条件的视频")
        continue
    
    # 3. 豆包AI总结
    print(f"\n[Step 3] 调用豆包AI总结...")
    
    for idx, video_info in enumerate(filtered_videos, 1):
        account_name = account["name"]
        video_url = video_info["doubao_url"]
        title = video_info["title"]
        
        print(f"\n[{idx}/{len(filtered_videos)}] 正在总结: {account_name} - {title[:30]}...")
        
        # 调用豆包API
        summary = doubao.summarize_video(video_url, title, account_name)
        
        # 存储结果
        result_item = {
            "account_name": account_name,
            "video_url": video_url,
            "video_title": title,
            "doubao_summary": summary,
            "publish_time": video_info["create_time"].strftime("%Y-%m-%d %H:%M:%S")
        }
        final_results.append(result_item)
        
        # 实时输出结果（CSV格式）
        print("\n" + "=" * 80)
        print("实时结果输出（CSV格式）：")
        print(f"{account_name}, {video_url}, \"{title}\", \"{summary[:100]}...\"")
        print("=" * 80)
        
        # 延迟防止限流
        time.sleep(API_DELAY)
    
    print(f"\n[OK] 账号 {account_name} 处理完成")

# 4. 总结输出
print("\n\n" + "=" * 90)
print("所有账号处理完成！")
print("=" * 90)
print(f"总计获取视频: {len(final_results)} 个")

if final_results:
    print("\n最终结果（CSV格式）：")
    print("=" * 90)
    print("账号名称, 视频地址, 视频标题, 豆包AI总结内容")
    print("-" * 90)
    
    for item in final_results:
        # CSV格式：账号名称, 视频地址, "视频标题", "豆包AI总结"
        account_name = item["account_name"]
        video_url = item["video_url"]
        title = item["video_title"].replace('"', '""')  # CSV转义双引号
        summary = item["doubao_summary"].replace('"', '""')
        
        print(f'{account_name}, {video_url}, "{title}", "{summary}"')
    
    # 同时保存到文件
    output_file = f"douyin_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(output_file, "w", encoding="utf-8-sig") as f:  # utf-8-sig for Excel
        f.write("账号名称,视频地址,视频标题,豆包AI总结内容\n")
        for item in final_results:
            account_name = item["account_name"]
            video_url = item["video_url"]
            title = item["video_title"].replace('"', '""')
            summary = item["doubao_summary"].replace('"', '""')
            f.write(f'{account_name},{video_url},"{title}","{summary}"\n')
    
    print("\n" + "=" * 90)
    print(f"结果已保存到: {output_file}")
else:
    print("没有符合条件的视频，请检查时间设置是否正确")