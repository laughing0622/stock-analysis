import os
import sys

# ============================================
# 代理配置 - 根据你的环境修改此变量
# ============================================
# 格式：http://代理IP:端口 或 socks5://代理IP:端口
# 示例：Clash默认是 http://127.0.0.1:7890
PROXY_URL = "http://127.0.0.1:7890"  # 如果不需要代理，请留空字符串 ""

# 必须在导入requests之前设置代理环境变量
if PROXY_URL and PROXY_URL.strip():
    os.environ['http_proxy'] = PROXY_URL
    os.environ['https_proxy'] = PROXY_URL
    os.environ['HTTP_PROXY'] = PROXY_URL
    os.environ['HTTPS_PROXY'] = PROXY_URL
    print(f"已配置代理: {PROXY_URL}")
else:
    # 禁用代理
    os.environ['http_proxy'] = ''
    os.environ['https_proxy'] = ''
    os.environ['HTTP_PROXY'] = ''
    os.environ['HTTPS_PROXY'] = ''
    print("已禁用代理")

# =============================================================================
# 导入其他模块
# =============================================================================
import json
import requests
from datetime import datetime
from typing import List, Dict, Optional
import snscrape.modules.twitter as sntwitter
# =============================================================================
# 配置区域 - 请填写您的配置
# =============================================================================

# 目标用户配置
TARGET_USERNAME = "Mistery5387057"  # 替换为目标Twitter用户名
START_DATE = "2025-11-01"
END_DATE = "2025-11-08"

# 火山引擎豆包模型配置
DOUBAO_API_KEY = "3c039545-4532-4f17-a1dd-5b890e7b25c7"
DOUBAO_MODEL = "ep-20251108122940-dg6jq"

# 代理配置（如需要）
USE_PROXY = True
PROXY_HOST = "127.0.0.1"
PROXY_PORT = "7890"

# 输出配置
OUTPUT_DIR = "twint_output"

# 提示词配置
SUMMARY_PROMPT = """请基于以下推文内容进行专业总结，要求：
1. 提炼核心观点和主题
2. 识别重要事件或动态
3. 提取对A股、美股、黄金、具体公司、股票的观点

推文内容：
"""
# =============================================================================
# 网络连接测试模块
# =============================================================================

def test_connection():
    """测试网络连接和Twitter访问"""
    print("正在进行网络连接测试...")
    
    # 测试1：检查代理设置
    print("\n1. 代理设置:")
    print(f"   http_proxy: {os.environ.get('http_proxy', '未设置')}")
    print(f"   https_proxy: {os.environ.get('https_proxy', '未设置')}")
    
    # 测试2：测试连接Twitter
    print("\n2. 测试连接Twitter...")
    try:
        response = requests.get(
            "https://twitter.com",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        print(f"   成功连接Twitter，状态码: {response.status_code}")
        return True
    except Exception as e:
        print(f"   连接失败: {e}")
        print("\n错误排查建议:")
        print("   - 检查代理软件是否正在运行")
        print("   - 确认代理地址和端口是否正确")
        print("   - 在浏览器中测试能否访问Twitter")
        return False

# =============================================================================
# 推文收集模块
# =============================================================================

class TweetCollector:
    def __init__(self, username: str, start_date: str, end_date: str):
        self.username = username
        self.start_date = start_date
        self.end_date = end_date
    
    def collect_tweets(self, max_tweets: int = 300) -> List[Dict]:
        """收集推文"""
        print(f"\n开始收集 @{self.username} 的推文...")
        print(f"时间范围: {self.start_date} 至 {self.end_date}")
        
        query = f"from:{self.username} since:{self.start_date} until:{self.end_date}"
        
        tweets = []
        try:
            # 使用环境变量中的代理设置（不传递proxy参数）
            scraper = sntwitter.TwitterSearchScraper(query)
            
            for i, tweet in enumerate(scraper.get_items()):
                if i >= max_tweets:
                    print(f"已达到最大限制 {max_tweets} 条")
                    break
                
                # 提取图片URL
                image_urls = []
                if tweet.media:
                    for media in tweet.media:
                        if hasattr(media, 'fullUrl'):
                            image_urls.append(media.fullUrl)
                        elif hasattr(media, 'url'):
                            image_urls.append(media.url)
                
                processed_tweet = {
                    "id": tweet.id,
                    "username": tweet.user.username,
                    "displayname": tweet.user.displayname,
                    "date": str(tweet.date),
                    "text": tweet.rawContent,
                    "likes": tweet.likeCount,
                    "retweets": tweet.retweetCount,
                    "replies": tweet.replyCount,
                    "quotes": tweet.quoteCount,
                    "language": tweet.lang,
                    "hashtags": tweet.hashtags,
                    "mentions": tweet.mentions,
                    "urls": tweet.links,
                    "photos": image_urls,
                    "video": bool(tweet.media and any(m.type == 'video' for m in tweet.media)),
                    "link": f"https://twitter.com/{tweet.user.username}/status/{tweet.id}"
                }
                
                tweets.append(processed_tweet)
                
                # 进度提示
                if (i + 1) % 50 == 0:
                    print(f"已收集 {i + 1} 条推文...")
            
            print(f"成功收集 {len(tweets)} 条推文")
            return tweets
            
        except Exception as e:
            print(f"收集推文时出错: {e}")
            print("\n错误排查建议:")
            print("1. 检查代理设置（PROXY_URL变量）")
            print("2. 在浏览器中测试能否访问Twitter")
            print("3. 确认代理服务正在运行")
            print("4. 检查用户名是否存在且为公开账号")
            print("5. 可能是Twitter暂时限制访问，请稍后再试")
            return []

# =============================================================================
# 豆包总结模块
# =============================================================================

class DoubaoSummarizer:
    def __init__(self, api_key: str, model: str = "doubao-pro-32k"):
        self.api_key = api_key
        self.model = model
        self.endpoint = "https://ark.cn-beijing.volces.com/api/v3/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def create_tweets_text(self, tweets: List[Dict]) -> str:
        """构建推文文本"""
        tweets_text = ""
        total_chars = 0
        
        # 按时间排序
        sorted_tweets = sorted(tweets, key=lambda x: x['date'])
        
        for i, tweet in enumerate(sorted_tweets, 1):
            tweet_text = f"""
--- 推文 {i} ---
日期: {tweet['date']}
内容: {tweet['text']}
点赞: {tweet['likes']}
转发: {tweet['retweets']}
图片: {len(tweet['photos'])}张
链接: {tweet['link']}
"""
            if total_chars + len(tweet_text) > 20000:
                break
                
            tweets_text += tweet_text
            total_chars += len(tweet_text)
        
        return tweets_text
    
    def summarize(self, tweets: List[Dict], prompt: str) -> Optional[str]:
        """调用豆包模型"""
        if not tweets:
            return "未提供推文数据"
        
        tweets_text = self.create_tweets_text(tweets)
        full_prompt = f"{prompt}\n{tweets_text}"
        
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": full_prompt}],
            "max_tokens": 4000,
            "temperature": 0.3,
            "top_p": 0.9
        }
        
        try:
            response = requests.post(self.endpoint, headers=self.headers, json=payload, timeout=60)
            
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            else:
                print(f"豆包API失败: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"调用豆包模型出错: {e}")
            return None

# =============================================================================
# 文件处理模块
# =============================================================================

def save_data(data: any, filename: str, output_dir: str = OUTPUT_DIR):
    """保存数据到文件"""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    
    if isinstance(data, (dict, list)):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    else:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(str(data))
    
    print(f"数据已保存: {filepath}")
    return filepath

def print_statistics(tweets: List[Dict]):
    """打印统计信息"""
    if not tweets:
        return
    
    print("\n" + "="*50)
    print("推文统计")
    print("="*50)
    
    print(f"总推文数: {len(tweets)}")
    print(f"总点赞数: {sum(t.get('likes', 0) for t in tweets):,}")
    print(f"总转发数: {sum(t.get('retweets', 0) for t in tweets):,}")
    print(f"含图片推文: {sum(1 for t in tweets if t.get('photos'))}")
    print(f"含视频推文: {sum(1 for t in tweets if t.get('video'))}")
    
    # 日期范围
    dates = [datetime.fromisoformat(t['date'].replace('Z', '+00:00')) for t in tweets]
    if dates:
        print(f"最早推文: {min(dates).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"最新推文: {max(dates).strftime('%Y-%m-%d %H:%M:%S')}")

# =============================================================================
# 主程序
# =============================================================================

def main():
    # 配置检查
    if DOUBAO_API_KEY == "您的火山引擎API Key":
        print("错误：请先配置火山引擎API Key")
        print("请在代码中找到 DOUBAO_API_KEY 变量并替换")
        return
    
    if TARGET_USERNAME == "Mistery5387057":
        print("提示：当前使用默认用户名 Mistery5387057")
        print("如需更改，请修改 TARGET_USERNAME 变量")
    
    print("开始执行推文收集与智能总结...")
    print("="*60)
    
    # 测试网络连接
    if not test_connection():
        print("\n网络连接测试失败，请检查代理设置")
        print("当前代理配置:", PROXY_URL if PROXY_URL.strip() else "无代理")
        return
    
    # 1. 收集推文
    collector = TweetCollector(TARGET_USERNAME, START_DATE, END_DATE)
    tweets = collector.collect_tweets(max_tweets=300)
    
    if not tweets:
        print("\n未能获取推文数据，程序终止")
        return
    
    # 2. 打印统计
    print_statistics(tweets)
    
    # 3. 保存原始数据
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    save_data({
        "metadata": {
            "username": TARGET_USERNAME,
            "collection_time": timestamp,
            "tweet_count": len(tweets),
            "date_range": f"{START_DATE} to {END_DATE}"
        },
        "tweets": tweets
    }, f"{TARGET_USERNAME}_raw_{timestamp}.json")
    
    # 4. 调用豆包总结
    print("\n正在调用豆包模型进行总结...")
    summarizer = DoubaoSummarizer(DOUBAO_API_KEY, DOUBAO_MODEL)
    
    summary = summarizer.summarize(tweets, SUMMARY_PROMPT)
    
    if summary:
        print("\n" + "="*50)
        print("豆包模型总结结果：")
        print("="*50)
        print(summary)
        print("="*50)
        
        save_data(summary, f"{TARGET_USERNAME}_summary_{timestamp}.txt")
        print("\n程序执行完成！")
    else:
        print("\n总结生成失败")

if __name__ == "__main__":
    main()