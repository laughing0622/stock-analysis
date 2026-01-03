import os

# 配置代理
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# 测试抓取
import snscrape.modules.twitter as sntwitter

try:
    scraper = sntwitter.TwitterSearchScraper("from:elonmusk since:2024-01-01")
    for i, tweet in enumerate(scraper.get_items()):
        if i >= 3:  # 只测试3条
            break
        print(f" 成功: {tweet.date} - {tweet.content[:50]}...")
    print("修复成功！")
except Exception as e:
    print(f" 失败: {e}")