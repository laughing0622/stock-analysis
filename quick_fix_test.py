"""快速测试换手率修复"""
import sys
sys.path.insert(0, r'd:\stockproject\my work\info')

from data_engine import engine

print("测试换手率数据修复...")
print("调用 update_today_breadth()...")

try:
    engine.update_today_breadth()
    print("更新完成")
except Exception as e:
    print(f"错误: {e}")
    import traceback
    traceback.print_exc()
