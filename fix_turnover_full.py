"""
全量修复换手率数据
直接运行 run_backfill.py 重建所有历史数据
"""
import subprocess
import sys
import os

print("="*60)
print("开始全量修复换手率数据")
print("="*60)

# 获取当前脚本所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
backfill_script = os.path.join(current_dir, 'run_backfill.py')

print(f"\n执行脚本: {backfill_script}")
print("预计耗时: 1-3分钟")
print("请耐心等待...\n")
print("-"*60)

try:
    # 直接运行 run_backfill.py
    result = subprocess.run(
        [sys.executable, backfill_script],
        cwd=current_dir,
        capture_output=False,  # 实时显示输出
        text=True
    )
    
    if result.returncode == 0:
        print("\n" + "="*60)
        print("✅ 换手率数据全量修复完成！")
        print("="*60)
        print("\n后续操作：")
        print("1. 刷新 Streamlit 网页")
        print("2. 在宏观页面查看换手率指标")
        print("3. 点击图例显示'换手率>3%占比'和'换手率>5%占比'")
    else:
        print("\n" + "="*60)
        print(f"❌ 修复失败，返回码: {result.returncode}")
        print("="*60)
        
except Exception as e:
    print(f"\n❌ 执行异常: {e}")
    import traceback
    traceback.print_exc()
