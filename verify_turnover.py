import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_engine import DataEngine, INDEX_MAP
from datetime import datetime

def test_indicators():
    engine = DataEngine()
    
    # Pick a recent trading day
    # Assuming today or yesterday was a trading day. If not, Tushare might return empty, handled by engine.
    # Let's try to find a recent valid date.
    cal = engine.get_trade_cal(days=5)
    if not cal:
        print("Could not get trade calendar")
        return
    
    target_date = cal[-1]
    print(f"Testing for date: {target_date}")
    
    # Test for one index (e.g., Shanghai Index)
    idx_name = '上证指数'
    idx_code = INDEX_MAP[idx_name]
    
    print(f"Testing index: {idx_name} ({idx_code})")
    
    try:
        # Call the modified function
        result = engine.calculate_market_indicators(idx_code, idx_name, target_date)
        
        print("\nResult:")
        print(f"Values returned: {len(result)}")
        print(f"Market Width: {result[0]}%")
        print(f"Panic Sentiment: {result[1]}%")
        if len(result) >= 4:
            print(f"Turnover < 3%: {result[2]}%")
            print(f"Turnover > 5%: {result[3]}%")
        else:
            print("FAILED: Did not return new indicators")
            
    except Exception as e:
        print(f"Error during execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_indicators()
