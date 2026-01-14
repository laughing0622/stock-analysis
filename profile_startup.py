import time
import sys
import os

print("Starting profile...")

start = time.time()
import pandas as pd
print(f"Import pandas: {time.time() - start:.4f}s")

start = time.time()
import tushare as ts
print(f"Import tushare: {time.time() - start:.4f}s")

start = time.time()
import sklearn.linear_model
print(f"Import sklearn: {time.time() - start:.4f}s")

start = time.time()
try:
    import google.genai
    print(f"Import google.genai: {time.time() - start:.4f}s")
except ImportError:
    print("Import google.genai: Failed (not installed)")

start = time.time()
# Add current directory to path so we can import local modules
sys.path.append(os.getcwd())
import data_engine
print(f"Import data_engine: {time.time() - start:.4f}s")

start = time.time()
engine = data_engine.DataEngine()
print(f"Instantiate DataEngine: {time.time() - start:.4f}s")
