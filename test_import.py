import sys
print("=== Initial sys.path ===")
for i, p in enumerate(sys.path[:10]):
    print(f"{i}: {p[:80]}")

print("\n=== Importing config ===")
import config
print(f"config module: {config.__file__}")
print(f"config.DB_PATH: {config.DB_PATH}")

print("\n=== Importing data_engine ===")
from data_engine import engine
print("data_engine imported")

print("\n=== Importing pages.tab5_strategies ===")
from pages.tab5_strategies import render_strategies_tab
print("pages.tab5_strategies imported")

print("\n=== Final sys.path ===")
for i, p in enumerate(sys.path[:10]):
    print(f"{i}: {p[:80]}")
