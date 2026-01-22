import sys
import os
import sqlite3
import pandas as pd
import numpy as np
import tushare as ts
import requests
import json
from datetime import datetime, timedelta
from db_manager import db
# from tasks.strategy_algo import StrategyAlgo
from utils.symbol_standard import normalize_to_std



# 1. 环境适配
# 【修复乱码】移除强制编码设置，让系统自动适配 Windows/Mac 终端默认编码
# sys.stdout.reconfigure(encoding='utf-8', errors='replace') 
# sys.stderr.reconfigure(encoding='utf-8', errors='replace')
os.environ['no_proxy'] = '*'

# 2. Tushare 设置
TS_TOKEN = '5605c33e633cea87ce20c9cfb7ad82df258c29017b40188a829ef13e'
ts.set_token(TS_TOKEN)
try:
    pro = ts.pro_api()
except:
    pass

# 3. 基础配置
INDEX_MAP = {
    '沪深300': '000300.SH',
    '创业板指': '399006.SZ',
    '中证2000': '932000.CSI', 
    '上证指数': '000001.SH'
}

STRATEGY_POOL = {
    'CORE_B': ['512890.SH', '159949.SZ', '513100.SH', '518880.SH'],
    'ALL': [
        '159552.SZ', '159222.SZ', '159363.SZ', '159217.SZ', '588230.SH',
        '159545.SZ', '159583.SZ', '159562.SZ', '159566.SZ', '513100.SH',
        '518880.SH', '513500.SH', '513400.SH', '513290.SH', '512400.SH',
        '159949.SZ', '159869.SZ', '562500.SH', '159985.SZ', '512890.SH',
        '588110.SH', '159967.SZ', '159502.SZ'
    ]
}

# ==========================================
# 配债策略配置
# ==========================================
CONV_MAX_HOLD = 5
CONV_PB_MIN = 1.0
CONV_MIN_MV = 0           # 亿元
CONV_MAX_MV = 200         # 亿元
CONV_EVENT_STALE = 365    # 天数
CONV_AVOID_MONTHS = []   # 不设置避险月份，info 仅作为选股参考
CONV_WEIGHT_BOND_RATIO = 0.3
CONV_WEIGHT_ISSUE_SIZE = -0.7
CONV_TARGET_STAGES = [
    '发审委/上市委通过', '上市委通过',
    '证监会核准/同意注册', '同意注册'
]
CONV_HOLDING_FILE = os.path.join(os.path.dirname(__file__), 'convertible_holdings.csv')
CONV_CACHE_FILE = os.path.join(os.path.dirname(__file__), 'data', 'convertible_latest.csv')
DEBUG_LOG_PATH = r'd:\stockproject\my work\.cursor\debug.log'
# 可转债低估策略
CB_CACHE_FILE = os.path.join(os.path.dirname(__file__), 'data', 'bond_low_latest.csv')
CB_MAX_SHOW = 10
CB_TOP_STAR = 5

# ==========================================
# 期指分析配置
# ==========================================
FUTURES_VARIETIES = ['IF', 'IC', 'IM', 'IH']
FUTURES_NAME_MAP = {
    'IF': 'IF(沪深300)',
    'IC': 'IC(中证500)',
    'IM': 'IM(中证1000)',
    'IH': 'IH(上证50)'
}

class DataEngine:
    def __init__(self):
        self.db_path = 'data/stock_data.db'
        self.run_id = "pre-fix"

    # ==========================================
    # 调试日志 (NDJSON)
    # ==========================================
    def _agent_log(self, hypothesis_id, location, message, data=None):
        payload = {
            "sessionId": "debug-session",
            "runId": self.run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        try:
            # #region agent log
            with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            # #endregion
        except Exception:
            pass

    # ==========================================
    # 工具函数
    # ==========================================
    def get_trade_cal(self, days=120):
        """获取最近N个交易日 (返回List)"""
        end_d = datetime.now().strftime('%Y%m%d')
        # 往回多取一些以确保覆盖
        start_d = (datetime.now() - timedelta(days=days*2 + 100)).strftime('%Y%m%d')
        try:
            df = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_d, end_date=end_d)
            # 确保按日期升序 (旧 -> 新)
            df = df.sort_values('cal_date', ascending=True)
            return df['cal_date'].values[-days:].tolist()
        except:
            return []

    # ==========================================
    # 模块 A: 宏观择时
    # ==========================================
    def check_breadth_data_exists(self):
        conn = db.get_conn()
        try:
            df = pd.read_sql("SELECT count(*) as cnt FROM market_breadth", conn)
            return df.iloc[0]['cnt'] > 0
        except: return False
        finally: conn.close()

    def init_mock_history(self):
        """初始化真实的历史数据，使用Tushare API获取真实数据"""
        print(">> [Macro] 初始化真实历史数据库...")
        from run_backfill import run_full_backfill
        run_full_backfill()
        print(">> [Macro] 历史数据初始化完成")

    def calculate_crowd_index(self, trade_date):
        """计算真实的拥挤度指标：成交额排名前5%的个股成交额占全部A成交额的比例"""
        try:
            # 获取全部A股的成交额数据
            df_daily = pro.daily(trade_date=trade_date, fields='ts_code,amount')
            if df_daily.empty:
                return 0
            
            # 计算成交额前5%的个股
            total_stocks = len(df_daily)
            top_5_pct_count = max(1, int(total_stocks * 0.05))  # 至少1只
            
            # 按成交额降序排序
            df_sorted = df_daily.sort_values('amount', ascending=False)
            
            # 获取前5%的个股
            top_5_pct = df_sorted.head(top_5_pct_count)
            
            # 计算前5%成交额总和
            top_5_pct_amount = top_5_pct['amount'].sum()
            
            # 计算全部A股成交额总和
            total_amount = df_daily['amount'].sum()
            
            # 计算拥挤度
            if total_amount > 0:
                crowd_index = (top_5_pct_amount / total_amount) * 100
                return round(crowd_index, 2)
            else:
                return 0
        except Exception as e:
            print(f"计算拥挤度异常: {e}")
            # 如果API调用失败或计算出错，返回模拟数据
            return np.random.uniform(30, 55)
    
    def calculate_market_indicators(self, index_code, index_name, trade_date):
        """计算真实的市场指标：市场宽度和恐慌情绪"""
        try:
            # 获取成分股
            stock_list = self._get_index_constituents(index_code, trade_date)
            if not stock_list:
                print(f"   -> {index_name} 无成分股数据")
                return 0, 0, 0, 0
            
            # 计算需要的日期范围
            # MA20需要20天数据，连跌3天需要3天数据
            end_date = trade_date
            start_date = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=25)).strftime('%Y%m%d')  # 多取5天作为缓冲
            
            # 按批次处理成分股，每批最多1000个
            batch_size = 1000
            all_data = []
            
            # 按批次处理成分股，每批最多500个（同时拉取daily和adj_factor）
            batch_size = 500
            all_daily_data = []
            all_adj_data = []
            all_basic_data = []
            
            for i in range(0, len(stock_list), batch_size):
                batch_stocks = stock_list[i:i+batch_size]
                print(f"   -> 拉取第 {i//batch_size+1} 批 {len(batch_stocks)} 只成分股 {start_date} ~ {end_date} 的数据...")
                
                try:
                    # 1. 获取每日行情数据
                    df_daily = pro.daily(ts_code=','.join(batch_stocks), start_date=start_date, end_date=end_date, fields='ts_code,trade_date,close,pct_chg')
                    if not df_daily.empty:
                        all_daily_data.append(df_daily)
                    
                    # 2. 获取复权因子
                    df_adj = pro.adj_factor(ts_code=','.join(batch_stocks), start_date=start_date, end_date=end_date)
                    if not df_adj.empty:
                        all_adj_data.append(df_adj)
                    
                    # 3. 获取每日指标数据 (turnover_rate)
                    # 注意：daily_basic接口批量查询会返回空数据，需要逐个查询
                    for stock_code in batch_stocks:
                        try:
                            df_basic = pro.daily_basic(ts_code=stock_code, start_date=start_date, end_date=end_date, fields='ts_code,trade_date,turnover_rate')
                            if not df_basic.empty:
                                all_basic_data.append(df_basic)
                        except:
                            pass  # 换手率数据获取失败不影响其他指标
                except Exception as e:
                    print(f"   -> 第 {i//batch_size+1} 批数据拉取失败: {e}")
                    continue
            
            if not all_daily_data or not all_adj_data:
                print(f"   -> {index_name} 无足够数据")
                return 0, 0, 0, 0
            
            # 合并所有批次的数据
            df_daily = pd.concat(all_daily_data)
            df_adj = pd.concat(all_adj_data)
            
            # 合并复权因子到每日数据
            df_d = pd.merge(df_daily, df_adj, on=['ts_code', 'trade_date'], how='inner')
            
            # 合并基础数据
            if all_basic_data:
                df_basic = pd.concat(all_basic_data)
                df_d = pd.merge(df_d, df_basic, on=['ts_code', 'trade_date'], how='left')
                df_d['turnover_rate'] = df_d['turnover_rate'].fillna(0)
            else:
                df_d['turnover_rate'] = 0.0
            
            # 计算复权价
            df_d['hfq_close'] = df_d['close'] * df_d['adj_factor']
            
            # 按股票分组计算MA20和连跌3日
            df_d = df_d.sort_values(['ts_code', 'trade_date'])
            
            # 计算MA20：使用rolling(20).mean()，与run_backfill.py保持一致
            df_d['ma20'] = df_d.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20, min_periods=10).mean())
            df_d['is_above_ma20'] = (df_d['hfq_close'] > df_d['ma20']).astype(int)
            
            # 计算连跌3日：使用pct_chg，与run_backfill.py保持一致
            df_d['is_down'] = (df_d['pct_chg'] < 0)
            df_d['down_1'] = df_d.groupby('ts_code')['is_down'].shift(1)
            df_d['down_2'] = df_d.groupby('ts_code')['is_down'].shift(2)
            df_d['is_down_3days'] = (df_d['is_down'] & df_d['down_1'] & df_d['down_2']).astype(int)
            
            # 计算换手率指标
            df_d['is_turnover_lt_3'] = (df_d['turnover_rate'] < 3.0).astype(int)
            df_d['is_turnover_gt_5'] = (df_d['turnover_rate'] > 5.0).astype(int)
            
            # 筛选目标日期的数据
            df_target = df_d[df_d['trade_date'] == trade_date].copy()
            
            # 计算统计指标
            valid_stocks = len(df_target)
            ma20_count = df_target['is_above_ma20'].sum()
            down3_count = df_target['is_down_3days'].sum()
            turnover_lt_3_count = df_target['is_turnover_lt_3'].sum()
            turnover_gt_5_count = df_target['is_turnover_gt_5'].sum()
            
            print(f"   -> 处理 {valid_stocks} 只股票的数据...")
            
            # 计算百分比，使用有效股票数量而不是总成分股数量
            pct_above_ma20 = (ma20_count / valid_stocks) * 100 if valid_stocks > 0 else 0
            pct_down_3days = (down3_count / valid_stocks) * 100 if valid_stocks > 0 else 0
            
            pct_turnover_lt_3 = (turnover_lt_3_count / valid_stocks) * 100 if valid_stocks > 0 else 0
            pct_turnover_gt_5 = (turnover_gt_5_count / valid_stocks) * 100 if valid_stocks > 0 else 0
            
            print(f"   -> {index_name} 有效股票数量: {valid_stocks}, 市场宽度: {pct_above_ma20:.1f}%, 恐慌情绪: {pct_down_3days:.1f}%")
            print(f"      换手率<3%: {pct_turnover_lt_3:.1f}%, 换手率>5%: {pct_turnover_gt_5:.1f}%")
            
            return pct_above_ma20, pct_down_3days, pct_turnover_lt_3, pct_turnover_gt_5
        except Exception as e:
            print(f"计算市场指标异常({index_name}): {e}")
            # 异常情况下返回默认值
            return 50.0, 10.0, 0.0, 0.0
    
    def _get_index_constituents(self, index_code, date_str):
        """获取指数成分股"""
        try:
            if index_code == '000001.SH':
                # 上证指数：获取上交所上市股票
                df = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,list_date')
                df = df[df['list_date'] <= date_str]
                return df['ts_code'].tolist()  # 返回所有成分股
            else:
                # 其他指数：获取指数权重成分股
                start_dt = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=31)).strftime('%Y%m%d')
                df = pro.index_weight(index_code=index_code, start_date=start_dt, end_date=date_str)
                if df.empty:
                    start_dt_long = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=180)).strftime('%Y%m%d')
                    df = pro.index_weight(index_code=index_code, start_date=start_dt_long, end_date=date_str)
                if df.empty:
                    return []
                latest_date = df['trade_date'].max()
                codes = df[df['trade_date'] == latest_date]['con_code'].unique().tolist()
                return codes  # 返回所有成分股
        except Exception as e:
            print(f"获取成分股异常: {e}")
            return []
    
    def _get_trade_dates_between(self, start_date, end_date):
        """获取两个日期之间的所有交易日"""
        try:
            df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
            return sorted(df['cal_date'].tolist())
        except Exception as e:
            print(f"获取交易日历异常: {e}")
            return []
    
    def backfill_missing_data(self):
        """补充缺失的历史数据"""
        print(">> [Macro] 开始补充缺失数据...")
        
        conn = db.get_conn()
        try:
            # 计算需要补充的日期范围
            today = datetime.now().strftime('%Y%m%d')
            print(f"   当前日期: {today}")
            
            # 从run_backfill.py导入全量回填函数
            from run_backfill import run_full_backfill
            
            # 执行全量回填，确保所有指数都有完整的数据
            run_full_backfill()
            
            conn.commit()
            print(f"\n✅ 所有指数缺失数据补充完成")
        except Exception as e:
            print(f"\n❌ 缺失数据补充异常: {e}")
        finally:
            conn.close()
    
    def update_today_breadth(self):
        """更新所有指数的数据，包括：
        1. 更新今日数据
        """
        print(">> [Macro] 开始数据更新...")
        
        # 目标日期：今天
        target_date = datetime.now().strftime('%Y%m%d')
        conn = db.get_conn()
        
        try:
            # 检查今天是否是交易日
            try:
                df_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=target_date, end_date=target_date)
                if df_cal.empty:
                    print(f"   {target_date} 不是交易日，跳过更新")
                    return
            except:
                pass
            
            # 计算今日的拥挤度（所有指数共用）
            crowd_index = self.calculate_crowd_index(target_date)
            
            # 处理所有指数
            for name, code in INDEX_MAP.items():
                print(f"   更新 {name} 今日数据...")
                try:
                    # 获取指数收盘价，特殊处理中证2000指数
                    if code == '932000.CSI':  # 中证2000
                        # 中证2000指数可能没有足够的历史数据，使用更宽松的时间范围
                        df_idx = pro.index_daily(ts_code=code, start_date=target_date, end_date=target_date)
                    else:
                        # 其他指数正常处理
                        df_idx = pro.index_daily(ts_code=code, trade_date=target_date)
                    
                    if df_idx.empty:
                        print(f"   -> {name} 无今日行情数据")
                        continue
                    
                    # 计算真实的市场宽度和恐慌情绪
                    pct_above_ma20, pct_down_3days, pct_turnover_lt_3, pct_turnover_gt_5 = self.calculate_market_indicators(code, name, target_date)
                    
                    # 插入或更新数据
                    conn.execute('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?,?,?)', 
                                (target_date, code, name, float(df_idx.iloc[0]['close']), 
                                 pct_above_ma20, pct_down_3days, crowd_index,
                                 pct_turnover_lt_3, pct_turnover_gt_5))
                    print(f"   -> {name} 今日数据更新完成")
                except Exception as e:
                    print(f"   -> {name} 更新异常: {e}")
                    continue
            
            conn.commit()
            print(f"\n✅ 所有指数今日数据更新完成")
        except Exception as e:
            print(f"\n❌ 今日数据更新异常: {e}")
        finally:
            conn.close()

    def get_breadth_data(self, index_name):
        conn = db.get_conn()
        df = pd.read_sql(f"SELECT * FROM market_breadth WHERE index_name='{index_name}' ORDER BY trade_date", conn)
        conn.close()
        return df

    def update_breadth_incremental(self, start_date=None, max_gap_days=7):
        """
        增量更新宏观择时数据
        Args:
            start_date: 手动指定开始日期(用于补漏)，默认为数据库最新日期+1
            max_gap_days: 最大允许补漏天数，防止意外大量API调用
        """
        print(">> [Macro] 开始增量更新...")
        conn = db.get_conn()

        try:
            # 1. 确定更新日期范围
            if start_date is None:
                latest_query = "SELECT MAX(trade_date) as max_date FROM market_breadth"
                df_latest = pd.read_sql(latest_query, conn)
                latest_date = df_latest.iloc[0]['max_date']

                if latest_date:
                    start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                    print(f"   数据库最新日期: {latest_date}")
                else:
                    print("   数据库为空，需要先执行全量初始化")
                    conn.close()
                    return

            # 2. 获取交易日历
            end_date = datetime.now().strftime('%Y%m%d')
            trade_dates = self.get_trade_cal(days=500)

            # 3. 筛选需要更新的交易日
            dates_to_update = [d for d in trade_dates if d >= start_date and d <= end_date]

            if not dates_to_update:
                print("   ✅ 数据已是最新，无需更新")
                conn.close()
                return

            # 4. 检查是否超过最大补漏天数
            if len(dates_to_update) > max_gap_days:
                error_msg = f"需要更新的天数({len(dates_to_update)})超过限制({max_gap_days}天)，请使用'全量重建'或增大max_gap_days参数"
                print(f"   ⚠️ {error_msg}")
                print(f"   提示: 数据库最新日期为{start_date}，建议执行全量重建")
                conn.close()
                return False

            print(f"   需要更新 {len(dates_to_update)} 个交易日: {dates_to_update[0]} ~ {dates_to_update[-1]}")

            # 5. 批量更新每个交易日
            success_count = 0
            for i, trade_date in enumerate(dates_to_update):
                print(f"   处理第 {i+1}/{len(dates_to_update)} 天: {trade_date}")

                try:
                    # 计算拥挤度
                    crowd_index = self.calculate_crowd_index(trade_date)

                    # 处理所有指数
                    for name, code in INDEX_MAP.items():
                        try:
                            # 获取指数收盘价
                            if code == '932000.CSI':
                                df_idx = pro.index_daily(ts_code=code, start_date=trade_date, end_date=trade_date)
                            else:
                                df_idx = pro.index_daily(ts_code=code, trade_date=trade_date)

                            if df_idx.empty:
                                continue

                            # 计算市场指标
                            pct_above_ma20, pct_down_3days, pct_turnover_lt_3, pct_turnover_gt_5 = \
                                self.calculate_market_indicators(code, name, trade_date)

                            # 插入或更新数据
                            conn.execute('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?,?,?)',
                                         (trade_date, code, name, float(df_idx.iloc[0]['close']),
                                          pct_above_ma20, pct_down_3days, crowd_index,
                                          pct_turnover_lt_3, pct_turnover_gt_5))
                        except Exception as e:
                            print(f"      -> {name} 更新失败: {e}")
                            continue

                    success_count += 1

                    # 每5个交易日提交一次，避免事务过大
                    if (i + 1) % 5 == 0:
                        conn.commit()
                        print(f"      已提交 {success_count}/{i+1} 天")

                except Exception as e:
                    print(f"   处理 {trade_date} 失败: {e}")
                    continue

            conn.commit()
            print(f"\n✅ 增量更新完成，成功更新 {success_count}/{len(dates_to_update)} 天")
            return True

        except Exception as e:
            print(f"\n❌ 增量更新异常: {e}")
            return False
        finally:
            conn.close()

    # ==========================================
    # 模块 B: 日内监控
    # ==========================================
    def _save_daily_nodes(self, date_str, nodes):
        conn = db.get_conn()
        try:
            conn.execute('''INSERT OR IGNORE INTO daily_nodes VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                (date_str, nodes.get('竞价/开盘',0), nodes.get('15分钟',0), nodes.get('30分钟',0), nodes.get('60分钟',0), nodes.get('午盘',0), nodes.get('收盘',0)))
            conn.commit()
        except: pass
        finally: conn.close()

    def _get_nodes_from_db(self, date_str):
        conn = db.get_conn()
        df = pd.read_sql(f"SELECT * FROM daily_nodes WHERE trade_date='{date_str}'", conn)
        conn.close()
        return df.iloc[0].to_dict() if not df.empty else None

    def _fetch_sina_kline(self, symbol):
        try:
            res = requests.get("https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData", 
                             params={"symbol":symbol, "scale":"1", "ma":"no", "datalen":"1200"}, timeout=3).json()
            if res:
                df = pd.DataFrame(res)
                df['time'] = pd.to_datetime(df['day'])
                df['vol_hand'] = pd.to_numeric(df['volume'], errors='coerce')
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                # 成交额(亿元) = 手数 * 收盘价 * 100 / 1e8
                df['amt_100m'] = df['vol_hand'] * df['close'] * 100 / 1e8
                return df[['time', 'vol_hand', 'amt_100m']]
        except Exception as e:
            print(f"   拉取{symbol}失败: {e}")
        return pd.DataFrame()

    def get_minute_data_analysis(self, force_refresh=False):
        """
        获取分钟数据分析
        Args:
            force_refresh: 强制刷新今日数据，忽略缓存
        """
        today = datetime.now().strftime('%Y%m%d')

        # 检查今日数据是否已存在
        if not force_refresh:
            conn = db.get_conn()
            try:
                df_check = pd.read_sql(f"SELECT * FROM daily_nodes WHERE trade_date='{today}'", conn)
                if not df_check.empty:
                    print(f">> [Intraday] 今日 {today} 数据已存在，使用缓存")
                    conn.close()
                    return self._load_minute_data_from_db(today)
            except Exception as e:
                print(f"   检查缓存失败: {e}")
            finally:
                conn.close()

        print(f">> [Intraday] 同步分钟数据...")
        df_sh = self._fetch_sina_kline("sh000001")
        df_sz = self._fetch_sina_kline("sz399001")
        if df_sh.empty or df_sz.empty: return None

    def _load_minute_data_from_db(self, date_str):
        """从数据库加载分钟数据"""
        conn = db.get_conn()
        try:
            # 获取当日节点数据
            df_nodes = pd.read_sql(f"SELECT * FROM daily_nodes WHERE trade_date='{date_str}'", conn)
            if df_nodes.empty:
                return None

            row = df_nodes.iloc[0]
            today_nodes = {
                '竞价/开盘': row['node_open'],
                '15分钟': row['node_15'],
                '30分钟': row['node_30'],
                '60分钟': row['node_60'],
                '午盘': row['node_lunch'],
                '收盘': row['node_close']
            }

            # 获取昨日节点数据
            date_obj = datetime.strptime(date_str, '%Y%m%d').date()
            prev_date_obj = date_obj - timedelta(days=1)
            df_prev = pd.read_sql(f"SELECT * FROM daily_nodes WHERE trade_date='{prev_date_obj.strftime('%Y%m%d')}'", conn)

            yesterday_nodes = {}
            if not df_prev.empty:
                prev_row = df_prev.iloc[0]
                yesterday_nodes = {
                    '竞价/开盘': prev_row['node_open'],
                    '15分钟': prev_row['node_15'],
                    '30分钟': prev_row['node_30'],
                    '60分钟': prev_row['node_60'],
                    '午盘': prev_row['node_lunch'],
                    '收盘': prev_row['node_close']
                }

            return {
                "yesterday_date": prev_date_obj.strftime('%Y-%m-%d') if yesterday_nodes else "无",
                "today_date": date_str,
                "yesterday_nodes": yesterday_nodes,
                "today_nodes": today_nodes,
                "yesterday_curve": pd.DataFrame(columns=['hhmm', 'cumsum']),
                "today_curve": pd.DataFrame(columns=['hhmm', 'cumsum'])
            }
        except Exception as e:
            print(f"   从数据库加载数据失败: {e}")
            return None
        finally:
            conn.close()

    def get_minute_data_analysis_old(self):
        """原版获取分钟数据分析（保留用于对比）"""
        print(f">> [Intraday] 同步分钟数据...")
        df_sh = self._fetch_sina_kline("sh000001")
        df_sz = self._fetch_sina_kline("sz399001")
        if df_sh.empty or df_sz.empty: return None

        df_m = pd.merge(df_sh, df_sz, on='time', how='inner', suffixes=('_sh', '_sz'))
        df_m['amt'] = df_m['amt_100m_sh'] + df_m['amt_100m_sz']

        all_dates = sorted(df_m['time'].dt.date.unique())
        if not all_dates: return None

        curr, last = datetime.now().date(), all_dates[-1]
        t_d = curr if last < curr else last
        # 计算上个交易日
        conn = db.get_conn()
        try:
            # 从数据库获取所有交易日，按日期降序排列
            df_trade_dates = pd.read_sql("SELECT DISTINCT trade_date FROM daily_nodes ORDER BY trade_date DESC", conn)
            # 转换为日期类型
            df_trade_dates['trade_date'] = pd.to_datetime(df_trade_dates['trade_date']).dt.date
            # 获取今天的日期字符串
            today_str = str(t_d)
            # 找出所有在今天之前的交易日
            past_dates = df_trade_dates[df_trade_dates['trade_date'] < t_d]['trade_date'].tolist()
            # 上个交易日是最近的一个过去交易日
            y_d = past_dates[0] if past_dates else t_d - timedelta(days=1)
        except:
            # 如果数据库查询失败，默认使用昨天
            y_d = t_d - timedelta(days=1)
        finally:
            conn.close()

        # 使用昨日官方全市场成交额对新浪数据做口径校准
        try:
            y_str = y_d.strftime('%Y%m%d') if hasattr(y_d, 'strftime') else str(y_d).replace('-', '')
            df_off = pro.daily(trade_date=y_str, fields='amount')
            if not df_off.empty:
                official_amt = df_off['amount'].sum() / 100000  # 千元 -> 亿元
                raw_amt = df_m[df_m['time'].dt.date == y_d]['amt'].sum()
                if official_amt > 0 and raw_amt > 0:
                    scale = official_amt / raw_amt
                    df_m['amt'] = df_m['amt'] * scale
                    print(f"   [Intraday] 校准系数: {scale:.4f}, 昨日官方:{official_amt:.1f}亿, 原始:{raw_amt:.1f}亿")
        except Exception as e:
            print(f"   [Intraday] 校准成交额失败: {e}")

        df_t = df_m[df_m['time'].dt.date == t_d].copy() if last >= curr else pd.DataFrame(columns=['time','amt'])
        df_y = df_m[df_m['time'].dt.date == y_d].copy() if y_d in all_dates else pd.DataFrame(columns=['time','amt'])

        def calc_nodes(df_src, b_date):
            if df_src.empty: return {}, pd.DataFrame(columns=['hhmm', 'cumsum'])
            df_src = df_src.sort_values('time').reset_index(drop=True)
            df_src['hhmm'] = df_src['time'].dt.strftime('%H:%M')
            df_src['cumsum'] = df_src['amt'].cumsum()
            res = {"竞价/开盘": df_src.iloc[0]['amt'] if df_src.iloc[0]['time'].strftime('%H:%M') <= '09:35' else 0}
            for k,v in {"15分钟":"09:45","30分钟":"10:00","60分钟":"10:30","午盘":"11:30","收盘":"15:00"}.items():
                res[k] = df_src.loc[df_src['time']<=pd.to_datetime(f"{b_date} {v}:00"), 'amt'].sum()
            return res, df_src[['hhmm','cumsum']]

        yn, yc = (None, pd.DataFrame(columns=['hhmm', 'cumsum']))
        # 1. 处理上个交易日数据
        # 先尝试从数据库获取（用于缓存），但展示逻辑优先使用最新计算结果
        yn_db = self._get_nodes_from_db(str(y_d))
        if not df_y.empty:
            # 有昨日分钟数据：直接按成交额(亿元)重新计算节点
            yn, yc = calc_nodes(df_y, str(y_d))
            # 只有当收盘成交额大于0时才保存
            if yn.get('收盘', 0) > 0:
                self._save_daily_nodes(str(y_d), yn)
        else:
            # 无昨日分钟数据：退化为使用数据库缓存
            yn = yn_db or {}
            yc = pd.DataFrame(columns=['hhmm', 'cumsum'])
        # 如果昨日节点仍为空，保证为字典，防止显示错误
        if not yn:
            yn = {}

        # 2. 处理今日数据
        tn, tc = calc_nodes(df_t, str(t_d))
        # 只有当有实际成交量时才保存今日数据
        if any(tn.values()):
            self._save_daily_nodes(str(t_d), tn)

        return {"yesterday_date": str(y_d) if y_d else "无", "today_date": str(t_d),
                "yesterday_nodes": yn, "today_nodes": tn, "yesterday_curve": yc, "today_curve": tc}

    # ==========================================
    # 模块 C: 板块宽度 (修复数据获取 + 独立入库)
    # ==========================================
    def update_sector_breadth(self, lookback_days=250, incremental=True):
        """
        更新板块宽度数据
        Args:
            lookback_days: 回溯天数（仅在全量模式下使用）
            incremental: True=增量更新（默认），False=全量重建
        """
        print(f">> [Sector] 启动板块数据更新 ({'增量' if incremental else '全量'}模式)...")
        
        conn = db.get_conn()
        
        # 1. 确定需要更新的日期范围
        if incremental:
            # 增量模式：获取数据库中最新日期
            try:
                latest_query = "SELECT MAX(trade_date) as max_date FROM sector_breadth"
                df_latest = pd.read_sql(latest_query, conn)
                latest_date = df_latest.iloc[0]['max_date']
                
                if latest_date:
                    print(f"   数据库最新日期: {latest_date}")
                    # 从最新日期的下一个交易日开始
                    all_cal = self.get_trade_cal(days=500)  # 获取足够多的交易日
                    if latest_date in all_cal:
                        idx = all_cal.index(latest_date)
                        dates_to_update = all_cal[idx+1:]  # 只更新新增的日期
                    else:
                        dates_to_update = [d for d in all_cal if d > latest_date]
                    
                    if not dates_to_update:
                        print("   ✅ 数据已是最新，无需更新")
                        conn.close()
                        return
                    
                    print(f"   需要增量更新 {len(dates_to_update)} 个交易日")
                    # 计算需要的日期范围（含Buffer算MA20）
                    dates_sorted = sorted(dates_to_update)
                    start_date = dates_sorted[0]
                    end_date = dates_sorted[-1]
                    dates_ui = dates_to_update

                    # 【关键修复】增量更新时，需要额外拉取历史数据来计算MA20
                    # 从第一个需要更新的日期往前推25个交易日（MA20需要20天，多留几天余量）
                    buffer_days = 25
                    if start_date in all_cal:
                        idx = all_cal.index(start_date)
                        start_date = all_cal[max(0, idx - buffer_days)]
                        print(f"   为了计算MA20，实际拉取从 {start_date} 开始的数据")
                else:
                    # 数据库为空，执行全量初始化
                    print("   数据库为空，执行全量初始化...")
                    incremental = False
            except Exception as e:
                print(f"   查询最新日期失败: {e}，转为全量模式")
                incremental = False
        
        if not incremental:
            # 全量模式：按原逻辑处理
            real_lookback = int(lookback_days * 1.6 + 60)
            full_cal = self.get_trade_cal(days=real_lookback)
            if not full_cal: 
                print("   ❌ 无法获取日历")
                conn.close()
                return
                
            dates_sorted = sorted(full_cal)
            start_date = dates_sorted[0]
            end_date = dates_sorted[-1]
            dates_ui = self.get_trade_cal(days=lookback_days)
        
        print(f"   实际拉取数据范围: {start_date} ~ {end_date}")

        # 2. 构建行业映射
        map_l1, map_l2 = {}, {}
        print("   正在同步申万行业成分...")
        try:
            df_l1 = pro.index_classify(level='L1', src='SW2021')
            for l1_code in df_l1['index_code'].tolist():
                try:
                    df_m = pro.index_member_all(l1_code=l1_code, is_new='Y')
                    if df_m.empty: continue
                    for _, row in df_m.iterrows():
                        if row['l1_name']: map_l1[row['ts_code']] = row['l1_name']
                        if row['l2_name']: map_l2[row['ts_code']] = row['l2_name']
                except: continue
            print(f"   已覆盖 {len(map_l1)} 只股票")
        except Exception as e:
            print(f"   映射失败: {e}")
            return

        all_stocks = list(set(list(map_l1.keys()) + list(map_l2.keys())))
        if not all_stocks: return

        # 3. 拉取行情
        chunk_size = 50
        all_data = []
        print(f"   开始拉取行情...")
        for i in range(0, len(all_stocks), chunk_size):
            ts_codes = ",".join(all_stocks[i:i+chunk_size])
            try:
                df_d = pro.daily(ts_code=ts_codes, start_date=start_date, end_date=end_date, fields='ts_code,trade_date,close')
                df_a = pro.adj_factor(ts_code=ts_codes, start_date=start_date, end_date=end_date, fields='ts_code,trade_date,adj_factor')
                if not df_d.empty:
                    if not df_a.empty:
                        df_m = pd.merge(df_d, df_a, on=['ts_code','trade_date'], how='left')
                        df_m['hfq'] = df_m['close'] * df_m['adj_factor'].fillna(1.0)
                        all_data.append(df_m[['ts_code','trade_date','hfq']])
                    else:
                        df_d.rename(columns={'close':'hfq'}, inplace=True)
                        all_data.append(df_d)
            except: pass
            if i > 0 and i % 2000 == 0: print(f"   已处理 {i} 只...")

        if not all_data:
            print("   ❌ 未获取到行情数据")
            return
        
        df_total = pd.concat(all_data)

        # 4. 计算
        print("   计算 MA20...")
        df_close = df_total.pivot(index='ts_code', columns='trade_date', values='hfq')
        df_ma20 = df_close.rolling(window=20, axis=1).mean()
        
        target_cols = [d for d in dates_ui if d in df_close.columns]
        if not target_cols: 
            print("   ❌ 日期范围不匹配")
            return
        
        df_bias = (df_close[target_cols] > df_ma20[target_cols])

        # 5. 入库 (增量模式 vs 全量模式)
        try:
            print("   正在写入数据库...")
            
            if not incremental:
                # 全量模式：清空旧数据
                conn.execute("DELETE FROM sector_breadth")
                print("   [全量模式] 已清空旧数据")
            
            def save(mp, lv):
                if not mp: return
                s = pd.Series(mp)
                valid = s.index.intersection(df_bias.index)
                if len(valid) == 0: return
                
                res = df_bias.loc[valid]
                res.index = res.index.map(s)
                
                final = res.groupby(level=0).mean() * 100
                final = (final + 0.5).astype(int).stack().reset_index()
                final.columns = ['sector_name', 'trade_date', 'pct_above_ma20']
                final['level'] = lv
                
                if incremental:
                    # 增量模式：使用 INSERT OR REPLACE
                    for _, row in final.iterrows():
                        conn.execute(
                            "INSERT OR REPLACE INTO sector_breadth (trade_date, sector_name, level, pct_above_ma20) VALUES (?, ?, ?, ?)",
                            (row['trade_date'], row['sector_name'], row['level'], row['pct_above_ma20'])
                        )
                    print(f"   > {lv} 增量入库 {len(final)} 条")
                else:
                    # 全量模式：直接append
                    final.to_sql('sector_breadth', conn, if_exists='append', index=False)
                    print(f"   > {lv} 全量入库 {len(final)} 条")

            save(map_l1, 'level1')
            save(map_l2, 'level2')
            conn.commit()
            print(f"   [OK] {'增量' if incremental else '全量'}更新完成")
        finally:
            conn.close()

    def get_sector_heatmap_data(self, level='level1', days=30):
        conn = db.get_conn()
        try:
            dates = self.get_trade_cal(days)
            if not dates: return [], [], []
            
            # 拼接 SQL
            date_list_str = "','".join(dates)
            sql = f"SELECT trade_date, sector_name, pct_above_ma20 FROM sector_breadth WHERE level='{level}' AND trade_date IN ('{date_list_str}') ORDER BY trade_date"
            
            df = pd.read_sql(sql, conn)
            if df.empty: return [], [], []
            
            # 透视表 (默认列是按日期升序排列: Old -> New)
            p = df.pivot(index='sector_name', columns='trade_date', values='pct_above_ma20').fillna(0)
            
            # 1. 行业排序: 按最新一天(原本是最后一列)的数值排序
            p = p.sort_values(p.columns[-1], ascending=True)
            
            # 2. 【关键修改】时间轴反转: 将列反向 (New -> Old)
            p = p[p.columns[::-1]]
            
            # 3. 格式化日期
            fmt_dates = [datetime.strptime(d, '%Y%m%d').strftime('%Y-%m-%d') for d in p.columns]
            
            # 返回的数据结构现在是: 日期[新->旧], 行业[弱->强], 矩阵[对应]
            return fmt_dates, p.index.tolist(), p.values.astype(int)
        finally: conn.close()

    # ==========================================
    # 模块 D: 策略实验室 (基于 v20 修复)
    # ==========================================
    def update_strategy_data(self, incremental=True):
        """
        更新 ETF 数据
        Args:
            incremental: True=增量更新（默认），False=全量刷新
        """
        print(f">> [Strategy] 正在同步策略数据 ({'增量' if incremental else '全量'}模式)...")
        pool = STRATEGY_POOL['ALL']
        
        conn = db.get_conn()
        
        # 1. 存名称映射
        try:
            df_basic = pro.fund_basic(market='E')
            name_map = dict(zip(df_basic['ts_code'], df_basic['name']))
            df_names = pd.DataFrame(list(name_map.items()), columns=['ts_code', 'name'])
            df_names.to_sql('etf_names', conn, if_exists='replace', index=False)
        except: pass

        # 2. 检查表结构，确保 adj_factor 字段存在
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(etf_daily)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'adj_factor' not in columns:
                print("   检测到表结构缺少 adj_factor 字段，重建表...")
                incremental = False
        except:
            pass

        if not incremental:
            # 全量模式：强制重建表
            print("   [全量模式] 重建策略数据库表结构...")
            try:
                conn.execute("DROP TABLE IF EXISTS etf_daily")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS etf_daily (
                        ts_code TEXT,
                        trade_date TEXT,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        vol REAL,
                        adj_factor REAL,
                        PRIMARY KEY (ts_code, trade_date)
                    )
                """)
                conn.commit()
            except Exception as e:
                print(f"   重建表失败: {e}")

        # 3. 获取更新日期范围
        today = datetime.now().strftime('%Y%m%d')
        
        for ts_code in pool:
            try:
                if incremental:
                    # 增量模式：查询该ETF的最新日期
                    try:
                        query = f"SELECT MAX(trade_date) as max_date FROM etf_daily WHERE ts_code='{ts_code}'"
                        df_check = pd.read_sql(query, conn)
                        latest_date = df_check.iloc[0]['max_date']
                        
                        if latest_date:
                            # 从最新日期的下一天开始更新
                            start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                            
                            # 如果已是最新，跳过
                            if start_date > today:
                                continue
                        else:
                            # 该ETF无数据，获取最近550天
                            start_date = (datetime.now() - timedelta(days=550)).strftime('%Y%m%d')
                    except:
                        # 查询失败，默认获取最近550天
                        start_date = (datetime.now() - timedelta(days=550)).strftime('%Y%m%d')
                else:
                    # 全量模式：获取最近550天
                    start_date = (datetime.now() - timedelta(days=550)).strftime('%Y%m%d')
                
                # 拉取行情
                df = pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=today)
                if df.empty: 
                    print(f"   {ts_code}: 无行情数据")
                    continue
                
                # 拉取复权因子
                df_adj = pro.fund_adj(ts_code=ts_code, start_date=start_date, end_date=today)
                
                # 合并
                if not df_adj.empty:
                    df = df.merge(df_adj[['trade_date', 'adj_factor']], on='trade_date', how='left')
                    # 关键：填充缺失因子 (ffill)
                    df['adj_factor'] = df['adj_factor'].fillna(method='ffill').fillna(1.0)
                else:
                    df['adj_factor'] = 1.0
                
                # 存入数据库
                # 明确指定列，防止乱序
                df_save = df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'adj_factor']]
                
                if incremental:
                    # 增量模式：使用 INSERT OR REPLACE 避免重复
                    for _, row in df_save.iterrows():
                        conn.execute(
                            """INSERT OR REPLACE INTO etf_daily 
                               (ts_code, trade_date, open, high, low, close, vol, adj_factor) 
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (row['ts_code'], row['trade_date'], row['open'], row['high'], 
                             row['low'], row['close'], row['vol'], row['adj_factor'])
                        )
                    conn.commit()
                    print(f"   {ts_code}: 增量写入 {len(df_save)} 行")
                else:
                    # 全量模式：直接append
                    df_save.to_sql('etf_daily', conn, if_exists='append', index=False)
                    print(f"   {ts_code}: 全量写入 {len(df_save)} 行")
                
            except Exception as e:
                print(f"   Err {ts_code}: {e}")
        
        conn.close()
        print(f"   [策略数据] {'增量' if incremental else '全量'}更新完成")

    # ==========================================
    # 模块 E: 配债事件驱动策略
    # ==========================================
    def _normalize_series(self, s):
        if s.max() == s.min():
            return 1.0
        return (s - s.min()) / (s.max() - s.min())

    def _get_jisilu_prelist(self):
        """抓取集思录待发转债数据"""
        url = f"https://www.jisilu.cn/data/cbnew/pre_list/?___jsl=LST___t={int(datetime.now().timestamp()*1000)}"
        session = requests.Session()
        session.trust_env = False
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.jisilu.cn/data/cbnew/pre_list/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest'
        }
        try:
            res = session.get(url, headers=headers, timeout=10, verify=False)
            if res.status_code != 200:
                self._agent_log("H1", "data_engine:_get_jisilu_prelist", "status_error", {"status": res.status_code})
                return pd.DataFrame()
            data = res.json()
            if 'rows' not in data:
                self._agent_log("H1", "data_engine:_get_jisilu_prelist", "schema_error", {"sample": res.text[:120]})
                return pd.DataFrame()
            rows = data.get('rows', [])
            parsed = []
            for item in rows:
                cell = item.get('cell', {})
                # 标准化代码
                raw_code = item.get('id')
                std_code = normalize_to_std(raw_code)
                parsed.append({
                    'stock_code': std_code,
                    'stock_name': cell.get('stock_nm'),
                    'issue_size': float(cell.get('amount', 0) or 0),
                    'bond_ratio': float(cell.get('cb_amount', 0) or 0),
                    'progress': cell.get('progress_nm'),
                    'progress_date': cell.get('progress_dt'),
                    'pb_jisilu': float(cell.get('pb', 0) or 0)
                })
            df = pd.DataFrame(parsed)
            self._agent_log("H1", "data_engine:_get_jisilu_prelist", "fetch_ok", {"rows": len(df)})
            return df
        except Exception as e:
            self._agent_log("H1", "data_engine:_get_jisilu_prelist", "exception", {"err": str(e)})
            return pd.DataFrame()

    def _calc_convertible_strategy(self):
        """执行配债事件驱动选股，返回结果和计划"""
        now = datetime.now()
        if now.month in CONV_AVOID_MONTHS:
            self._agent_log("H2", "data_engine:_calc_convertible_strategy", "timing_block", {"month": now.month})
            return {
                "timing_safe": False,
                "timing_msg": f"当前为避险月份({now.month})，空仓",
                "df": pd.DataFrame(),
                "counts": {},
                "plan": {"sell": [], "buy": [], "hold": []}
            }

        df = self._get_jisilu_prelist()
        raw_cnt = len(df)
        if df.empty:
            return {"timing_safe": True, "timing_msg": "无数据", "df": pd.DataFrame(), "counts": {"raw": 0}, "plan": {"sell": [], "buy": [], "hold": []}}

        # 阶段筛选与僵尸过滤
        df = df[df['progress'].isin(CONV_TARGET_STAGES)].copy()
        df['progress_date'] = pd.to_datetime(df['progress_date'])
        df['days_diff'] = (now - df['progress_date']).dt.days
        df = df[df['days_diff'] <= CONV_EVENT_STALE]
        stage_cnt = len(df)

        # 基本面筛选
        df = df[df['bond_ratio'] > 0].copy()
        df['market_cap'] = df['issue_size'] / (df['bond_ratio'] / 100)
        df['pb'] = df['pb_jisilu']
        df = df[df['pb'] >= CONV_PB_MIN]
        df = df[(df['market_cap'] >= CONV_MIN_MV) & (df['market_cap'] <= CONV_MAX_MV)]
        fund_cnt = len(df)

        if df.empty:
            self._agent_log("H2", "data_engine:_calc_convertible_strategy", "empty_after_filter", {"raw": raw_cnt, "stage": stage_cnt})
            return {"timing_safe": True, "timing_msg": "过滤后无标的", "df": pd.DataFrame(), "counts": {"raw": raw_cnt, "stage": stage_cnt, "fund": 0}, "plan": {"sell": [], "buy": [], "hold": []}}

        # 打分
        df['norm_ratio'] = self._normalize_series(df['bond_ratio'])
        df['norm_size'] = self._normalize_series(df['issue_size'])
        df['score'] = CONV_WEIGHT_BOND_RATIO * df['norm_ratio'] + CONV_WEIGHT_ISSUE_SIZE * df['norm_size']
        df_sorted = df.sort_values(by=['score', 'bond_ratio'], ascending=[False, False])
        df_top = df_sorted.head(CONV_MAX_HOLD).copy()
        df_display = df_sorted.head(CONV_MAX_HOLD*2).reset_index(drop=True)  # 前5+后5，共10
        df_display['rank_group'] = df_display.index.to_series().apply(lambda i: "TOP5" if i < CONV_MAX_HOLD else "NEXT5")
        df_display['is_star'] = df_display['rank_group'].eq("TOP5")

        # 仓位计划
        old_stocks = []
        if os.path.exists(CONV_HOLDING_FILE):
            try:
                old_df = pd.read_csv(CONV_HOLDING_FILE, dtype={'stock_code': str})
                old_stocks = old_df['stock_code'].tolist()
            except Exception:
                old_stocks = []
        new_stocks = df_top['stock_code'].astype(str).tolist()
        sell_list = [c for c in old_stocks if c not in new_stocks]
        buy_list = [c for c in new_stocks if c not in old_stocks]
        plan = {"sell": sell_list, "buy": buy_list, "hold": new_stocks}

        # 保存持仓
        try:
            df_top.to_csv(CONV_HOLDING_FILE, index=False, encoding='utf-8-sig')
        except Exception as e:
            self._agent_log("H2", "data_engine:_calc_convertible_strategy", "save_hold_err", {"err": str(e)})

        # 缓存结果
        try:
            df_display.to_csv(CONV_CACHE_FILE, index=False, encoding='utf-8-sig')
        except Exception as e:
            self._agent_log("H2", "data_engine:_calc_convertible_strategy", "save_cache_err", {"err": str(e)})

        self._agent_log("H2", "data_engine:_calc_convertible_strategy", "calc_ok", {
            "raw": raw_cnt, "stage": stage_cnt, "fund": fund_cnt, "top": len(df_top)
        })

        return {
            "timing_safe": True,
            "timing_msg": "正常",
            "df": df_display,
            "counts": {"raw": raw_cnt, "stage": stage_cnt, "fund": fund_cnt},
            "plan": plan
        }

    def update_convertible_strategy(self):
        """手动刷新配债策略"""
        self.run_id = "pre-fix"
        return self._calc_convertible_strategy()

    def get_convertible_strategy_rank(self):
        """获取配债策略结果（优先用缓存，没有则现算）"""
        if os.path.exists(CONV_CACHE_FILE):
            try:
                df = pd.read_csv(CONV_CACHE_FILE)
                if not df.empty:
                    # 兼容旧缓存：补齐新列
                    if 'rank_group' not in df.columns:
                        df['rank_group'] = ['TOP5'] * min(len(df), CONV_MAX_HOLD) + ['NEXT5'] * max(0, len(df)-CONV_MAX_HOLD)
                    if 'is_star' not in df.columns:
                        df['is_star'] = df['rank_group'].eq('TOP5')
                    self._agent_log("H3", "data_engine:get_convertible_strategy_rank", "use_cache", {"rows": len(df)})
                    return {
                        "timing_safe": True,
                        "timing_msg": "缓存结果",
                        "df": df,
                        "counts": {},
                        "plan": {"sell": [], "buy": [], "hold": df['stock_code'].astype(str).tolist()}
                    }
            except Exception as e:
                self._agent_log("H3", "data_engine:get_convertible_strategy_rank", "cache_err", {"err": str(e)})
        # 无缓存或失败则现算
        return self._calc_convertible_strategy()

    # ==========================================
    # 模块 F: 可转债低估策略（双低/多普勒三低）
    # ==========================================
    def _get_jsl_redeem_list(self):
        url = f"https://www.jisilu.cn/data/cbnew/redeem_list/?___jsl=LST___t={int(datetime.now().timestamp()*1000)}"
        session = requests.Session()
        session.trust_env = False
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.jisilu.cn/data/cbnew/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest'
        }
        try:
            res = session.get(url, headers=headers, timeout=10, verify=False)
            if res.status_code != 200:
                self._agent_log("H4", "data_engine:_get_jsl_redeem_list", "status_error", {"status": res.status_code})
                return pd.DataFrame()
            data = res.json()
            if 'rows' not in data:
                self._agent_log("H4", "data_engine:_get_jsl_redeem_list", "schema_error", {"sample": res.text[:120]})
                return pd.DataFrame()
            rows = [x.get('cell', {}) for x in data.get('rows', [])]
            df = pd.DataFrame(rows)
            self._agent_log("H4", "data_engine:_get_jsl_redeem_list", "fetch_ok", {"rows": len(df)})
            return df
        except Exception as e:
            self._agent_log("H4", "data_engine:_get_jsl_redeem_list", "exception", {"err": str(e)})
            return pd.DataFrame()

    def _calc_bond_low_strategy(self):
        df = self._get_jsl_redeem_list()
        raw_cnt = len(df)
        if df.empty:
            return {"df": pd.DataFrame(), "counts": {"raw": 0}}

        # 必要字段缺失则返回
        # 尝试补全缺失字段
        # premium_rt 缺失时用转股价值计算：conv = sprice/convert_price*100
        if 'premium_rt' not in df.columns:
            if 'sprice' in df.columns and 'convert_price' in df.columns and 'price' in df.columns:
                conv = pd.to_numeric(df['sprice'], errors='coerce') / pd.to_numeric(df['convert_price'], errors='coerce') * 100
                df['premium_rt'] = (pd.to_numeric(df['price'], errors='coerce') / conv - 1) * 100
            else:
                df['premium_rt'] = np.nan
        if 'increase_rt' not in df.columns:
            df['increase_rt'] = np.nan  # 缺失则置空，不做强制退出

        required = ['bond_id','bond_nm','price','premium_rt','curr_iss_amt','increase_rt','sprice','redeem_flag','redeem_icon']
        missing = [c for c in required if c not in df.columns]
        if missing:
            self._agent_log("H4", "data_engine:_calc_bond_low_strategy", "missing_cols", {"missing": missing, "cols": list(df.columns)})
            return {"df": pd.DataFrame(), "counts": {"raw": raw_cnt, "usable": 0}}

        df = df.copy()
        df['bond_id'] = df['bond_id'].astype(str).apply(normalize_to_std)
        df['increase_rt'] = pd.to_numeric(df['increase_rt'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['premium_rt'] = pd.to_numeric(df['premium_rt'], errors='coerce')
        df['curr_iss_amt'] = pd.to_numeric(df['curr_iss_amt'], errors='coerce')
        df['sprice'] = pd.to_numeric(df['sprice'], errors='coerce')
        df['convert_price'] = pd.to_numeric(df.get('convert_price', np.nan), errors='coerce')
        if 'redeem_real_days' in df.columns:
            df['redeem_real_days'] = pd.to_numeric(df['redeem_real_days'], errors='coerce')
        if 'redeem_count_days' in df.columns:
            df['redeem_count_days'] = pd.to_numeric(df['redeem_count_days'], errors='coerce')

        # 过滤现价异常（0 或 100，通常是新债/待发行）
        df = df[(df['price'] > 0) & (df['price'] != 100)]

        # 重新计算转股溢价率以对齐参考脚本：转股价值=正股价/转股价*100
        conv_value = df['sprice'] / df['convert_price'] * 100
        df['转股溢价率'] = df['price'] / conv_value - 1
        # 集思录双低=转债价格+转股溢价率*100
        df['集思录双低'] = df['price'] + 100 * df['转股溢价率']

        # 强赎状态映射
        df['强赎状态'] = df['redeem_flag'] + df['redeem_icon']
        if 'redeem_real_days' in df.columns:
            df.loc[df['redeem_real_days'] > 0, '强赎状态'] = '两周强赎'
            df.loc[df['redeem_real_days'] > 6, '强赎状态'] = '即将强赎'
        df.loc[df['强赎状态'] == 'YR', '强赎状态'] = '已公告强赎'
        df.loc[df['强赎状态'] == 'XB', '强赎状态'] = '已满足强赎'
        df.loc[df['强赎状态'] == 'NG', '强赎状态'] = '公告不强赎'
        df.loc[df['强赎状态'] == 'X', '强赎状态'] = ''

        # 过滤强赎/NR
        df = df[~df['强赎状态'].isin(['NR','XR','已公告强赎','已满足强赎','即将强赎','两周强赎'])]
        usable_cnt = len(df)
        if df.empty:
            self._agent_log("H4", "data_engine:_calc_bond_low_strategy", "empty_after_redeem_filter", {"raw": raw_cnt})
            return {"df": pd.DataFrame(), "counts": {"raw": raw_cnt, "usable": 0}}

        # 计算双低 / 多普勒三低 (2:3:2) 对齐参考脚本
        df['转股溢价率_得分'] = df['转股溢价率'].rank(ascending=True)
        df['收盘价_得分'] = df['price'].rank(ascending=True)
        df['剩余规模_得分'] = df['curr_iss_amt'].rank(ascending=True)
        df['多普勒三低'] = df['转股溢价率_得分'] * 2 + df['剩余规模_得分'] * 3 + df['收盘价_得分'] * 2

        # 排序取前10
        df_sorted = df.sort_values('多普勒三低', ascending=True).reset_index(drop=True)
        df_show = df_sorted.head(CB_MAX_SHOW).copy()
        df_show['rank_group'] = df_show.index.to_series().apply(lambda i: "TOP5" if i < CB_TOP_STAR else "NEXT5")
        df_show['is_star'] = df_show['rank_group'].eq("TOP5")

        # 选取展示列并重命名
        df_show = df_show.rename(columns={
            'bond_id': '代码',
            'bond_nm': '名称',
            'increase_rt': '涨跌幅%',
            'price': '现价',
            'curr_iss_amt': '剩余规模'
        })
        df_show = df_show[['代码','名称','涨跌幅%','现价','转股溢价率','剩余规模','集思录双低','多普勒三低','强赎状态','rank_group','is_star']]

        # 缓存
        try:
            df_show.to_csv(CB_CACHE_FILE, index=False, encoding='utf-8-sig')
        except Exception as e:
            self._agent_log("H4", "data_engine:_calc_bond_low_strategy", "cache_err", {"err": str(e)})

        self._agent_log("H4", "data_engine:_calc_bond_low_strategy", "calc_ok", {"raw": raw_cnt, "usable": usable_cnt, "show": len(df_show)})
        return {"df": df_show, "counts": {"raw": raw_cnt, "usable": usable_cnt}}

    def update_bond_low_strategy(self):
        """刷新可转债低估策略"""
        self.run_id = "pre-fix"
        return self._calc_bond_low_strategy()

    def get_bond_low_strategy(self):
        """获取可转债低估策略（缓存优先）"""
        if os.path.exists(CB_CACHE_FILE):
            try:
                df = pd.read_csv(CB_CACHE_FILE)
                if not df.empty:
                    if 'rank_group' not in df.columns:
                        df['rank_group'] = df.index.to_series().apply(lambda i: "TOP5" if i < CB_TOP_STAR else "NEXT5")
                    if 'is_star' not in df.columns:
                        df['is_star'] = df['rank_group'].eq("TOP5")
                    self._agent_log("H4", "data_engine:get_bond_low_strategy", "use_cache", {"rows": len(df)})
                    return {"df": df, "counts": {}}
            except Exception as e:
                self._agent_log("H4", "data_engine:get_bond_low_strategy", "cache_err", {"err": str(e)})
        return self._calc_bond_low_strategy()

    def get_strategy_rank(self):
        conn = db.get_conn()
        
        # 读取名称映射
        try:
            df_names = pd.read_sql("SELECT * FROM etf_names", conn)
            name_map = dict(zip(df_names['ts_code'], df_names['name']))
        except: name_map = {}
        
        from tasks.strategy_algo import StrategyAlgo
        algo = StrategyAlgo()
        res = []
        pool = STRATEGY_POOL['ALL']
        core_b = STRATEGY_POOL['CORE_B']
        
        for code in pool:
            try:
                # 读取数据
                df = pd.read_sql(f"SELECT * FROM etf_daily WHERE ts_code='{code}' ORDER BY trade_date", conn)
                if df.empty: continue
                
                # 0. 计算前复权 (Close QFQ)
                if 'adj_factor' not in df.columns: df['adj_factor'] = 1.0
                latest_adj = df.iloc[-1]['adj_factor']
                if latest_adj == 0 or pd.isna(latest_adj): latest_adj = 1.0
                df['close_qfq'] = df['close'] * (df['adj_factor'] / latest_adj)
                
                # 1. 策略A 计算 (含硬风控)
                # 硬风控检查
                is_safe, risk_msg = algo.check_risk_control(df['close_qfq'].values)
                # WLS 计算
                score_a, ret_a, r2_a = algo.calc_strategy_a_wls(df, m_days=25)
                
                # 策略A 状态判定 (保持原逻辑)
                status_a = "是"
                if not is_safe:
                    status_a = f"否 ({risk_msg})"
                elif score_a > 6.0:
                    status_a = "否 (疯牛)"
                elif score_a <= 0:
                    status_a = "否 (趋势弱)"
                
                # 2. 策略B 计算 (因子)
                factors_b = algo.calc_strategy_b_factors(df)
                
                # 3. 构造显示名称
                cn_name = name_map.get(code, code)
                star = "⭐" if code in core_b else ""
                display_name = f"{code}  {cn_name} {star}"

                res.append({
                    'code': code,
                    'raw_name': cn_name,
                    'display_name': display_name,
                    'is_core': code in core_b,
                    'score_a': score_a,
                    'status_a': status_a, # 策略A保留状态
                    'factors_b': factors_b
                })
            except Exception as e:
                print(f"   计算 {code} 失败: {e}")
                continue
            
        conn.close()
        
        if not res: return pd.DataFrame()
        df_res = pd.DataFrame(res)
        
        # 4. 策略B Z-Score 标准化 (纯打分，不进行风控过滤)
        valid_b = df_res[df_res['factors_b'].notnull()].copy()
        if not valid_b.empty:
            mat = np.array(valid_b['factors_b'].tolist())
            means = mat.mean(axis=0)
            stds = mat.std(axis=0) + 1e-9
            z = (mat - means) / stds
            # 权重: 0.3, 0.3, 0.4
            scores = z[:,0]*0.3 + z[:,1]*0.3 + z[:,2]*0.4
            df_res.loc[valid_b.index, 'score_b'] = scores
        else:
            df_res['score_b'] = -999
            
        # 5. 生成最终表格
        final_rows = []
        for _, row in df_res.iterrows():
            final_rows.append({
                "标的": row['display_name'],
                "策略A_得分": row['score_a'],
                "策略A_入选": row['status_a'], # 列名定死为 '策略A_入选'
                "策略B_得分": row['score_b'],
                # 策略B 不需要状态列
                "_raw_name": row['raw_name'],
                "_is_core": row['is_core']
            })
            
        return pd.DataFrame(final_rows)
    
    def save_intraday_data(self):
        """保存日内数据到数据库，用于自动运行"""
        print(f">> [Intraday Auto] 保存日内数据...")
        df_sh = self._fetch_sina_kline("sh000001")
        df_sz = self._fetch_sina_kline("sz399001")
        if df_sh.empty or df_sz.empty:
            print("   -> 新浪API无数据，跳过")
            return
        
        df_m = pd.merge(df_sh, df_sz, on='time', how='inner', suffixes=('_sh', '_sz'))
        df_m['vol'] = (df_m['vol_hand_sh'] + df_m['vol_hand_sz']) / 10000 
        
        all_dates = sorted(df_m['time'].dt.date.unique())
        if not all_dates: return
        
        curr, last = datetime.now().date(), all_dates[-1]
        t_d = curr if last < curr else last
        
        df_t = df_m[df_m['time'].dt.date == t_d].copy()
        if df_t.empty:
            print("   -> 今日无数据，跳过")
            return
        
        def calc_nodes(df_src, b_date):
            if df_src.empty: return {}, pd.DataFrame(columns=['hhmm', 'cumsum'])
            df_src = df_src.sort_values('time').reset_index(drop=True)
            df_src['hhmm'] = df_src['time'].dt.strftime('%H:%M')
            df_src['cumsum'] = df_src['amt'].cumsum()
            res = {"竞价/开盘": df_src.iloc[0]['amt'] if df_src.iloc[0]['time'].strftime('%H:%M') <= '09:35' else 0}
            for k,v in {"15分钟":"09:45","30分钟":"10:00","60分钟":"10:30","午盘":"11:30","收盘":"15:00"}.items():
                res[k] = df_src.loc[df_src['time']<=pd.to_datetime(f"{b_date} {v}:00"), 'amt'].sum()
            return res, df_src[['hhmm','cumsum']]
        
        # 计算今日节点数据
        tn, tc = calc_nodes(df_t, str(t_d))
        # 只有当有实际成交量时才保存
        if any(tn.values()):
            self._save_daily_nodes(str(t_d), tn)
            print(f"   -> 已保存今日({t_d})数据")
        else:
            print("   -> 今日无有效成交量，跳过保存")

    # ==========================================
    # 模块 E: 期指分析
    # ==========================================
    def get_futures_smart_date(self):
        """获取期指分析的智能日期"""
        now = datetime.now()
        current_hour = now.hour
        today_str = now.strftime('%Y%m%d')
        start_search = (now - timedelta(days=60)).strftime('%Y%m%d')
        
        try:
            cal_df = pro.trade_cal(exchange='SSE', is_open='1', start_date=start_search, end_date=today_str, fields='cal_date')
            cal_df = cal_df.sort_values('cal_date')
            trade_dates = cal_df['cal_date'].tolist()
            if len(trade_dates) < 5:
                return None, None
            
            # 17点后算当日收盘，否则取前一日
            if today_str in trade_dates:
                target_idx = -1 if current_hour >= 17 else -2
            else:
                target_idx = -1
            return trade_dates[target_idx], trade_dates[target_idx - 1]
        except:
            return None, None
    
    def get_futures_contract_info(self):
        """获取期货合约基础信息(主要是交割日)"""
        info_map = {}
        try:
            df = pro.fut_basic(exchange='CFFEX', fut_type='1', fields='symbol,delist_date')
            for _, row in df.iterrows():
                info_map[row['symbol']] = row['delist_date']
        except Exception as e:
            print(f"获取合约信息失败: {e}")
        return info_map
    
    def get_futures_holdings(self, date_str, varieties=None):
        """获取某日的期货持仓结构数据
        返回: { 'IF': { 'total_net': -100, 'contracts': [{'symbol': 'IF2512', 'net': -50}, ...] } }
        """
        if varieties is None:
            varieties = FUTURES_VARIETIES
        
        data = {}
        try:
            df = pro.fut_holding(trade_date=date_str, exchange='CFFEX')
            if df.empty:
                return {}
            
            for v in varieties:
                df_v = df[df['symbol'].str.contains(f'^{v}')]
                if df_v.empty:
                    continue
                
                # 筛选中信机构
                citic = df_v[df_v['broker'].str.contains('中信', na=False)]
                
                # 计算各合约净单
                contract_details = []
                all_symbols = df_v['symbol'].unique()
                total_net = 0
                
                for sym in all_symbols:
                    record = citic[citic['symbol'] == sym]
                    net = 0
                    if not record.empty:
                        net = record['long_hld'].sum() - record['short_hld'].sum()
                    
                    contract_details.append({
                        'symbol': sym,
                        'net': int(net)
                    })
                    total_net += net
                
                data[v] = {
                    'total_net': int(total_net),
                    'contracts': contract_details
                }
        except Exception as e:
            print(f"获取持仓数据失败: {e}")
        return data
    
    def analyze_futures_position_change(self, target_date, prev_date):
        """分析期指调仓行为
        返回: {
            'target_date': str,
            'prev_date': str,
            'delist_map': dict,
            'varieties': { 'IF': {...}, ... }
        }
        """
        if not target_date or not prev_date:
            return None
        
        delist_map = self.get_futures_contract_info()
        data_now = self.get_futures_holdings(target_date)
        data_prev = self.get_futures_holdings(prev_date)
        
        result = {
            'target_date': target_date,
            'prev_date': prev_date,
            'delist_map': delist_map,
            'varieties': {}
        }
        
        for v in FUTURES_VARIETIES:
            if v not in data_now or v not in data_prev:
                continue
            
            # 合约详情
            now_contracts = {item['symbol']: item['net'] for item in data_now[v]['contracts']}
            prev_contracts = {item['symbol']: item['net'] for item in data_prev[v]['contracts']}
            all_symbols = sorted(list(set(now_contracts.keys()) | set(prev_contracts.keys())))
            
            contracts = []
            for sym in all_symbols:
                net_now = now_contracts.get(sym, 0)
                net_prev = prev_contracts.get(sym, 0)
                change = net_now - net_prev
                
                # 判断状态
                status = ""
                if abs(change) > 500:
                    status = "大幅平空/加多" if change > 0 else "大幅加空"
                elif abs(change) > 100:
                    status = "平空" if change > 0 else "加空"
                
                contracts.append({
                    'symbol': sym,
                    'delist_date': delist_map.get(sym, '未知'),
                    'net_now': net_now,
                    'net_prev': net_prev,
                    'change': change,
                    'status': status
                })
            
            # 总体情况
            total_now = data_now[v]['total_net']
            total_prev = data_prev[v]['total_net']
            total_change = total_now - total_prev
            
            # 生成解读
            interpretation = self._interpret_futures_change(total_now, total_change, contracts)
            
            result['varieties'][v] = {
                'total_net': total_now,
                'total_change': total_change,
                'contracts': contracts,
                'interpretation': interpretation
            }
        
        return result
    
    def _interpret_futures_change(self, total_net, total_change, contracts):
        """根据持仓变化生成解读文字"""
        lines = []
        
        # 总体态度
        if total_net > 0:
            attitude = "净多头"
        elif total_net < 0:
            attitude = "净空头"
        else:
            attitude = "中性"
        
        # 变动趋势
        if total_change > 500:
            trend = "大幅减空" if total_net < 0 else "大幅加多"
        elif total_change > 100:
            trend = "减空" if total_net < 0 else "加多"
        elif total_change < -500:
            trend = "大幅加空" if total_net < 0 else "大幅减多"
        elif total_change < -100:
            trend = "加空" if total_net < 0 else "减多"
        else:
            trend = "维持观察"
        
        lines.append(f"中信持仓{attitude}({total_net}手), {trend}({total_change:+d}手)")
        
        # 检测移仓行为
        old_contracts = [c for c in contracts if c['change'] > 500]  # 大幅平仓
        new_contracts = [c for c in contracts if c['change'] < -500]  # 大幅开仓
        
        if old_contracts and new_contracts:
            if total_change > 0:
                lines.append("移仓过程丢弃空单 → 真实看多信号")
            else:
                lines.append("检测到合约间调仓，但总净单下降")
        
        return " | ".join(lines)

    def update_futures_holdings_history(self, start_date=None, max_gap_days=7):
        """
        增量更新期指持仓历史数据
        Args:
            start_date: 手动指定开始日期，默认为数据库最新日期+1
            max_gap_days: 最大允许补漏天数
        """
        print(">> [Futures] 开始增量更新持仓历史...")
        conn = db.get_conn()

        try:
            # 1. 确定更新日期范围
            if start_date is None:
                latest_query = "SELECT MAX(trade_date) as max_date FROM futures_holdings_history"
                df_latest = pd.read_sql(latest_query, conn)
                latest_date = df_latest.iloc[0]['max_date']

                if latest_date:
                    start_date = (datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
                    print(f"   数据库最新日期: {latest_date}")
                else:
                    print("   数据库为空，从最近60个交易日开始")
                    trade_dates = self.get_trade_cal(days=60)
                    if not trade_dates:
                        conn.close()
                        return
                    start_date = trade_dates[0]

            # 2. 获取交易日历
            end_date = datetime.now().strftime('%Y%m%d')
            trade_dates = self.get_trade_cal(days=500)

            # 3. 筛选需要更新的交易日
            dates_to_update = [d for d in trade_dates if d >= start_date and d <= end_date]

            if not dates_to_update:
                print("   ✅ 期指数据已是最新，无需更新")
                conn.close()
                return

            # 4. 检查是否超过最大补漏天数
            if len(dates_to_update) > max_gap_days:
                error_msg = f"需要更新的天数({len(dates_to_update)})超过限制({max_gap_days}天)，请使用'全量重建'或增大max_gap_days参数"
                print(f"   ⚠️ {error_msg}")
                print(f"   提示: 数据库最新日期为{start_date}，建议执行全量重建")
                conn.close()
                return False

            print(f"   需要更新 {len(dates_to_update)} 个交易日: {dates_to_update[0]} ~ {dates_to_update[-1]}")

            # 5. 批量更新每个交易日
            success_count = 0
            for i, trade_date in enumerate(dates_to_update):
                print(f"   处理第 {i+1}/{len(dates_to_update)} 天: {trade_date}")

                try:
                    # 获取当日持仓数据
                    data = self.get_futures_holdings(trade_date)
                    if not data:
                        print(f"      当日无数据，跳过")
                        continue

                    # 获取前一交易日持仓数据
                    if i == 0:
                        # 如果是第一天，需要单独获取前一日的数据
                        prev_date = None
                        idx = trade_dates.index(trade_date)
                        if idx > 0:
                            prev_date = trade_dates[idx - 1]
                    else:
                        prev_date = dates_to_update[i - 1]

                    if prev_date:
                        data_prev = self.get_futures_holdings(prev_date)
                    else:
                        data_prev = {}

                    # 存储当日数据
                    for variety, info in data.items():
                        for contract in info['contracts']:
                            symbol = contract['symbol']
                            net = contract['net']

                            # 计算变化量
                            change_net = 0
                            if prev_date and variety in data_prev:
                                prev_contracts = {c['symbol']: c['net'] for c in data_prev[variety]['contracts']}
                                prev_net = prev_contracts.get(symbol, 0)
                                change_net = net - prev_net

                            # 插入或更新数据
                            conn.execute(
                                'INSERT OR REPLACE INTO futures_holdings_history VALUES (?,?,?,?,?,?)',
                                (trade_date, variety, symbol, net, 0, change_net)
                            )

                    success_count += 1

                    # 每3个交易日提交一次
                    if (i + 1) % 3 == 0:
                        conn.commit()
                        print(f"      已提交 {success_count}/{i+1} 天")

                except Exception as e:
                    print(f"   处理 {trade_date} 失败: {e}")
                    continue

            conn.commit()
            print(f"\n✅ 期指持仓历史增量更新完成，成功更新 {success_count}/{len(dates_to_update)} 天")
            return True

        except Exception as e:
            print(f"\n❌ 期指持仓历史增量更新异常: {e}")
            return False
        finally:
            conn.close()

    def get_stock_daily(self, ts_code, days=250):
        """
        获取个股日线数据（复权后）
        """
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days * 1.5)).strftime('%Y%m%d')  # 多取一些保证交易日足够

            # 1. 获取日线行情
            df_daily = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df_daily.empty:
                print(f"   {ts_code} 无日线数据")
                return pd.DataFrame()

            # 2. 获取复权因子
            df_adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            # 3. 数据处理
            # 统一日期格式
            df_daily = df_daily.sort_values('trade_date')
            
            if not df_adj.empty:
                df_merge = pd.merge(df_daily, df_adj, on=['ts_code', 'trade_date'], how='left')
                # 填充最新的复权因子到缺失值 (假设最近几天没有除权除息，因子不变)
                df_merge['adj_factor'] = df_merge['adj_factor'].fillna(method='ffill').fillna(1.0)
            else:
                df_merge = df_daily.copy()
                df_merge['adj_factor'] = 1.0

            # 计算后复权价格 (以前复权为准其实更符合看盘习惯，但这里计算后复权作为绝对值参考，或者计算前复权给画图)
            # 画图通常使用前复权 (QFQ)。这里我们计算前复权。
            # QFQ = P * (Adj / Latest_Adj)
            latest_adj = df_merge['adj_factor'].iloc[-1]
            if latest_adj == 0: latest_adj = 1.0
            
            # 计算前复权数据
            for col in ['open', 'high', 'low', 'close']:
                df_merge[f'qfq_{col}'] = df_merge[col] * df_merge['adj_factor'] / latest_adj

            # 保留需要的列
            result = df_merge[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount', 
                               'qfq_open', 'qfq_high', 'qfq_low', 'qfq_close', 'adj_factor']]
            
            # 按日期升序
            result = result.sort_values('trade_date').reset_index(drop=True)
            
            # 截取最近 N 天
            return result.iloc[-days:]
            
        except Exception as e:
            print(f"获取个股数据异常 {ts_code}: {e}")
            return pd.DataFrame()

    def fuzzy_search_stock(self, keyword):
        """
        模糊搜索股票：支持代码(自动补后缀)和名称(中文)
        返回: (ts_code, name) 或 (None, None)
        """
        keyword = str(keyword).strip()
        if not keyword: return None, None

        try:
            # 1. 如果是纯数字，尝试补全后缀
            if keyword.isdigit() and len(keyword) == 6:
                ts_code = normalize_to_std(keyword)
                
                # 验证是否存在
                df = pro.stock_basic(ts_code=ts_code, fields='ts_code,name')
                if not df.empty:
                    return df.iloc[0]['ts_code'], df.iloc[0]['name']
            
            # 2. 如果不是代码，或者是代码但没搜到，或者是非6位数字，进行全名匹配
            # 这里的缓存机制很简单：首次加载全量列表（只有几千行，很快）
            if not hasattr(self, '_stock_basic_cache'):
                print(">> [Data] 加载全市场股票列表用于搜索...")
                self._stock_basic_cache = pro.stock_basic(fields='ts_code,symbol,name', list_status='L')
            
            df_all = self._stock_basic_cache
            
            # 优先精确匹配名称
            match = df_all[df_all['name'] == keyword]
            if not match.empty:
                return match.iloc[0]['ts_code'], match.iloc[0]['name']
            
            # 其次包含匹配名称
            match = df_all[df_all['name'].str.contains(keyword)]
            if not match.empty:
                # 返回市值最大的那个？或者第一个。这里简单返回第一个
                return match.iloc[0]['ts_code'], match.iloc[0]['name']
                
            # 再次尝试匹配 symbol (比如输入 600519 但没输后缀)
            match = df_all[df_all['symbol'] == keyword]
            if not match.empty:
                return match.iloc[0]['ts_code'], match.iloc[0]['name']

            return None, None
            
        except Exception as e:
            print(f"搜索股票异常: {e}")
            return None, None

    def get_stock_data_for_llm(self, ts_code, days=365):
        """
        获取投喂给 LLM 的全量数据
        包含：OHLCV, MA(5,10,20,30,60,120,200,250), VMA20
        """
        try:
            # 多取 300 天用于计算长期均线
            real_days = days + 300
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=real_days)).strftime('%Y%m%d')
            
            # 1. 获取日线 (不复权？威科夫通常看复权后的连续K线，这里统一用前复权)
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df.empty: return pd.DataFrame()
            
            # 2. 复权因子
            df_adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not df_adj.empty:
                df = df.merge(df_adj, on=['ts_code','trade_date'], how='left')
                df['adj_factor'] = df['adj_factor'].fillna(method='ffill').fillna(1.0)
                latest_adj = df['adj_factor'].iloc[0] # tushare返回是倒序的，第一行是最新
                
                # 计算前复权
                for col in ['open','high','low','close']:
                    df[col] = df[col] * df['adj_factor'] / latest_adj
            
            # 3. 排序 (旧 -> 新) 用于计算均线
            df = df.sort_values('trade_date').reset_index(drop=True)
            
            # 4. 计算均线指标
            ma_list = [5, 10, 20, 30, 50, 60, 120, 200, 250]
            for ma in ma_list:
                df[f'MA{ma}'] = df['close'].rolling(ma).mean()
            
            df['VMA20'] = df['vol'].rolling(20).mean()
            
            # 5. 截取用户请求的最近 N 天
            df_final = df.iloc[-days:].copy()
            
            # 6. 格式化日期
            df_final['date'] = pd.to_datetime(df_final['trade_date']).dt.strftime('%Y-%m-%d')
            
            # 7. 保留给 LLM 的列
            cols = ['date', 'open', 'high', 'low', 'close', 'vol', 'VMA20'] + [f'MA{ma}' for ma in ma_list]
            return df_final[cols]
            
        except Exception as e:
            print(f"LLM数据获取异常: {e}")
            return pd.DataFrame()

    # ==========================================
    # 模块 G: 成交量选股
    # ==========================================
    def get_volume_stocks(self, vol_threshold=25.0, vol_ratio_threshold=1.2, realtime_mode=False):
        """
        筛选成交量放大的个股
        
        Args:
            vol_threshold: 成交额绝对阈值(亿元)
            vol_ratio_threshold: 放量倍数
            realtime_mode: 是否使用实时数据（True=qstock实时行情，False=Tushare收盘数据）
        
        Returns:
            DataFrame: 包含符合条件的个股信息
        """
        if realtime_mode:
            return self._get_volume_stocks_realtime(vol_threshold, vol_ratio_threshold)
        else:
            return self._get_volume_stocks_daily(vol_threshold, vol_ratio_threshold)
    
    def _get_volume_stocks_daily(self, vol_threshold, vol_ratio_threshold):
        """
        使用Tushare日线数据筛选个股（收盘数据）
        行为：
        - 若当日收盘数据已生成，则使用当日；
        - 否则自动回退至最近一个有数据的交易日（通常是昨日收盘）。
        """
        try:
            # 1. 获取最近若干个交易日（多取几天，便于回退）
            trade_dates = self.get_trade_cal(days=8)
            if len(trade_dates) < 6:
                print("获取交易日失败")
                return pd.DataFrame()

            # 2. 从最近日期向前寻找“最新有日线数据的交易日”
            today_date = None
            df_today = pd.DataFrame()
            for d in reversed(trade_dates):
                df_tmp = pro.daily(trade_date=d, fields='ts_code,amount')
                if not df_tmp.empty:
                    today_date = d
                    df_today = df_tmp
                    break

            if today_date is None or df_today.empty:
                print("最近交易日均无日线数据")
                return pd.DataFrame()

            # 3. 选取 today_date 之前的 5 个交易日作为比较基准
            past_all = [d for d in trade_dates if d < today_date]
            if len(past_all) < 5:
                print("历史交易日不足5天，无法计算5日均值")
                return pd.DataFrame()
            past_5_dates = past_all[-5:]

            print(f"[收盘模式] 筛选日期: 参考日={today_date}, 过去5日={past_5_dates[0]}~{past_5_dates[-1]}")

            # 4. 获取过去5日成交数据
            past_data = []
            for date in past_5_dates:
                df_day = pro.daily(trade_date=date, fields='ts_code,amount')
                if not df_day.empty:
                    past_data.append(df_day)

            if not past_data:
                print("过去5日数据为空")
                return pd.DataFrame()

            # 5. 合并过去5日数据并计算均值
            df_past = pd.concat(past_data, ignore_index=True)
            df_past_avg = df_past.groupby('ts_code')['amount'].mean().reset_index()
            df_past_avg.columns = ['ts_code', 'avg_amount_5d']

            # 6. 合并参考日和历史数据
            df_merged = pd.merge(df_today, df_past_avg, on='ts_code', how='inner')

            # 7. 计算指标
            df_merged['amount'] = df_merged['amount'].fillna(0)
            df_merged['avg_amount_5d'] = df_merged['avg_amount_5d'].fillna(0)

            # 今日成交额(亿元)
            df_merged['今日成交额'] = df_merged['amount'] / 100000  # 千元转亿元

            # 放量倍数 = 今日成交额 / 5日均成交额
            df_merged['放量倍数'] = df_merged.apply(
                lambda x: x['amount'] / x['avg_amount_5d'] if x['avg_amount_5d'] > 0 else 0,
                axis=1
            )

            # 8. 筛选条件：今日成交额 >= 阈值 AND 放量倍数 >= 阈值
            df_filtered = df_merged[
                (df_merged['今日成交额'] >= vol_threshold) &
                (df_merged['放量倍数'] >= vol_ratio_threshold)
            ].copy()

            if df_filtered.empty:
                print("无符合条件的个股")
                return pd.DataFrame()

            # 9. 获取股票名称
            df_basic = pro.stock_basic(fields='ts_code,name')
            df_filtered = pd.merge(df_filtered, df_basic, on='ts_code', how='left')

            # 10. 整理输出格式
            df_filtered['5日均成交额'] = df_filtered['avg_amount_5d'] / 100000  # 千元转亿元

            result = df_filtered[[
                'ts_code', 'name', '今日成交额', '5日均成交额', '放量倍数'
            ]].copy()

            result.columns = ['代码', '名称', '今日成交额', '5日均成交额', '放量倍数']

            # 按今日成交额排序
            result = result.sort_values('今日成交额', ascending=False).reset_index(drop=True)

            print(f"[收盘模式] 筛选完成，共{len(result)}只个股")
            return result

        except Exception as e:
            print(f"成交量选股异常: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _get_volume_stocks_realtime(self, vol_threshold, vol_ratio_threshold):
        """
        使用实时数据筛选个股（日内实时监控）
        优先使用 AkShare 全市场快照，失败时回退到 qstock
        """
        try:
            df_realtime = pd.DataFrame()
            source = None
    
            # 1. 优先尝试 AkShare（东方财富全市场快照）
            try:
                import akshare as ak
                print("[实时模式] 尝试使用 AkShare 实时行情...")
                df_ak = ak.stock_zh_a_spot_em()
                if isinstance(df_ak, pd.DataFrame) and not df_ak.empty:
                    df_realtime = df_ak.copy()
                    source = "akshare"
                    print(f"[实时模式] AkShare 返回 {len(df_realtime)} 行")
                else:
                    print("[实时模式] AkShare 返回空数据")
            except Exception as e:
                print(f"[实时模式] AkShare 获取失败，原因: {e} (可能为网络/代理/反爬限制)")
    
            # 2. 如 AkShare 失败或无数据，退回 qstock
            if df_realtime.empty:
                try:
                    # 避免 qstock 在子线程中注册 signal 导致报错：signal only works in main thread
                    import signal as _signal
                    _signal.signal = lambda *args, **kwargs: None
                    import qstock as qs
                    print("[实时模式] 使用 qstock 实时行情...")
                    df_qs = qs.realtime_data(market='沪深A')
                    if isinstance(df_qs, pd.DataFrame) and not df_qs.empty:
                        df_realtime = df_qs.copy()
                        source = "qstock"
                        print(f"[实时模式] qstock 返回 {len(df_realtime)} 行")
                    else:
                        print("[实时模式] qstock 返回空数据")
                except Exception as e:
                    print(f"[实时模式] qstock 获取失败: {e}")
                    return pd.DataFrame()
    
            if df_realtime.empty:
                print("[实时模式] 实时行情数据为空，AkShare/qstock 均不可用")
                return pd.DataFrame()
    
            # 3. 获取过去5个交易日（用于计算5日均值）
            trade_dates = self.get_trade_cal(days=5)
            if len(trade_dates) < 5:
                print("获取交易日失败")
                return pd.DataFrame()
    
            past_5_dates = trade_dates[-5:]
            print(f"[实时模式] 过去5日: {past_5_dates[0]}~{past_5_dates[-1]}")
    
            # 4. 获取过去5日历史成交数据（从 Tushare）
            past_data = []
            for date in past_5_dates:
                df_day = pro.daily(trade_date=date, fields='ts_code,amount')
                if not df_day.empty:
                    past_data.append(df_day)
    
            if not past_data:
                print("过去5日数据为空")
                return pd.DataFrame()
    
            df_past = pd.concat(past_data, ignore_index=True)
            df_past_avg = df_past.groupby('ts_code')['amount'].mean().reset_index()
            df_past_avg.columns = ['ts_code', 'avg_amount_5d']
    
            # 5. 处理实时数据：不同来源统一为 ts_code + 千元 amount
            # 使用统一的标准化函数
            
            if source == "akshare":
                # AkShare: 代码列为 '代码' 或 'symbol'，成交额列为 '成交额'（单位：元）
                code_col = '代码' if '代码' in df_realtime.columns else ('symbol' if 'symbol' in df_realtime.columns else None)
                amt_col = '成交额' if '成交额' in df_realtime.columns else None
                name_col = '名称' if '名称' in df_realtime.columns else ('name' if 'name' in df_realtime.columns else None)
                if code_col is None or amt_col is None or name_col is None:
                    print("[实时模式] AkShare 实时数据字段不完整，缺少 代码/名称/成交额")
                    return pd.DataFrame()
                df_realtime['ts_code'] = df_realtime[code_col].astype(str).apply(normalize_to_std)
                df_realtime['amount'] = pd.to_numeric(df_realtime[amt_col], errors='coerce').fillna(0) / 1000.0  # 元 -> 千元
                df_realtime['名称'] = df_realtime[name_col].astype(str)
            else:
                # qstock: 代码 / 名称 / 成交额(元)
                df_realtime['ts_code'] = df_realtime['代码'].astype(str).apply(normalize_to_std)
                df_realtime['amount'] = pd.to_numeric(df_realtime['成交额'], errors='coerce').fillna(0) / 1000.0
    
            # 6. 合并实时数据和历史均值
            df_merged = pd.merge(
                df_realtime[['ts_code', '名称', 'amount']],
                df_past_avg,
                on='ts_code',
                how='inner'
            )
    
            # 7. 计算指标
            df_merged['amount'] = df_merged['amount'].fillna(0)
            df_merged['avg_amount_5d'] = df_merged['avg_amount_5d'].fillna(0)
    
            # 今日成交额(亿元)
            df_merged['今日成交额'] = df_merged['amount'] / 100000  # 千元转亿元
    
            # 放量倍数 = 今日成交额 / 5日均成交额
            df_merged['放量倍数'] = df_merged.apply(
                lambda x: x['amount'] / x['avg_amount_5d'] if x['avg_amount_5d'] > 0 else 0,
                axis=1
            )
    
            # 8. 筛选条件：今日成交额 >= 阈值 AND 放量倍数 >= 阈值
            df_filtered = df_merged[
                (df_merged['今日成交额'] >= vol_threshold) &
                (df_merged['放量倍数'] >= vol_ratio_threshold)
            ].copy()
    
            if df_filtered.empty:
                print("[实时模式] 无符合条件的个股")
                return pd.DataFrame()
    
            # 9. 整理输出格式
            df_filtered['5日均成交额'] = df_filtered['avg_amount_5d'] / 100000  # 千元转亿元
    
            result = df_filtered[[
                'ts_code', '名称', '今日成交额', '5日均成交额', '放量倍数'
            ]].copy()
    
            result.columns = ['代码', '名称', '今日成交额', '5日均成交额', '放量倍数']
    
            # 按今日成交额排序
            result = result.sort_values('今日成交额', ascending=False).reset_index(drop=True)
    
            print(f"[实时模式] 筛选完成，共{len(result)}只个股，数据源: {source}")
            return result
    
        except Exception as e:
            print(f"实时选股异常: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

engine = DataEngine()