"""
雪球策略数据适配器
直接使用sqlite3查询数据库，避免导入xueqiu模块产生的冲突
"""
import sqlite3
import pandas as pd
import json
import yaml
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
# 引入标准格式化工具
try:
    from utils.symbol_standard import normalize_to_std
except ImportError:
    # 兼容路径问题，如果直接运行此文件
    import sys
    sys.path.append(str(Path(__file__).parent))
    from utils.symbol_standard import normalize_to_std



class XueqiuDataAdapter:
    """雪球策略数据适配器 - 直接查询数据库版本"""

    def __init__(self):
        # 使用配置中的路径
        self._db_path = r"D:\stockproject\xueqiu_qmt_trader\data\xueqiu_qmt.db"
        self._stock_names_cache = self._load_stock_names_cache()
        self._portfolio_names_map = self._load_portfolio_names_map()

    def _load_stock_names_cache(self) -> Dict[str, str]:
        """加载股票名称缓存"""
        cache_path = r"D:\stockproject\xueqiu_qmt_trader\data\stock_names_cache.json"
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('name_map', {})
        except Exception as e:
            print(f"加载股票名称缓存失败: {e}")
            return {}

    def _load_portfolio_names_map(self) -> Dict[str, str]:
        """从雪球项目config.yaml加载组合代码到名称的映射"""
        config_path = r"D:\stockproject\xueqiu_qmt_trader\config\config.yaml"
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config.get('xueqiu', {}).get('portfolio_names', {})
        except Exception as e:
            print(f"加载组合名称映射失败: {e}")
            return {}

    def get_stock_name(self, symbol: str) -> str:
        """
        获取股票名称

        Args:
            symbol: 股票代码（支持多种格式，自动提取6位代码）

        Returns:
            股票名称，失败时返回代码本身
        """
        # 提取6位纯数字代码
        clean_symbol = symbol
        if '.' in symbol:
            clean_symbol = symbol.split('.')[0]
        # 去除可能的字母前缀
        clean_symbol = ''.join(c for c in clean_symbol if c.isdigit())[:6]

        return self._stock_names_cache.get(clean_symbol, symbol)

    def get_portfolio_name(self, portfolio_code: str) -> str:
        """
        获取组合名称

        Args:
            portfolio_code: 组合代码（如 ZH3204652）

        Returns:
            组合名称，失败时返回代码本身
        """
        return self._portfolio_names_map.get(portfolio_code, portfolio_code)

    def get_strategy_display_name(self, strategy_name: str) -> str:
        """
        获取策略显示名称（将组合代码替换为组合名）

        Args:
            strategy_name: 策略名称（如 Xueqiu_Strategy_ZH3204652）

        Returns:
            格式化后的显示名称（如 "雪球组合-资产轮动"）
        """
        # 处理雪球组合策略名称
        if strategy_name.startswith('Xueqiu_Strategy_'):
            portfolio_code = strategy_name.replace('Xueqiu_Strategy_', '')
            portfolio_name = self.get_portfolio_name(portfolio_code)
            return f"雪球组合-{portfolio_name}"

        # 其他策略直接返回
        return strategy_name

    def check_connection(self) -> bool:
        """检查数据库连接"""
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute("SELECT 1")
            conn.close()
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """执行查询并返回字典列表"""
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"查询失败: {e}")
            return []

    def get_all_active_strategies(self) -> List[str]:
        """获取所有活跃策略名称"""
        query = """
            SELECT DISTINCT strategy_name
            FROM strategy_nav
            WHERE status = 'active'
            ORDER BY strategy_name
        """
        results = self._execute_query(query)
        return [r['strategy_name'] for r in results]

    def get_portfolio_nav_curve(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取总仓位加权净值曲线（时间加权法，不受资金流入流出影响）

        计算逻辑：
        1. 总资产 = Σ(策略持仓市值) + QMT总现金
        2. 总净值 = Σ(nav * capital_base) / Σ(capital_base) [时间加权法]

        说明：使用持仓市值而不是总资产，避免策略现金与QMT现金重复计算
        """
        # 使用时间加权法：按资金基数加权平均各策略NAV
        # 这样新增/撤出资金不会影响净值曲线，真实反映投资收益
        query = """
            SELECT dp.date,
                   SUM(dp.nav * dp.capital_base) / NULLIF(SUM(dp.capital_base), 0) as portfolio_nav,
                   SUM(dp.capital_base) as total_capital_base,
                   COALESCE(SUM(dp.mkt_value), 0) + COALESCE(qc.total_qmt_cash, 0) as total_asset
            FROM daily_performance dp
            LEFT JOIN (
                SELECT date, SUM(total_cash) as total_qmt_cash
                FROM qmt_account_cash
                GROUP BY date
            ) qc ON dp.date = qc.date
            WHERE dp.nav > 0 AND dp.capital_base > 0
            GROUP BY dp.date
            ORDER BY dp.date
        """
        results = self._execute_query(query)
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        if df.empty:
            return df

        # portfolio_nav 已经是加权后的净值，无需再计算
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        return df[['date', 'portfolio_nav', 'total_asset', 'total_capital_base']]

    def get_strategy_nav_curve(self, strategy_name: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取单策略净值曲线"""
        query = """
            SELECT date, nav, total_asset, capital_base,
                   daily_return, cumulative_return, status
            FROM strategy_nav
            WHERE strategy_name = ?
            ORDER BY date
        """
        results = self._execute_query(query, (strategy_name,))
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        return df

    def get_contribution_analysis(self, date: str = None) -> List[Dict]:
        """获取策略贡献度分析"""
        if not date:
            # 获取最新日期
            query = "SELECT MAX(date) as max_date FROM strategy_nav"
            result = self._execute_query(query)
            if result:
                date = result[0]['max_date']
            else:
                return []

        query = """
            SELECT strategy_name, nav, total_asset, capital_base
            FROM strategy_nav
            WHERE date = ? AND status = 'active'
        """
        results = self._execute_query(query, (date,))
        if not results:
            return []

        total_asset = sum(r['total_asset'] for r in results)

        return [
            {
                'strategy_name': self.get_strategy_display_name(r['strategy_name']),
                'nav': r['nav'],
                'total_asset': r['total_asset'],
                'weight': r['total_asset'] / total_asset if total_asset > 0 else 0,
                'contribution': r['nav'] * r['total_asset'] / total_asset if total_asset > 0 else 0
            }
            for r in results
        ]

    def get_performance_metrics(self, strategy_name: str, start_date: str = None, end_date: str = None) -> Dict:
        """获取策略性能指标"""
        query = """
            SELECT nav, daily_return, cumulative_return, date
            FROM strategy_nav
            WHERE strategy_name = ?
            ORDER BY date DESC
            LIMIT 100
        """
        results = self._execute_query(query, (strategy_name,))
        if not results:
            return {}

        df = pd.DataFrame(results)
        if len(df) < 2:
            return {}

        # 计算性能指标
        cumulative_return = df['cumulative_return'].iloc[0] if 'cumulative_return' in df.columns else 0
        current_nav = df['nav'].iloc[0]

        # 最大回撤
        peak = df['nav'].expanding(min_periods=1).max()
        drawdown = (df['nav'] - peak) / peak
        max_drawdown = drawdown.min()

        # 胜率
        win_count = len(df[df['daily_return'] > 0])
        total_count = len(df[df['daily_return'] != 0])
        win_rate = win_count / total_count if total_count > 0 else 0

        return {
            'cumulative_return': cumulative_return,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'current_nav': current_nav,
            'start_date': df['date'].iloc[-1],
            'end_date': df['date'].iloc[0]
        }

    def get_virtual_positions(self, strategy_name: str = None, portfolio_code: str = None) -> List[Dict]:
        """
        获取虚拟持仓（包含名称和现价）

        注意：现价暂时使用成本价代替，后续可接入QMT实时行情
        """
        if strategy_name and not portfolio_code:
            query = """
                SELECT symbol, quantity, cost_price, updated_at
                FROM virtual_positions
                WHERE strategy_name = ?
                ORDER BY updated_at DESC
            """
            results = self._execute_query(query, (strategy_name,))
        elif portfolio_code:
            query = """
                SELECT symbol, quantity, cost_price, updated_at
                FROM virtual_positions
                WHERE portfolio_code = ?
                ORDER BY symbol
            """
            results = self._execute_query(query, (portfolio_code,))
        else:
            results = []

        positions = []
        for r in results:
            # 【修复】从数据库读取时强制标准化，修复可能的脏数据(如 .BJ)
            symbol = normalize_to_std(r['symbol'])
            name = self.get_stock_name(symbol)
            cost_price = float(r['cost_price'])
            quantity = int(r['quantity'])
            # 现价暂时使用成本价，盈亏显示为0
            current_price = cost_price

            positions.append({
                'symbol': symbol,
                'name': name,
                'quantity': quantity,
                'cost_price': cost_price,
                'current_price': current_price,
                'updated_at': r.get('updated_at')
            })

        return positions

    def get_portfolio_positions(self, portfolio_code: str) -> List[Dict]:
        """获取雪球组合持仓"""
        return self.get_virtual_positions(portfolio_code=portfolio_code)

    def get_daily_performance(self, strategy_name: str, days: int = 30) -> pd.DataFrame:
        """获取策略每日业绩"""
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        query = """
            SELECT date, strategy_name, total_asset, daily_pnl,
                   total_pnl, yield, nav
            FROM daily_performance
            WHERE strategy_name = ? AND date >= ?
            ORDER BY date DESC
        """
        results = self._execute_query(query, (strategy_name, start_date))
        return pd.DataFrame(results) if results else pd.DataFrame()

    def get_rebalance_records(self, portfolio_code: str = None, limit: int = 50) -> List[Dict]:
        """获取调仓记录"""
        if portfolio_code:
            query = """
                SELECT portfolio_code, symbol, name, rebalance_type,
                       prev_weight, target_weight, price, detected_time
                FROM rebalance_records
                WHERE portfolio_code = ?
                ORDER BY detected_time DESC
                LIMIT ?
            """
            results = self._execute_query(query, (portfolio_code, limit))
        else:
            query = """
                SELECT portfolio_code, symbol, name, rebalance_type,
                       prev_weight, target_weight, price, detected_time
                FROM rebalance_records
                ORDER BY detected_time DESC
                LIMIT ?
            """
            results = self._execute_query(query, (limit,))

        return results

    def get_latest_snapshot(self, portfolio_code: str) -> Optional[Dict]:
        """获取组合最新快照"""
        query = """
            SELECT portfolio_code, portfolio_name, owner_name,
                   net_value, total_gain, snapshot_time
            FROM portfolio_snapshots
            WHERE portfolio_code = ?
            ORDER BY snapshot_time DESC
            LIMIT 1
        """
        results = self._execute_query(query, (portfolio_code,))
        return results[0] if results else None

    def get_portfolio_allocation(self, portfolio_code: str) -> Optional[Dict]:
        """获取组合资金分配"""
        strategy_name = f"Xueqiu_Strategy_{portfolio_code}"
        query = """
            SELECT initial_capital, current_cash
            FROM strategy_allocations
            WHERE strategy_name = ?
        """
        results = self._execute_query(query, (strategy_name,))
        return results[0] if results else None

    def get_last_nav(self, strategy_name: str) -> Optional[Dict]:
        """获取最新净值"""
        query = """
            SELECT date, nav, total_asset, capital_base,
                   daily_return, cumulative_return, status
            FROM strategy_nav
            WHERE strategy_name = ?
            ORDER BY date DESC
            LIMIT 1
        """
        results = self._execute_query(query, (strategy_name,))
        return results[0] if results else None

    def get_strategy_funds(self, strategy_name: str) -> Optional[Dict]:
        """
        获取策略资金信息（现金、总资产等）

        Returns:
            {
                'total_asset': 总资产,
                'current_cash': 现金,
                'market_value': 持仓市值,
                'nav': 净值,
                'daily_return': 日收益率,
                'cumulative_return': 累计收益率
            }
        """
        nav_data = self.get_last_nav(strategy_name)
        if not nav_data:
            return None

        total_asset = nav_data.get('total_asset', 0)

        # 从strategy_allocations获取现金信息
        query = """
            SELECT current_cash
            FROM strategy_allocations
            WHERE strategy_name = ?
        """
        alloc_results = self._execute_query(query, (strategy_name,))
        current_cash = alloc_results[0].get('current_cash', 0) if alloc_results else 0

        # 计算持仓市值
        market_value = total_asset - current_cash

        return {
            'total_asset': total_asset,
            'current_cash': current_cash,
            'market_value': market_value,
            'nav': nav_data.get('nav', 1.0),
            'daily_return': nav_data.get('daily_return', 0),
            'cumulative_return': nav_data.get('cumulative_return', 0)
        }

    def get_candidate_pool(self, strategy_name: str, limit: int = 20) -> List[Dict]:
        """
        获取策略候选池数据

        Args:
            strategy_name: 策略名称
            limit: 返回数量限制

        Returns:
            候选池数据列表，按排名排序
        """
        query = """
            SELECT symbol, name, rank, score, data, updated_at
            FROM candidate_pools
            WHERE strategy_name = ?
            ORDER BY rank ASC
            LIMIT ?
        """
        results = self._execute_query(query, (strategy_name, limit))

        if not results:
            return []

        candidates = []
        for r in results:
            # 解析JSON数据
            try:
                data = json.loads(r['data']) if isinstance(r['data'], str) else r['data']
            except:
                data = {}

            candidates.append({
                'symbol': r['symbol'],
                'name': r['name'],
                'rank': r['rank'],
                'score': r['score'],
                'data': data,
                'updated_at': r.get('updated_at')
            })

        return candidates


# 单例实例
xueqiu_adapter = XueqiuDataAdapter()
