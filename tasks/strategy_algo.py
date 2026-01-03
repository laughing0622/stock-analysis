import pandas as pd
import numpy as np
import math
from sklearn.linear_model import LinearRegression

class StrategyAlgo:
    """
    策略算法库 (v21.0 WLS版)
    """

    @staticmethod
    def check_risk_control(prices):
        """
        通用硬风控 (策略A/B共用):
        1. 均线保护: 现价 > MA20
        2. 防暴跌: 单日跌幅 > -3%
        3. 防高位回撤: 较25日高点回撤 > -10%
        返回: (bool_pass, str_reason)
        """
        if len(prices) < 25: return False, "数据不足"
        
        current = prices[-1]
        prev = prices[-2]
        
        # 1. 均线检查 (MA20)
        ma20 = np.mean(prices[-20:])
        if current < ma20: return False, "跌破均线"
        
        # 2. 暴跌检查
        pct = (current / prev) - 1
        if pct < -0.03: return False, "单日暴跌"
        
        # 3. 高位回撤
        high_25 = np.max(prices[-25:])
        dd = (current / high_25) - 1
        if dd < -0.10: return False, "高位回撤"
        
        return True, ""

    @staticmethod
    def calc_strategy_a_wls(df_daily, m_days=25):
        """
        策略A (WLS趋势):
        得分 = 年化收益 * R2 (加权)
        """
        prices = df_daily['close_qfq'].values
        if len(prices) < m_days: return -999, 0, 0
        
        # 取最近 m_days
        y = np.log(prices[-m_days:])
        x = np.arange(len(y))
        # 权重 1.0 -> 2.0
        weights = np.linspace(1, 2, len(y))
        
        # WLS 拟合
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        
        # 计算加权 R2
        y_pred = slope * x + intercept
        y_mean_weighted = np.average(y, weights=weights)
        ss_res = np.sum(weights * (y - y_pred)**2)
        ss_tot = np.sum(weights * (y - y_mean_weighted)**2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        
        # 年化收益
        annual_ret = math.exp(slope * 250) - 1
        
        # 计算得分
        if annual_ret < 0:
            score = annual_ret
        else:
            score = annual_ret * r2
            
        return score, annual_ret, r2

    @staticmethod
    def calc_strategy_b_factors(df_daily, bias_n=20, momentum_day=25, slope_n=25):
        """
        策略B (三因子) - 完全按照回测代码逻辑
        返回 (bias_score, slope_score, eff_score) 原始值
        参数:
            df_daily: DataFrame, 必须包含 open/high/low/close_qfq
            bias_n: 乖离率MA窗口 (默认20)
            momentum_day: 动量计算窗口 (默认25)
            slope_n: 斜率计算窗口 (默认25)
        """
        # 数据清洗: 去除NaN
        df = df_daily.dropna()
        if len(df) < momentum_day + bias_n:
            return None
        
        close = df['close_qfq']
        
        # --- 1. 乖离动量因子 ---
        try:
            # 计算乖离度: close / MA(close, BIAS_N)
            ma = close.rolling(window=bias_n, min_periods=1).mean()
            bias = close / ma
            
            # 取最近 M 天的乖离度
            bias_recent = bias.iloc[-momentum_day:]
            
            # 检查切片中是否包含 NaN/Inf
            if bias_recent.isnull().values.any() or np.isinf(bias_recent.values).any():
                return None
            
            # 归一化处理 (增加分母防0判断)
            start_bias = bias_recent.iloc[0]
            if start_bias == 0 or np.isnan(start_bias):
                return None
            
            x = np.arange(momentum_day).reshape(-1, 1)
            y = (bias_recent / start_bias).values
            
            # 二次检查 y 的有效性
            if np.isnan(y).any() or np.isinf(y).any():
                return None
            
            lr = LinearRegression()
            lr.fit(x, y)
            bias_s = lr.coef_[0] * 10000
        except Exception:
            return None
        
        # --- 2. 斜率动量因子 ---
        try:
            prices_slope = close.iloc[-slope_n:]
            start_price = prices_slope.iloc[0]
            if start_price == 0:
                return None
            
            norm_prices = prices_slope / start_price
            
            x_slope = np.arange(1, slope_n + 1).reshape(-1, 1)
            y_slope = norm_prices.values
            
            if np.isnan(y_slope).any() or np.isinf(y_slope).any():
                return None
            
            lr_slope = LinearRegression()
            lr_slope.fit(x_slope, y_slope)
            slope = lr_slope.coef_[0]
            r2 = lr_slope.score(x_slope, y_slope)
            slope_s = 10000 * slope * r2
        except Exception:
            return None
        
        # --- 3. 效率动量因子 ---
        try:
            # 需要有OHLC字段
            if 'open' not in df.columns or 'high' not in df.columns or 'low' not in df.columns:
                # 如果没有OHLC, 用close代替pivot
                df_recent = df.iloc[-momentum_day:]
                pivot = close.iloc[-momentum_day:]
            else:
                df_recent = df.iloc[-momentum_day:]
                # 使用复权后的OHLC计算pivot (假设都有对应的qfq列或用close_qfq代替)
                # 简化处理: 如果没有其他qfq列,用close_qfq作为基准
                if 'open_qfq' in df.columns:
                    pivot = (df_recent['open_qfq'] + df_recent['high_qfq'] + 
                            df_recent['low_qfq'] + df_recent['close_qfq']) / 4.0
                else:
                    # 使用未复权OHLC计算pivot (因为数据库可能只有close复权)
                    pivot = (df_recent['open'] + df_recent['high'] + 
                            df_recent['low'] + df_recent['close']) / 4.0
            
            # 防止pivot为0或负数导致log报错
            if (pivot <= 0).any():
                return None
            
            # 动量: 对数收益率
            momentum_val = 100 * np.log(pivot.iloc[-1] / pivot.iloc[0])
            
            # 效率系数
            direction = abs(np.log(pivot.iloc[-1]) - np.log(pivot.iloc[0]))
            volatility = np.log(pivot).diff().abs().sum()
            
            efficiency_ratio = direction / volatility if volatility > 0 else 0
            eff_s = momentum_val * efficiency_ratio
            
        except Exception:
            return None
        
        return (bias_s, slope_s, eff_s)