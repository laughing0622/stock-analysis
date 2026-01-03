import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class WyckoffAnalyzer:
    def __init__(self, df):
        """
        df: DataFrame with columns ['trade_date', 'qfq_open', 'qfq_high', 'qfq_low', 'qfq_close', 'vol']
        """
        self.df = df.copy()
        if 'qfq_close' in self.df.columns:
            self.df['close'] = self.df['qfq_close']
            self.df['open'] = self.df['qfq_open']
            self.df['high'] = self.df['qfq_high']
            self.df['low'] = self.df['qfq_low']
        
        # Ensure dates are datetime
        self.df['date'] = pd.to_datetime(self.df['trade_date'])
        self.df = self.df.sort_values('date').reset_index(drop=True)

    def _identify_pivots(self, window=5):
        """Identify pivot high and lows for support/resistance"""
        df = self.df
        df['is_pivot_high'] = df['high'].rolling(window*2+1, center=True).max() == df['high']
        df['is_pivot_low'] = df['low'].rolling(window*2+1, center=True).min() == df['low']
        return df

    def _analyze_volume_price(self):
        """Analyze Volume Spread Analysis (VSA) concepts"""
        df = self.df
        # Spread (Range)
        df['spread'] = df['high'] - df['low']
        df['spread_avg'] = df['spread'].rolling(20).mean()
        
        # Volume Moving Average
        df['vol_ma20'] = df['vol'].rolling(20).mean()
        
        # Anomalies
        # 1. High Volume, Wide Spread (Strength/Weakness depending on close)
        df['is_wide_spread'] = df['spread'] > 1.5 * df['spread_avg']
        df['is_high_vol'] = df['vol'] > 1.8 * df['vol_ma20']
        
        # 2. High Volume, Narrow Spread (Churning/Stopping Volume)
        df['is_narrow_spread'] = df['spread'] < 0.7 * df['spread_avg']
        df['is_churning'] = df['is_high_vol'] & df['is_narrow_spread']
        
        # 3. Climax (Very High Vol + Wide Spread)
        df['is_climax'] = (df['vol'] > 2.5 * df['vol_ma20']) & df['is_wide_spread']
        
        return df

    def plot_analysis(self, stock_name=""):
        df = self._identify_pivots()
        df = self._analyze_volume_price()
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.03, row_heights=[0.7, 0.3],
                            specs=[[{"secondary_y": True}], [{"secondary_y": False}]])

        # 1. Candlestick
        fig.add_trace(go.Candlestick(
            x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'],
            name='Price', increasing_line_color='#ef5350', decreasing_line_color='#26a69a'
        ), row=1, col=1)

        # 2. Support/Resistance Lines (from Pivots)
        # Simple algorithm: Take last 3 major pivots
        pivots_high = df[df['is_pivot_high']]
        pivots_low = df[df['is_pivot_low']]
        
        # Draw recent significant levels
        if not pivots_high.empty:
            last_high = pivots_high.iloc[-1]['high']
            fig.add_hline(y=last_high, line_dash="dot", line_color="red", opacity=0.5, row=1, col=1, annotation_text="Res", annotation_position="top right")
        if not pivots_low.empty:
            last_low = pivots_low.iloc[-1]['low']
            fig.add_hline(y=last_low, line_dash="dot", line_color="green", opacity=0.5, row=1, col=1, annotation_text="Sup", annotation_position="bottom right")

        # 3. Annotate VSA Signals
        # Churning (Potential Turning Point)
        churn_pts = df[df['is_churning']]
        if not churn_pts.empty:
            fig.add_trace(go.Scatter(
                x=churn_pts['date'], y=churn_pts['high'], mode='markers',
                marker=dict(symbol='diamond', size=8, color='orange'),
                name='Churning (Effort!=Result)'
            ), row=1, col=1)

        # Climax
        climax_pts = df[df['is_climax']]
        if not climax_pts.empty:
            fig.add_trace(go.Scatter(
                x=climax_pts['date'], y=climax_pts['high']*1.01, mode='markers',
                marker=dict(symbol='star', size=10, color='purple'),
                name='Climax Action'
            ), row=1, col=1)

        # 4. Volume Bar
        colors = ['#ef5350' if c >= o else '#26a69a' for c, o in zip(df['close'], df['open'])]
        fig.add_trace(go.Bar(
            x=df['date'], y=df['vol'], name='Volume', marker_color=colors, opacity=0.8
        ), row=2, col=1)
        
        # Volume MA
        fig.add_trace(go.Scatter(
            x=df['date'], y=df['vol_ma20'], name='Vol MA20', line=dict(color='orange', width=1)
        ), row=2, col=1)

        # Layout
        fig.update_layout(
            title=f"Wyckoff / VSA Analysis - {stock_name}",
            xaxis_rangeslider_visible=False,
            height=800,
            xaxis2_title="Date",
            yaxis1_title="Price",
            yaxis2_title="Volume",
            hovermode="x unified",
            showlegend=True,
            legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center")
        )
        
        return fig
