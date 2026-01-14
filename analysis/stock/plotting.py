import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def plot_wyckoff_chart(df, ai_result, stock_name):
    """
    根据 DataFrame 和 AI 返回的 JSON 结果绘制威科夫分析图
    """
    # 确保日期格式一致
    df['date'] = pd.to_datetime(df['date'])
    
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.03, 
        row_heights=[0.7, 0.3],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )

    # 1. K线图
    fig.add_trace(go.Candlestick(
        x=df['date'],
        open=df['open'], high=df['high'], low=df['low'], close=df['close'],
        name='K线',
        increasing_line_color='#ef5350', decreasing_line_color='#26a69a'
    ), row=1, col=1)

    # 2. 均线
    if 'MA50' in df.columns:
        fig.add_trace(go.Scatter(x=df['date'], y=df['MA50'], name='MA50', line=dict(color='blue', width=1, dash='dot')), row=1, col=1)
    if 'MA200' in df.columns:
        fig.add_trace(go.Scatter(x=df['date'], y=df['MA200'], name='MA200', line=dict(color='orange', width=1, dash='dash')), row=1, col=1)

    # 3. 成交量
    colors = ['#ef5350' if c >= o else '#26a69a' for c, o in zip(df['close'], df['open'])]
    fig.add_trace(go.Bar(
        x=df['date'], y=df['vol'], name='成交量', marker_color=colors
    ), row=2, col=1)
    
    if 'VMA20' in df.columns:
        fig.add_trace(go.Scatter(x=df['date'], y=df['VMA20'], name='VMA20', line=dict(color='black', width=1)), row=2, col=1)

    # ==========================
    # 绘制 AI 识别的内容
    # ==========================
    if isinstance(ai_result, dict) and 'phases' in ai_result:
        for phase in ai_result['phases']:
            try:
                # 绘制矩形框
                fill_color = "rgba(0, 255, 0, 0.1)" # 默认绿
                if phase.get('type') == 'distribution':
                    fill_color = "rgba(255, 0, 0, 0.1)"
                elif phase.get('type') == 'accumulation':
                    fill_color = "rgba(0, 255, 0, 0.1)"
                
                fig.add_shape(
                    type="rect",
                    x0=phase['start_date'], x1=phase['end_date'],
                    y0=phase['bottom_price'], y1=phase['top_price'],
                    fillcolor=fill_color,
                    line=dict(width=0),
                    row=1, col=1
                )
                
                # 添加阶段名称标注
                mid_x = pd.to_datetime(phase['start_date']) + (pd.to_datetime(phase['end_date']) - pd.to_datetime(phase['start_date'])) / 2
                fig.add_annotation(
                    x=mid_x, y=phase['top_price'],
                    text=phase.get('name', 'Phase'),
                    showarrow=False,
                    yshift=10,
                    font=dict(color="black", size=10),
                    row=1, col=1
                )
            except Exception as e:
                print(f"绘制 Phase 出错: {e}")

    if isinstance(ai_result, dict) and 'events' in ai_result:
        for event in ai_result['events']:
            try:
                # 查找当天最高价/最低价用于定位
                row = df[df['date'] == pd.to_datetime(event['date'])]
                if row.empty: continue
                
                price_high = row.iloc[0]['high']
                price_low = row.iloc[0]['low']
                
                # 决定标注在上方还是下方
                # 通常 SC/Spring 标在下方，UTAD/SOS 标在上方，这里简单交替或根据类型
                # 简单起见：默认上方，如果是 Spring/SC 则下方
                evt_type = event.get('type', '').upper()
                is_bottom = evt_type in ['SC', 'SPRING', 'ST', 'LPS']
                
                y_pos = price_low if is_bottom else price_high
                y_anchor = "top" if is_bottom else "bottom"
                y_shift = 15 if not is_bottom else -15
                arrow_dir = 1 if is_bottom else -1 # 1指向上，-1指向下
                
                fig.add_annotation(
                    x=event['date'], y=y_pos,
                    text=f"<b>{evt_type}</b>",
                    hovertext=event.get('description', ''),
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=2,
                    arrowcolor="#333",
                    ax=0,
                    ay=-30 if not is_bottom else 30,
                    font=dict(color="#333", size=9),
                    row=1, col=1
                )
            except Exception as e:
                print(f"绘制 Event 出错: {e}")

    # Layout 设置
    fig.update_layout(
        title=f"威科夫分析图谱 - {stock_name}",
        height=800,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        margin=dict(l=10, r=10, t=50, b=10),
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center")
    )
    
    return fig
