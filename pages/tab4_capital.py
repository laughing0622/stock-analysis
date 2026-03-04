"""
Tab 4: 资金曲线
展示全周期净值曲线、策略贡献度、单策略对比
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from xueqiu_adapter import xueqiu_adapter


def render_capital_tab():
    """渲染资金曲线Tab"""
    st.markdown("#### 💰 资金曲线分析")

    # 检查连接
    if not xueqiu_adapter.check_connection():
        st.error("⚠️ 无法连接到雪球策略数据库，请检查配置")
        st.info("请确保 config.py 中 XUEQIU_DB_PATH 路径正确")
        st.info(f"配置路径: {xueqiu_adapter._direct_db_path or '未设置'}")
        return

    # 刷新按钮
    col_refresh, col_info = st.columns([1, 3])
    with col_refresh:
        if st.button("🔄 刷新数据", key="btn_capital_refresh"):
            st.rerun()

    # 日期范围选择
    with st.expander("📅 日期范围设置", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            default_start = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            start_date = st.text_input("开始日期", value=default_start, help="格式: YYYY-MM-DD")
        with col2:
            default_end = datetime.now().strftime('%Y-%m-%d')
            end_date = st.text_input("结束日期", value=default_end, help="格式: YYYY-MM-DD")

    # 子Tab组织
    tab_portfolio, tab_contribution, tab_comparison = st.tabs(["总仓位净值", "策略贡献度", "单策略对比"])

    with tab_portfolio:
        render_portfolio_nav(start_date, end_date)

    with tab_contribution:
        render_contribution_analysis()

    with tab_comparison:
        render_strategy_comparison(start_date, end_date)


def render_portfolio_nav(start_date: str, end_date: str):
    """渲染总仓位加权净值曲线"""
    st.markdown("##### 📊 总仓位加权净值曲线")

    df = xueqiu_adapter.get_portfolio_nav_curve(start_date, end_date)

    if df.empty:
        st.warning("暂无净值数据，可能原因：")
        st.info("- 数据库中无strategy_nav记录\n- 日期范围内无数据\n- 策略尚未运行")
        return

    # 指标卡片
    latest = df.iloc[-1]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("最新净值", f"{latest['portfolio_nav']:.4f}")
    c2.metric("总资产", f"{latest['total_asset']:,.0f}")

    # 计算收益率
    if len(df) > 1:
        initial_nav = df.iloc[0]['portfolio_nav']
        total_return = (latest['portfolio_nav'] - initial_nav) / initial_nav
        c3.metric("累计收益率", f"{total_return:+.2%}")

        # 最大回撤
        peak = df['portfolio_nav'].expanding(min_periods=1).max()
        drawdown = (df['portfolio_nav'] - peak) / peak
        max_dd = drawdown.min()
        c4.metric("最大回撤", f"{max_dd:.2%}")

    # 净值曲线图
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['portfolio_nav'],
        mode='lines',
        name='净值',
        line=dict(color='#d62728', width=2)
    ))

    # 添加基准线
    fig.add_hline(y=1.0, line_dash="dot", line_color="gray", annotation_text="基准")

    fig.update_layout(
        title="总仓位加权净值曲线",
        xaxis_title="日期",
        yaxis_title="净值",
        hovermode="x unified",
        height=400
    )

    fig.update_xaxes(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1月", step="month", stepmode="backward"),
                dict(count=3, label="3月", step="month", stepmode="backward"),
                dict(count=6, label="6月", step="month", stepmode="backward"),
                dict(step="all", label="全部")
            ])
        )
    )

    st.plotly_chart(fig, width="stretch")

    # 数据表
    with st.expander("查看详细数据"):
        display_df = df.copy()
        display_df['日期'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
        display_df['净值'] = display_df['portfolio_nav'].apply(lambda x: f"{x:.4f}")
        display_df['总资产'] = display_df['total_asset'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(
            display_df[['日期', '净值', '总资产']],
            width="stretch",
            hide_index=True
        )


def render_contribution_analysis():
    """渲染策略贡献度分析"""
    st.markdown("##### 🎯 策略贡献度分析")

    contributions = xueqiu_adapter.get_contribution_analysis()

    if not contributions:
        st.warning("暂无贡献度数据，可能原因：")
        st.info("- 无活跃策略\n- 策略无资产分配\n- 策略未产生净值记录")
        return

    # 转换为DataFrame
    df = pd.DataFrame(contributions)

    # 饼图 - 资产占比
    fig_pie = go.Figure(data=[go.Pie(
        labels=df['strategy_name'],
        values=df['total_asset'],
        hole=0.3,
        textinfo='label+percent'
    )])
    fig_pie.update_layout(title="资产占比分布", height=400)
    st.plotly_chart(fig_pie, width="stretch")

    # 柱状图 - 净值贡献
    fig_bar = go.Figure(data=[go.Bar(
        x=df['strategy_name'],
        y=df['contribution'],
        text=df['contribution'].apply(lambda x: f"{x:.4f}"),
        textposition='auto'
    )])
    fig_bar.update_layout(
        title="净值贡献度",
        xaxis_title="策略",
        yaxis_title="贡献度",
        height=400
    )
    st.plotly_chart(fig_bar, width="stretch")

    # 数据表
    display_df = df.copy()
    display_df['资产占比'] = display_df['weight'].apply(lambda x: f"{x:.2%}")
    display_df['净值贡献'] = display_df['contribution'].apply(lambda x: f"{x:.4f}")
    st.dataframe(
        display_df[['strategy_name', 'nav', 'total_asset', '资产占比', '净值贡献']],
        column_config={
            'strategy_name': st.column_config.TextColumn('策略名称'),
            'nav': st.column_config.NumberColumn('净值', format="%.4f"),
            'total_asset': st.column_config.NumberColumn('总资产', format="%.0f"),
            '资产占比': st.column_config.TextColumn('资产占比'),
            '净值贡献': st.column_config.TextColumn('净值贡献')
        },
        width="stretch",
        hide_index=True
    )


def render_strategy_comparison(start_date: str, end_date: str):
    """渲染单策略净值对比"""
    st.markdown("##### 📈 单策略净值对比")

    strategies = xueqiu_adapter.get_all_active_strategies()

    if not strategies:
        st.warning("暂无活跃策略，可能原因：")
        st.info("- 数据库中无status='active'的策略记录\n- 所有策略已暂停或停止")
        return

    # 使用友好名称作为显示选项，但原始名称作为值
    strategy_display_map = {s: xueqiu_adapter.get_strategy_display_name(s) for s in strategies}
    display_strategies = [strategy_display_map[s] for s in strategies]

    # 策略选择（使用友好名称）
    selected_display = st.multiselect(
        "选择要对比的策略",
        options=display_strategies,
        default=display_strategies[:4] if len(display_strategies) >= 4 else display_strategies,
        help="最多可选择8个策略"
    )

    if not selected_display:
        st.info("请选择至少一个策略")
        return

    # 映射回原始策略名称
    reverse_map = {v: k for k, v in strategy_display_map.items()}
    selected_strategies = [reverse_map[d] for d in selected_display]

    # 获取净值数据
    fig = go.Figure()
    colors = ['#d62728', '#1f77b4', '#2ca02c', '#ff7f0e', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']

    for i, strategy in enumerate(selected_strategies[:8]):
        df = xueqiu_adapter.get_strategy_nav_curve(strategy, start_date, end_date)
        if not df.empty:
            display_name = strategy_display_map[strategy]
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['nav'],
                mode='lines',
                name=display_name,
                line=dict(color=colors[i % len(colors)], width=2)
            ))

    fig.add_hline(y=1.0, line_dash="dot", line_color="gray", annotation_text="基准")

    fig.update_layout(
        title="单策略净值对比",
        xaxis_title="日期",
        yaxis_title="净值",
        hovermode="x unified",
        height=500,
        legend=dict(x=1.02, y=1, xanchor='left', yanchor='top')
    )

    fig.update_xaxes(
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1月", step="month", stepmode="backward"),
                dict(count=3, label="3月", step="month", stepmode="backward"),
                dict(count=6, label="6月", step="month", stepmode="backward"),
                dict(step="all", label="全部")
            ])
        )
    )

    st.plotly_chart(fig, width="stretch")

    # 性能指标对比表
    st.markdown("###### 📊 性能指标对比")
    metrics_data = []
    for strategy in selected_strategies:
        metrics = xueqiu_adapter.get_performance_metrics(strategy, start_date, end_date)
        if metrics:
            metrics_data.append({
                '策略': strategy_display_map[strategy],
                '累计收益率': f"{metrics.get('cumulative_return', 0):+.2%}",
                '最大回撤': f"{metrics.get('max_drawdown', 0):.2%}",
                '胜率': f"{metrics.get('win_rate', 0):.2%}",
                '当前净值': f"{metrics.get('current_nav', 1):.4f}"
            })

    if metrics_data:
        st.dataframe(
            pd.DataFrame(metrics_data),
            width="stretch",
            hide_index=True
        )
    else:
        st.info("暂无性能指标数据")
