"""
Tab 5: 量化策略
整合Info原有策略 + Xueqiu四大策略
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
from xueqiu_adapter import xueqiu_adapter


# ==========================================
# 统一持仓显示辅助函数
# ==========================================

def render_position_table(positions: list, total_asset: float = None, title: str = "当前持仓"):
    """
    渲染统一格式的持仓表格（7列标准格式）

    Args:
        positions: 持仓列表，每个元素包含 symbol, name, quantity, cost_price, current_price
        total_asset: 总资产（用于计算占比）
        title: 表格标题
    """
    if not positions:
        st.info(f"暂无{title}")
        return

    # 构建DataFrame
    df = pd.DataFrame(positions)

    # 计算盈亏、市值、占比
    df['盈亏额'] = (df['current_price'] - df['cost_price']) * df['quantity']
    df['盈亏%'] = ((df['current_price'] - df['cost_price']) / df['cost_price'] * 100).round(2)
    df['市值'] = (df['current_price'] * df['quantity']).round(0)

    if total_asset and total_asset > 0:
        df['占比'] = (df['市值'] / total_asset * 100).round(2)
    else:
        df['占比'] = (df['市值'] / df['市值'].sum() * 100).round(2)

    # 按市值降序排列
    df = df.sort_values('市值', ascending=False)

    # 显示表格
    st.markdown(f"**{title}**")

    display_df = df[['symbol', 'name', 'cost_price', 'current_price', '盈亏%', '市值', '占比']].copy()
    display_df.columns = ['代码', '名称', '成本', '现价', '盈亏', '市值', '占比']

    st.dataframe(
        display_df,
        column_config={
            '代码': st.column_config.TextColumn('代码', width='small'),
            '名称': st.column_config.TextColumn('名称', width='medium'),
            '成本': st.column_config.NumberColumn('成本', format="%.2f"),
            '现价': st.column_config.NumberColumn('现价', format="%.2f"),
            '盈亏': st.column_config.NumberColumn(
                '盈亏',
                format="%.2f%%",
                help="盈利红色，亏损绿色"
            ),
            '市值': st.column_config.NumberColumn('市值', format="%,.0f"),
            '占比': st.column_config.NumberColumn('占比', format="%.2f%%")
        },
        width="stretch",
        hide_index=True
    )


def render_strategy_header(strategy_name: str, funds: dict):
    """
    渲染策略标题栏（统一的资金统计格式）

    Args:
        strategy_name: 策略名称
        funds: 资金信息字典，包含 total_asset, current_cash, market_value, nav, daily_return, cumulative_return
    """
    if not funds:
        return

    # 累计收益颜色
    cumulative_return = funds.get('cumulative_return', 0) * 100
    daily_return = funds.get('daily_return', 0) * 100

    cumulative_color = "normal" if cumulative_return >= 0 else "inverse"
    daily_color = "normal" if daily_return >= 0 else "inverse"

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("现金", f"{funds.get('current_cash', 0):,.0f}")
    col2.metric("市值", f"{funds.get('market_value', 0):,.0f}")
    col3.metric(
        "累计收益",
        f"{cumulative_return:+.2f}%",
        delta_color=cumulative_color
    )
    col4.metric(
        "当日收益",
        f"{daily_return:+.2f}%",
        delta_color=daily_color
    )


def render_strategies_tab():
    """渲染量化策略Tab"""
    st.markdown("#### 🤖 量化策略监控")

    # 检查连接
    xueqiu_connected = xueqiu_adapter.check_connection()

    if not xueqiu_connected:
        st.warning("⚠️ 雪球策略数据库未连接，仅显示Info项目策略")

    # 子Tab组织
    tab_xueqiu, tab_etf, tab_cb, tab_jq = st.tabs([
        "雪球组合", "ETF策略", "可转债策略", "聚宽策略"
    ])

    with tab_xueqiu:
        render_xueqiu_portfolios()

    with tab_etf:
        render_etf_strategies()

    with tab_cb:
        render_convertible_bond_strategies()

    with tab_jq:
        render_joinquant_strategy()


def render_xueqiu_portfolios():
    """渲染雪球组合策略（平铺显示，按资金量排序）"""
    st.markdown("##### 🎯 雪球多组合监控")

    if not xueqiu_adapter.check_connection():
        st.error("无法连接数据库")
        return

    # 刷新按钮
    if st.button("🔄 刷新", key="btn_xueqiu_refresh"):
        st.rerun()

    # 获取配置的组合代码
    from config import XUEQIU_STRATEGIES
    portfolios = XUEQIU_STRATEGIES.get('雪球组合', {}).get('portfolios', [])

    if not portfolios:
        st.warning("未配置雪球组合，请在config.py中设置")
        return

    # 获取所有组合的资金信息并按资金量排序
    portfolio_data = []
    for portfolio_code in portfolios:
        strategy_name = f"Xueqiu_Strategy_{portfolio_code}"
        funds = xueqiu_adapter.get_strategy_funds(strategy_name)
        if funds:
            portfolio_name = xueqiu_adapter.get_portfolio_name(portfolio_code)
            portfolio_data.append({
                'code': portfolio_code,
                'name': portfolio_name,
                'funds': funds,
                'total_asset': funds.get('total_asset', 0)
            })

    # 按资金量从大到小排序
    portfolio_data.sort(key=lambda x: x['total_asset'], reverse=True)

    # 平铺显示所有组合
    for i, portfolio in enumerate(portfolio_data):
        portfolio_code = portfolio['code']
        portfolio_name = portfolio['name']
        funds = portfolio['funds']

        # 每个组合用分隔线分开
        if i > 0:
            st.markdown("---")

        # 组合标题（使用组合名）
        st.markdown(f"###### 📌 {portfolio_name} (`{portfolio_code}`)")

        # 资金信息
        render_strategy_header(f"雪球组合-{portfolio_name}", funds)

        # 持仓
        strategy_name = f"Xueqiu_Strategy_{portfolio_code}"
        positions = xueqiu_adapter.get_portfolio_positions(portfolio_code)
        render_position_table(positions, funds.get('total_asset'), f"{portfolio_name} 持仓")


def render_etf_strategies():
    """渲染ETF策略（合并Info + Xueqiu）"""
    st.markdown("##### 📊 ETF策略（动量 + 套利）")

    tab_etf_info, tab_etf_xueqiu = st.tabs(["ETF动量", "ETF套利"])

    with tab_etf_info:
        render_etf_momentum()

    with tab_etf_xueqiu:
        render_etf_arbitrage()


def render_etf_momentum():
    """渲染Info项目ETF动量策略"""
    st.markdown("###### 📈 ETF动量策略")

    # 延迟导入data_engine，避免循环导入
    import data_engine
    engine = data_engine.DataEngine()

    col_mode, col_btn = st.columns([1, 1])
    with col_mode:
        etf_update_mode = st.selectbox(
            "更新模式",
            ["增量更新", "全量重建"],
            key="etf_mode_selector",
            label_visibility="collapsed"
        )
    with col_btn:
        if st.button("🔄 同步ETF数据", key="btn_etf_info_refresh"):
            is_incremental = (etf_update_mode == "增量更新")
            with st.spinner(f"{'增量' if is_incremental else '全量'}刷新 ETF 行情..."):
                engine.update_strategy_data(incremental=is_incremental)
            st.rerun()

    df = engine.get_strategy_rank()
    if df.empty:
        st.warning("无策略数据，请点击刷新。")
    else:
        # 指标卡片
        df_a_ok = df[df['策略A_入选'] == '是'].sort_values('策略A_得分', ascending=False)
        top_a = df_a_ok.iloc[0] if not df_a_ok.empty else None
        df_b_sort = df.sort_values('策略B_得分', ascending=False)
        top_b = df_b_sort.iloc[0] if not df_b_sort.empty else None

        m1, m2, m3 = st.columns(3)
        if top_a is not None:
            m1.metric("策略A(趋势) 首选", f"{top_a['_raw_name']}", f"{top_a['策略A_得分']:.1f}")
        else:
            m1.metric("策略A(趋势) 首选", "空仓", "无标的入选")
        if top_b is not None:
            m2.metric("策略B(因子) 首选", f"{top_b['_raw_name']}", f"{top_b['策略B_得分']:.2f}")
        m3.metric("策略A风控拦截", f"{len(df[df['策略A_入选'].str.contains('否', na=False)])} 只")

        st.divider()

        display_cols = ["标的", "策略A_得分", "策略A_入选", "策略B_得分"]
        st.dataframe(
            df[display_cols],
            column_config={
                "标的": st.column_config.TextColumn("ETF 标的", width="medium"),
                "策略A_得分": st.column_config.ProgressColumn("策略A (趋势分)", format="%.1f", min_value=-20, max_value=20),
                "策略A_入选": st.column_config.TextColumn("策略A 状态", width="small"),
                "策略B_得分": st.column_config.ProgressColumn("策略B (因子分)", format="%.2f", min_value=-3, max_value=3),
            },
            width="stretch",
            hide_index=True,
            height=800
        )


def render_etf_arbitrage():
    """渲染Xueqiu ETF套利策略"""
    st.markdown("###### ⚡ ETF折价套利策略")

    if not xueqiu_adapter.check_connection():
        st.error("无法连接数据库")
        return

    from config import XUEQIU_STRATEGIES
    strategy_name = XUEQIU_STRATEGIES.get('ETF套利', {}).get('strategy_name', 'ETF_Arbitrage_Strategy')

    # 获取资金信息
    funds = xueqiu_adapter.get_strategy_funds(strategy_name)

    if funds:
        render_strategy_header("ETF折价套利", funds)
        st.divider()

    # 获取候选池TOP5
    candidates = xueqiu_adapter.get_candidate_pool(strategy_name, limit=5)
    if candidates:
        st.markdown("**TOP5折价榜**")
        df_candidates = pd.DataFrame(candidates)
        # 从data中提取详细数据
        candidate_details = []
        for c in candidates:
            data = c.get('data', {})
            candidate_details.append({
                '排名': c['rank'],
                '代码': c['symbol'],
                '名称': c['name'],
                '折价率': f"{data.get('premium', 0):.2f}%",
                '现价': f"{data.get('price', 0):.2f}"
            })
        df_top5 = pd.DataFrame(candidate_details)

        st.dataframe(
            df_top5,
            column_config={
                '排名': st.column_config.NumberColumn('排名', width='small'),
                '代码': st.column_config.TextColumn('代码', width='small'),
                '名称': st.column_config.TextColumn('名称', width='medium'),
                '折价率': st.column_config.TextColumn('折价率', width='small'),
                '现价': st.column_config.TextColumn('现价', width='small')
            },
            width="stretch",
            hide_index=True
        )
        st.divider()

    # 获取持仓并统一显示
    positions = xueqiu_adapter.get_virtual_positions(strategy_name)
    render_position_table(positions, funds.get('total_asset') if funds else None, "ETF套利持仓")


def render_convertible_bond_strategies():
    """渲染可转债策略（合并Info + Xueqiu）"""
    st.markdown("##### 🧊 可转债策略（配债 + 多普勒 + 三低轮动）")

    tab_conv_info, tab_cb_info, tab_cb_xueqiu = st.tabs([
        "配债事件", "多普勒三低", "三低轮动"
    ])

    with tab_conv_info:
        render_convertible_event()

    with tab_cb_info:
        render_convertible_low()

    with tab_cb_xueqiu:
        render_convertible_three_low()


def render_convertible_event():
    """渲染配债事件策略"""
    st.markdown("###### 🌩️ 配债事件驱动")

    import data_engine
    engine = data_engine.DataEngine()

    c_head, c_btn = st.columns([3, 2])
    with c_head:
        st.markdown("**待发转债事件筛选，双因子打分**")
    with c_btn:
        if st.button("🔄 重新计算配债", key="btn_conv_refresh"):
            with st.spinner("抓取集思录并计算打分..."):
                engine.update_convertible_strategy()
            st.rerun()

    res = engine.get_convertible_strategy_rank()
    df_c = res.get("df", pd.DataFrame())

    if df_c.empty:
        st.warning("暂无配债标的，请刷新或检查数据源。")
    else:
        top_c = df_c.iloc[0]
        m1c, m2c, m3c = st.columns(3)
        m1c.metric("Top1 标的", f"{top_c['stock_code']} {top_c['stock_name']}")
        m2c.metric("筛选数量", len(df_c))

        st.dataframe(df_c.head(10), width="stretch", hide_index=True, height=500)


def render_convertible_low():
    """渲染可转债低估策略"""
    st.markdown("###### 🧊 多普勒三低策略")

    import data_engine
    engine = data_engine.DataEngine()

    c_head, c_btn = st.columns([3, 2])
    with c_head:
        st.markdown("**剔除NR/强赎，按多普勒三低排序，展示前10**")
    with c_btn:
        if st.button("🔄 重新计算转债", key="btn_cb_low_refresh"):
            with st.spinner("抓取集思录可转债并计算..."):
                engine.update_bond_low_strategy()
            st.rerun()

    res_cb = engine.get_bond_low_strategy()
    df_cb = res_cb.get("df", pd.DataFrame())

    if df_cb.empty:
        st.warning("暂无可转债低估结果，请刷新或检查数据源。")
    else:
        st.dataframe(df_cb.head(10), width="stretch", hide_index=True, height=500)


def render_convertible_three_low():
    """渲染Xueqiu可转债三低轮动策略"""
    st.markdown("###### 🔄 可转债三低轮动策略")

    if not xueqiu_adapter.check_connection():
        st.error("无法连接数据库")
        return

    from config import XUEQIU_STRATEGIES
    strategy_name = XUEQIU_STRATEGIES.get('可转债三低轮动', {}).get('strategy_name', 'CB_ThreeLow_Strategy')

    # 刷新按钮
    col_head, col_btn = st.columns([3, 1])
    with col_head:
        st.markdown("**候选池TOP10（按多普勒三低排序）**")
    with col_btn:
        if st.button("🔄 刷新", key="btn_cb_three_low_refresh"):
            st.rerun()

    # 获取候选池TOP10
    candidates = xueqiu_adapter.get_candidate_pool(strategy_name, limit=10)
    if candidates:
        # 从data中提取详细数据
        candidate_details = []
        for c in candidates:
            data = c.get('data', {})
            candidate_details.append({
                '排名': c['rank'],
                '代码': c['symbol'],
                '名称': c['name'],
                '现价': f"{data.get('现价', 0):.2f}",
                '溢价率': f"{data.get('溢价率', 0):.2f}%",
                '剩余规模': f"{data.get('剩余规模', 0):.1f}亿",
                '双低': f"{data.get('双低', 0):.1f}"
            })
        df_top10 = pd.DataFrame(candidate_details)

        st.dataframe(
            df_top10,
            column_config={
                '排名': st.column_config.NumberColumn('排名', width='small'),
                '代码': st.column_config.TextColumn('代码', width='small'),
                '名称': st.column_config.TextColumn('名称', width='medium'),
                '现价': st.column_config.TextColumn('现价', width='small'),
                '溢价率': st.column_config.TextColumn('溢价率', width='small'),
                '剩余规模': st.column_config.TextColumn('剩余规模', width='small'),
                '双低': st.column_config.TextColumn('双低', width='small')
            },
            width="stretch",
            hide_index=True
        )
        st.divider()
    else:
        st.info("暂无候选池数据，请确保雪球策略服务正在运行")

    # 获取资金信息
    funds = xueqiu_adapter.get_strategy_funds(strategy_name)

    if funds:
        render_strategy_header("可转债三低轮动", funds)
        st.divider()

    # 获取持仓并统一显示
    positions = xueqiu_adapter.get_virtual_positions(strategy_name)
    render_position_table(positions, funds.get('total_asset') if funds else None, "可转债三低轮动持仓")


def render_joinquant_strategy():
    """渲染聚宽策略（支持多策略扩展）"""
    st.markdown("##### 🤖 聚宽策略监控")

    if not xueqiu_adapter.check_connection():
        st.error("无法连接数据库")
        return

    from config import XUEQIU_STRATEGIES

    # 获取所有聚宽策略列表（支持多策略扩展）
    jq_strategies = {}
    for key, value in XUEQIU_STRATEGIES.items():
        if '聚宽' in key or 'JoinQuant' in key:
            strategy_name = value.get('strategy_name', '')
            if strategy_name:
                jq_strategies[key] = strategy_name

    if not jq_strategies:
        st.warning("未配置聚宽策略")
        return

    # 策略选择（如果有多个）
    if len(jq_strategies) > 1:
        selected_strategy_key = st.selectbox(
            "选择策略",
            list(jq_strategies.keys()),
            format_func=lambda x: f"{x} ({jq_strategies[x]})"
        )
        strategy_name = jq_strategies[selected_strategy_key]
    else:
        # 只有一个策略，直接使用
        strategy_name = list(jq_strategies.values())[0]
        st.markdown(f"**当前策略**: `{strategy_name}`")

    # 刷新按钮
    if st.button("🔄 刷新", key="btn_jq_refresh"):
        st.rerun()

    st.divider()

    # 获取资金信息
    funds = xueqiu_adapter.get_strategy_funds(strategy_name)

    if funds:
        render_strategy_header(f"聚宽策略 {strategy_name}", funds)
        st.divider()

    # 获取持仓并统一显示
    positions = xueqiu_adapter.get_virtual_positions(strategy_name)
    render_position_table(positions, funds.get('total_asset') if funds else None, f"{strategy_name} 持仓")
