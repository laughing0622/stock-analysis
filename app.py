# 确保当前目录在sys.path最前面，避免导入xueqiu的config
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
# 无条件移除后重新插入到最前面，确保优先级
if current_dir in sys.path:
    sys.path.remove(current_dir)
sys.path.insert(0, current_dir)

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_engine import engine, INDEX_MAP, FUTURES_VARIETIES, FUTURES_NAME_MAP
from datetime import datetime, timedelta
from analysis.stock.wyckoff import WyckoffAnalyzer
from analysis.stock.llm_client import StockLLMClient
from analysis.stock.plotting import plot_wyckoff_chart
from config import GEMINI_API_KEY

# 导入新页面
from pages.tab4_capital import render_capital_tab
from pages.tab5_strategies import render_strategies_tab

st.set_page_config(layout="wide", page_title="AlphaMonitor Pro", page_icon="🦅")

# ==========================================
# 修复1：恢复 Tab 样式 (大字体+加粗)
# ==========================================
st.markdown("""
<style>
    /* 恢复 Tab 样式 */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 18px;
        font-weight: bold;
    }
    /* 调整单选按钮布局 */
    div.row-widget.stRadio > div { flex-direction: row; }
    div.row-widget.stRadio > div > label { 
        background-color: #f0f2f6; padding: 5px 15px; 
        border-radius: 5px; margin-right: 10px; border: 1px solid #e0e0e0;
    }
    div.row-widget.stRadio > div > label[data-baseweb="radio"] { background-color: transparent; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# Tab 5: 个股·深度 (新增)
# ==========================================
def render_stock_tab():
    st.markdown("#### 🔍 个股深度分析 (威科夫/LLM)")
    
    # 初始化 session state
    if 'stock_analysis_result' not in st.session_state:
        st.session_state.stock_analysis_result = None
    if 'stock_analysis_code' not in st.session_state:
        st.session_state.stock_analysis_code = ""

    # 配置区域
    with st.expander("🛠️ 设置与提示词", expanded=(st.session_state.stock_analysis_result is None)):
        c1, c2 = st.columns([1, 1])
        with c1:
            # 优先使用 Config 中的 Key，否则让用户输入
            api_key = GEMINI_API_KEY
            if not api_key:
                api_key = st.text_input("Gemini API Key", type="password", help="未在 config.py 配置，请在此输入")
            else:
                st.success("API Key 已从配置文件加载")
                
            stock_input = st.text_input("股票代码/名称", value="000001", help="支持输入: 600519, 茅台, 000001")
            days_input = st.number_input("分析天数", value=365, min_value=100, max_value=1000, step=100)

            default_system_prompt = """你现在是交易史上最伟大的人物理查德·D·威科夫（Richard D. Wyckoff）。
你需要对我提供的股票行情数据进行大师级的专业分析。
请严格遵循以下JSON格式输出你的分析结果，不要输出任何Markdown代码块标记（如 ```json），直接输出JSON字符串。

JSON输出格式要求：
{
    "analysis_text": "这里写你的威科夫语气分析报告，使用中文，包含对背景、阶段、关键行为的详细解读...",
    "market_phase": "当前所处阶段 (如 Phase A / Phase B / 吸筹 / 派发 / 上升趋势)",
    "phases": [
        {
            "name": "吸筹区/派发区/交易区间",
            "start_date": "YYYY-MM-DD",
            "end_date": "YYYY-MM-DD",
            "top_price": 15.5,
            "bottom_price": 12.0,
            "type": "accumulation" (或 distribution / neutral)
        }
    ],
    "events": [
        {
            "date": "YYYY-MM-DD",
            "type": "SC/ST/Spring/LPS/SOS/UTAD/SOW",
            "description": "简短说明理由"
        }
    ]
}"""
            system_prompt = st.text_area("🤖 角色设定 (System Prompt)", value=default_system_prompt, height=300, help="定义 AI 的角色和输出格式，通常不需要修改")
            
        with c2:
            default_user_prompt = """请重点分析当前的量价结构：
1. 是否出现恐慌抛售(SC)或抢购高潮(BC)？
2. 当前是吸筹还是派发？
3. 对未来一周的走势做出预测。"""
            custom_prompt = st.text_area("✍️ 补充指令 (User Prompt)", value=default_user_prompt, height=150)
        
        btn_analyze = st.button("🧠 开始威科夫分析", width="stretch", type="primary")

    # 执行分析逻辑
    if btn_analyze:
        if not api_key:
            st.error("请先配置 Gemini API Key")
            return
            
        with st.spinner("🕵️‍♂️ 正在寻找该股票..."):
            # 1. 模糊搜索
            ts_code, name = engine.fuzzy_search_stock(stock_input)
            if not ts_code:
                st.error(f"未找到股票: {stock_input}")
                return
            
        with st.spinner(f"📥 正在拉取 {name}({ts_code}) 的历史数据..."):
            # 2. 获取数据
            df = engine.get_stock_data_for_llm(ts_code, days=days_input)
            if df.empty:
                st.error("获取数据失败，请检查 Tushare Token 或网络")
                return
                
        with st.spinner("🤖 威科夫大师正在读图思考 (调用 Gemini)..."):
            # 3. 调用 LLM
            client = StockLLMClient()
            # 如果是临时输入的 Key，手动注入 (虽然 client 是单例，但这里简单处理)
            if not GEMINI_API_KEY and api_key:
                try:
                    import google.genai as genai
                except ImportError:
                    try:
                        from google import genai
                    except ImportError:
                        st.error("无法导入 google.genai 库")
                        return
                        
                client.client = genai.Client(api_key=api_key)
                client.api_available = True

            result = client.analyze_stock(f"{name}({ts_code})", df, custom_prompt, system_prompt)
            
            if "error" in result:
                st.error(result["error"])
                if "raw_response" in result:
                    with st.expander("查看原始返回"):
                        st.code(result["raw_response"])
            else:
                st.session_state.stock_analysis_result = {
                    "data": df,
                    "ai_result": result,
                    "info": {"code": ts_code, "name": name}
                }
                st.rerun()

    # 展示结果
    res = st.session_state.stock_analysis_result
    if res:
        info = res['info']
        ai_res = res['ai_result']
        df = res['data']
        
        st.divider()
        st.markdown(f"### 📊 {info['name']} ({info['code']}) - 威科夫结构图")
        
        # 1. 绘图
        fig = plot_wyckoff_chart(df, ai_res, info['name'])
        st.plotly_chart(fig, width="stretch")
        
        # 2. 报告
        st.markdown("### 📜 威科夫大师诊断报告")
        st.markdown(f"""
        <div style="background-color:#f8f9fa; padding:20px; border-radius:10px; border-left: 5px solid #d62728;">
            {ai_res.get('analysis_text', '大师没有留下任何文字...')}
        </div>
        """, unsafe_allow_html=True)
        
        # 3. 调试信息 (可选)
        with st.expander("查看原始 JSON 数据"):
            st.json(ai_res)


# ==========================================
# Tab 1: 日内·量能
# ==========================================
st.markdown("""
<style>
    /* 恢复 Tab 样式 */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 18px;
        font-weight: bold;
    }
    /* 调整单选按钮布局 */
    div.row-widget.stRadio > div { flex-direction: row; }
    div.row-widget.stRadio > div > label { 
        background-color: #f0f2f6; padding: 5px 15px; 
        border-radius: 5px; margin-right: 10px; border: 1px solid #e0e0e0;
    }
    div.row-widget.stRadio > div > label[data-baseweb="radio"] { background-color: transparent; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# Tab 1: 日内·量能
# ==========================================
def render_intraday_tab():
    # 创建子Tab：量能分析 + 成交量选股
    tab_volume, tab_stock_pick = st.tabs(["量能分析", "成交量选股"])
    
    # === 子Tab 1: 量能分析 ===
    with tab_volume:
        render_volume_analysis()
    
    # === 子Tab 2: 成交量选股 ===
    with tab_stock_pick:
        render_volume_stock_picker()

def render_volume_analysis():
    """量能分析子Tab"""
    if st.button("🔄 刷新数据", key='btn_refresh'): st.rerun()
    with st.spinner("同步分钟数据..."):
        data = engine.get_minute_data_analysis()
    if not data: return st.warning("等待开盘数据...")

    y_nodes, t_nodes = data['yesterday_nodes'], data['today_nodes']
    y_curve, t_curve = data['yesterday_curve'], data['today_curve']
    
    curr_vol = t_curve['cumsum'].iloc[-1] if not t_curve.empty and t_curve['cumsum'].iloc[-1]>0 else 0
    last_tm = t_curve['hhmm'].iloc[-1] if curr_vol>0 else "09:30"
    
    ratio, pred = 0, 0
    if y_nodes and curr_vol > 0:
        row_y = y_curve[y_curve['hhmm'] == last_tm]
        if not row_y.empty:
            y_same = row_y['cumsum'].values[0]
            if y_same>0:
                ratio = (curr_vol - y_same)/y_same
                pred = y_nodes.get('收盘',0)*(1+ratio)

    c1, c2, c3 = st.columns(3)
    c1.metric("当前成交额", f"{curr_vol:,.1f} 亿元")
    c2.metric("预测全天成交额", f"{pred:,.1f}" if pred else "--", f"{ratio*100:+.2f}%" if pred else "等待开盘")
    c3.metric("昨日全天成交额", f"{y_nodes.get('收盘',0):,.1f}")
    st.markdown("---")
    
    c_chart, c_table = st.columns([2,1])
    with c_chart:
        std_times = [(datetime(2000,1,1,9,30)+timedelta(minutes=i)).strftime('%H:%M') for i in range(121)] + \
                    [(datetime(2000,1,1,13,1)+timedelta(minutes=i)).strftime('%H:%M') for i in range(120)]
        df_std = pd.DataFrame({'hhmm': std_times})
        df_py = pd.merge(df_std, y_curve, on='hhmm', how='left')
        df_pt = pd.merge(df_std, t_curve, on='hhmm', how='left')

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_py['hhmm'], y=df_py['cumsum'], name="昨日", line=dict(color='gray', dash='dot')))
        fig.add_trace(go.Scatter(x=df_pt['hhmm'], y=df_pt['cumsum'], name="今日", fill='tozeroy', line=dict(color='#d62728')))
        fig.update_xaxes(type='category', categoryarray=std_times, nticks=8, showspikes=True, spikemode='across', spikesnap='cursor')
        fig.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=0), hovermode="x unified", legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, width="stretch")
    
    with c_table:
        rows = []
        for k in ["竞价/开盘","15分钟","30分钟","60分钟","午盘","收盘"]:
            y, t = y_nodes.get(k,0) if y_nodes else 0, t_nodes.get(k,0) if t_nodes else 0
            d = (t-y)/y*100 if y>0 and t>0 else 0
            icon = "🔥" if d>10 else ("❄️" if d<-10 else "")
            rows.append({"节点":k, "昨日":f"{y:,.0f}", "今日":f"{t:,.0f}" if t else "⏳", "幅度":f"{icon} {d:+.1f}%" if t else "-"})
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

def render_volume_stock_picker():
    """成交量选股子Tab"""
    # 初始化 session_state 保存筛选结果
    if 'volume_picker_result' not in st.session_state:
        st.session_state.volume_picker_result = None
    if 'volume_picker_is_realtime' not in st.session_state:
        st.session_state.volume_picker_is_realtime = False

    # 模式选择和参数设置区域
    col_mode, col_param1, col_param2, col_btn = st.columns([1, 1, 1, 1])
    
    with col_mode:
        screen_mode = st.selectbox(
            "筛选模式",
            ["收盘数据", "实时数据"],
            help="收盘数据：使用Tushare日线数据（全市场，收盘后使用）\n实时数据：优先使用AkShare全市场快照，失败时回退qstock热门100只"
        )
    
    with col_param1:
        vol_threshold = st.number_input(
            "成交额阈值(亿元)",
            min_value=1.0,
            max_value=500.0,
            value=25.0,
            step=1.0,
            help="筛选今日成交额超过该值的个股"
        )
    
    with col_param2:
        vol_ratio_threshold = st.number_input(
            "放量倍数",
            min_value=1.0,
            max_value=5.0,
            value=1.2,
            step=0.1,
            help="筛选今日成交额相比过去5日均值的放量倍数"
        )
    
    with col_btn:
        st.write("")  # 占位
        if st.button("🔍 开始筛选", width="stretch", key="btn_volume_pick"):
            is_realtime = screen_mode.startswith("实时")
            mode_desc = "实时模式（AkShare全市场优先）" if is_realtime else "收盘模式（全市场）"
            with st.spinner(f"正在筛选个股({mode_desc})..."):
                result_df = engine.get_volume_stocks(
                    vol_threshold=vol_threshold,
                    vol_ratio_threshold=vol_ratio_threshold,
                    realtime_mode=is_realtime
                )
                # 保存结果到 session_state
                st.session_state.volume_picker_result = result_df
                st.session_state.volume_picker_is_realtime = is_realtime
    
    # 从 session_state 读取结果
    result_df = st.session_state.volume_picker_result
    is_realtime = st.session_state.volume_picker_is_realtime
    
    # 主区域统一展示筛选结果
    if result_df is not None:
        if result_df.empty:
            if is_realtime:
                st.warning("⚠️ 实时模式未找到符合条件的个股。\n可能原因：1）当前确实无满足条件标的；2）实时数据源拉取失败（网络/代理/反爬），可查看后端日志。")
            else:
                st.warning("暂无符合条件的个股")
        else:
            mode_text = "实时" if is_realtime else "收盘"
            st.success(f"✅ {mode_text}筛选完成，共找到 {len(result_df)} 只个股")
            st.dataframe(
                result_df,
                width="stretch",
                hide_index=True,
                height=1300,
                column_config={
                    "代码": st.column_config.TextColumn("代码", width="small"),
                    "名称": st.column_config.TextColumn("名称", width="medium"),
                    "今日成交额": st.column_config.NumberColumn("今日成交额(亿)", format="%.2f"),
                    "5日均成交额": st.column_config.NumberColumn("5日均成交额(亿)", format="%.2f"),
                    "放量倍数": st.column_config.NumberColumn("放量倍数", format="%.2f")
                }
            )

# ==========================================
# Tab 2: 宏观·择时 (修复 2 & 3 & 4)
# ==========================================
# ==========================================
# Tab 2: 宏观·择时 (修复：去阴影、按钮置顶加大)
# ==========================================
def render_macro_tab():
    # 创建子Tab：择时 + 期指
    tab_timing, tab_futures = st.tabs(["择时指标", "期指监控"])
    
    # === 子Tab 1: 择时指标 ===
    with tab_timing:
        render_macro_timing()
    
    # === 子Tab 2: 期指监控 ===
    with tab_futures:
        render_futures_analysis()

def render_macro_timing():
    """择时指标子Tab"""
    # 顶部按钮区域：指数选择和数据更新
    col_sel, col_mode, col_btn = st.columns([3, 1, 1])
    with col_sel:
        idx_name = st.radio("选择指数:", list(INDEX_MAP.keys()), horizontal=True, label_visibility="collapsed")
    with col_mode:
        macro_update_mode = st.selectbox(
            "macro_mode_selector",
            ["增量更新", "全量重建"],
            key="macro_update_mode",
            label_visibility="collapsed",
            help="增量更新：补充缺失的交易日数据（快速）\n全量重建：重新2019年至今所有数据（慢但完整）"
        )
    with col_btn:
        if st.button("📊 数据更新", width="stretch"):
            if macro_update_mode == "增量更新":
                with st.spinner("正在增量更新数据..."):
                    engine.update_breadth_incremental()
                st.success("✅ 增量数据更新完成")
            else:
                with st.spinner("全量重建中（预计1-3分钟）..."):
                    import subprocess
                    import sys
                    subprocess.run([sys.executable, "run_backfill.py"], cwd=".")
                st.success("✅ 全量数据重建完成")
            st.rerun()

    if not engine.check_breadth_data_exists():
        st.info("正在初始化历史数据...")
        engine.init_mock_history()
        st.rerun()
        
    df = engine.get_breadth_data(idx_name)
    if df.empty: return st.warning("无数据")
    
    df['date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('date', ascending=True)
    latest = df.iloc[-1]
    
    # #region agent log - 检查数据范围
    import json
    log_path = r"d:\stockproject\my work\.cursor\debug.log"
    try:
        latest_date = df['date'].max()
        earliest_date = df['date'].min()
        one_month_ago = latest_date - pd.DateOffset(months=1)
        df_one_month = df[df['date'] >= one_month_ago]
        with open(log_path, 'a', encoding='utf-8') as f:
            log_entry = json.dumps({
                "sessionId": "debug-session", "runId": "pre-fix", "hypothesisId": "H6",
                "location": "app:render_macro_tab", "message": "check_data_range",
                "data": {
                    "total_rows": len(df),
                    "earliest_date": str(earliest_date),
                    "latest_date": str(latest_date),
                    "one_month_ago": str(one_month_ago),
                    "one_month_rows": len(df_one_month),
                    "date_range_days": (latest_date - earliest_date).days
                },
                "timestamp": int(pd.Timestamp.now().timestamp() * 1000)
            }, ensure_ascii=False) + "\n"
            f.write(log_entry)
    except Exception as e:
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                log_entry = json.dumps({
                    "sessionId": "debug-session", "runId": "pre-fix", "hypothesisId": "H6",
                    "location": "app:render_macro_tab", "message": "check_data_range_error",
                    "data": {"error": str(e)},
                    "timestamp": int(pd.Timestamp.now().timestamp() * 1000)
                }, ensure_ascii=False) + "\n"
                f.write(log_entry)
        except: pass
    # #endregion

    # 定义带颜色的指标显示函数
    def colored_metric(col, label, value, color):
        with col:
            # 将完整的HTML结构作为一个字符串传递，并添加unsafe_allow_html=True
            html = f"<div style='text-align: center;'><span style='font-size: 14px; color: #666;'>{label}</span><br><span style='font-size: 24px; font-weight: 600; color: {color};'>{value}</span></div>"
            st.markdown(html, unsafe_allow_html=True)
    
    c1, c2, c3, c4 = st.columns(4)
    
    # 指数点位
    colored_metric(c1, f"{idx_name}点位", f"{latest['close']:,.2f}", "#333333")
    
    # 恐慌情绪
    colored_metric(c2, "恐慌情绪", f"{latest['pct_down_3days']:.1f}%", "#17becf")
    
    # 市场宽度
    colored_metric(c3, "市场宽度", f"{latest['pct_above_ma20']:.1f}%", "#2ca02c")
    
    # 拥挤度指标显示
    crowd_value = latest.get('crowd_index', 0)
    crowd_text = f"{crowd_value:.1f}%"
    
    # 根据拥挤度数值设置颜色
    if crowd_value >= 50:
        crowd_color = "#d62728"  # 红色
    elif crowd_value >= 45:
        crowd_color = "#ff7f0e"  # 橙色
    else:
        crowd_color = "#2ca02c"  # 绿色
    
    colored_metric(c4, "拥挤度", crowd_text, crowd_color)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, 
        row_heights=[0.7, 0.3], specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )

    # 主图
    fig.add_trace(go.Scatter(x=df['date'], y=df['close'], name=f'{idx_name}', line=dict(color='#d62728', width=1.5)), row=1, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=df['date'], y=df['pct_down_3days'], name='恐慌情绪%', mode='lines', line=dict(color='#17becf', width=1.5)), row=1, col=1, secondary_y=True)
    
    # 新增：换手率指标 (默认关闭)
    if 'pct_turnover_lt_3' in df.columns:
        # 100% - 换手率小于3%的股票占比 = 换手率>=3%的股票占比
        y_val_lt3 = 100 - df['pct_turnover_lt_3']
        # 添加9日均线平滑
        y_val_lt3_ma9 = y_val_lt3.rolling(window=9, min_periods=1).mean()
        fig.add_trace(go.Scatter(x=df['date'], y=y_val_lt3_ma9, name='换手率>3%占比(9日均)', visible='legendonly', line=dict(color='#9467bd', width=1.5)), row=1, col=1, secondary_y=True)
    
    if 'pct_turnover_gt_5' in df.columns:
        # 添加9日均线平滑
        y_val_gt5_ma9 = df['pct_turnover_gt_5'].rolling(window=9, min_periods=1).mean()
        fig.add_trace(go.Scatter(x=df['date'], y=y_val_gt5_ma9, name='换手率>5%占比(9日均)', visible='legendonly', line=dict(color='#bcbd22', width=1.5)), row=1, col=1, secondary_y=True)
    
    # 副图 (修复：移除 fill='tozeroy'，仅保留折线)
    fig.add_trace(go.Scatter(x=df['date'], y=df['pct_above_ma20'], name='市场宽度%', line=dict(color='#2ca02c', width=1.5)), row=2, col=1)
    
    # 参考线
    fig.add_hline(y=20, line_dash="dot", line_color="green", opacity=0.5, row=2, col=1)
    fig.add_hline(y=80, line_dash="dot", line_color="red", opacity=0.5, row=2, col=1)

    # 布局调整：按钮置顶、字体加大
    # 修复1月按钮bug：设置初始范围并确保按钮配置正确
    latest_date = df['date'].max()
    one_month_ago = latest_date - pd.DateOffset(months=1)
    
    # #region agent log
    import json
    log_path = r"d:\stockproject\my work\.cursor\debug.log"
    try:
        with open(log_path, 'a', encoding='utf-8') as f:
            log_entry = json.dumps({
                "sessionId": "debug-session", "runId": "pre-fix", "hypothesisId": "H5",
                "location": "app:render_macro_tab", "message": "calc_1month_range",
                "data": {
                    "latest_date": str(latest_date),
                    "one_month_ago": str(one_month_ago),
                    "range_days": (latest_date - one_month_ago).days,
                    "button_config": "count=1, step=month, stepmode=backward"
                },
                "timestamp": int(pd.Timestamp.now().timestamp() * 1000)
            }, ensure_ascii=False) + "\n"
            f.write(log_entry)
    except: pass
    # #endregion
    
    # 修复1月按钮bug：将rangeselector应用到所有子图的x轴
    for i in range(2):
        fig.update_xaxes(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1月", step="month", stepmode="backward"),
                    dict(count=6, label="6月", step="month", stepmode="backward"),
                    dict(count=1, label="1年", step="year", stepmode="backward"),
                    dict(step="all", label="全部")
                ]),
                x=0, y=1.05, # y>1 移至坐标轴上方
                font=dict(size=14, color="#333"), # 字体加大
                bgcolor="#f0f2f6", activecolor="#ff4b4b"
            ),
            type="date",
            # 设置初始范围为最近一个月（修复1月按钮bug）
            range=[one_month_ago, latest_date],
            showspikes=True, 
            spikemode='across', 
            spikesnap='cursor', 
            spikedash='dash', 
            rangebreaks=[dict(bounds=["sat", "mon"])],
            row=i+1, col=1
        )
    
    fig.update_layout(
        height=600, margin=dict(l=10, r=10, t=30, b=0), # 顶部留出空间给按钮
        legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
        hovermode="x unified"
    )
    st.plotly_chart(fig, width="stretch")

def render_futures_analysis():
    """期指监控子Tab"""
    # 顶部刷新按钮
    if st.button("🔄 刷新期指数据", key="btn_futures_refresh"):
        st.rerun()
    
    # 获取日期
    with st.spinner("获取交易日期..."):
        target_date, prev_date = engine.get_futures_smart_date()
    
    if not target_date:
        st.warning("无法获取有效交易日，请稍后再试")
        return
    
    st.caption(f"分析日期: {target_date} | 对比日期: {prev_date}")
    
    # 获取分析数据
    with st.spinner("获取机构持仓数据..."):
        analysis = engine.analyze_futures_position_change(target_date, prev_date)
    
    if not analysis or not analysis.get('varieties'):
        st.warning("暂无期指数据，请检查tushare接口")
        return
    
    # 机构操作汇总：指标卡片展示各品种净单（突出增量）
    st.markdown("##### 🏛️ 中信机构持仓监控")
    cols = st.columns(len(FUTURES_VARIETIES))
    for i, v in enumerate(FUTURES_VARIETIES):
        if v in analysis['varieties']:
            variety_data = analysis['varieties'][v]
            v_name = FUTURES_NAME_MAP.get(v, v)
            total_net = variety_data['total_net']
            total_change = variety_data['total_change']
            
            with cols[i]:
                # 增量大字，总量小字
                st.markdown(
                    f"<div style='text-align: center;'>" 
                    f"<span style='font-size: 14px; color: #666;'>{v_name}</span><br>"
                    f"<span style='font-size: 32px; font-weight: 700; color: {'#d62728' if total_change < 0 else '#2ca02c'};'>{total_change:+d}</span><br>"
                    f"<span style='font-size: 16px; color: #999;'>总: {total_net:+d}手</span>"
                    f"</div>",
                    unsafe_allow_html=True
                )
    
    st.markdown("---")
    
    # 合约期限结构明细（替代详细解读）
    st.markdown("##### 📊 合约调仓明细")
    
    for v in FUTURES_VARIETIES:
        if v not in analysis['varieties']:
            continue
        
        variety_data = analysis['varieties'][v]
        v_name = FUTURES_NAME_MAP.get(v, v)
        contracts = variety_data['contracts']
        
        # 过滤掉无仓位无变动的合约
        active_contracts = [c for c in contracts if c['net_now'] != 0 or c['change'] != 0]
        
        if not active_contracts:
            continue
        
        # 添加解读信息作为标题的一部分
        interpretation = variety_data.get('interpretation', '')
        st.markdown(f"**{v_name}** | {interpretation}")
        
        # 构建数据表
        df_contracts = pd.DataFrame(active_contracts)
        df_contracts = df_contracts[['symbol', 'delist_date', 'net_now', 'change', 'status']]
        df_contracts.columns = ['合约代码', '交割日期', '今日净单', '较昨日变动', '状态']
        
        st.dataframe(
            df_contracts,
            width="stretch",
            hide_index=True,
            column_config={
                '合约代码': st.column_config.TextColumn('合约代码', width="small"),
                '交割日期': st.column_config.TextColumn('交割日期', width="small"),
                '今日净单': st.column_config.NumberColumn('今日净单', format="%d"),
                '较昨日变动': st.column_config.NumberColumn('较昨日变动', format="%+d"),
                '状态': st.column_config.TextColumn('状态', width="medium")
            },
            height=min(200, len(df_contracts) * 35 + 50)
        )
    
    # 解读说明（折叠状态）
    with st.expander("💡 解读说明"):
        st.markdown("""
        **移仓判断逻辑:**
        - 观察 [旧合约] 是否大幅平仓（变动为正）
        - 观察 [新合约] 是否等量加仓（变动为负）
        - 如果 [总变动] 为正，说明移仓过程中【丢弃】了部分空单 → **真实看多**
        
        **操作强度:**
        - |变动量| > 500: 大幅平空/大幅加空
        - |变动量| > 100: 平空/加空
        - 其他: 维持观察
        """)


# ==========================================
# Tab 3: 板块·轮动 (修复：日期格式、数值显示、X轴置顶)
# ==========================================
# ==========================================
# Tab 3: 板块·轮动 (UI 深度优化版)
# ==========================================
def render_sector_tab():
    st.markdown("#### 🌡️ 行业宽度热力图")
    
    # 更新模式选择器和按钮在同一行
    col_mode, col_btn = st.columns([1, 1])
    with col_mode:
        update_mode = st.selectbox(
            "mode_selector",
            ["增量更新", "全量重建"], 
            key="sector_update_mode",
            label_visibility="collapsed",
            help="增量更新：仅更新新增交易日（快速）\n全量重建：重新计算所有数据（慢但完整）"
        )
    with col_btn:
        if st.button("🔄 更新今日数据", width="stretch"):
            is_incremental = (update_mode == "增量更新")
            with st.spinner(f"{'增量更新' if is_incremental else '全量重建'}中(含复权+精准行业，约{'10-30秒' if is_incremental else '1-3分钟'})..."):
                engine.update_sector_breadth(lookback_days=250, incremental=is_incremental)
            st.rerun()

    c1, c2 = st.columns([2, 2])
    with c1: 
        sw_level = st.radio("行业级别", ["申万一级", "申万二级"], horizontal=True)
    with c2: 
        days_lookback = st.radio("显示窗口", [30, 60, 120, 250], horizontal=True, index=1)

    level_code = 'level1' if sw_level == "申万一级" else 'level2'
    # 获取的数据已经是 [最新日期 -> 最旧日期] 的顺序了
    dates, sectors, z_values = engine.get_sector_heatmap_data(level=level_code, days=days_lookback)
    
    if len(dates) == 0:
        st.warning("数据已清空或暂无数据，请点击右上角‘更新今日数据’重新初始化。")
        return

    # === X轴 间隔显示逻辑 ===
    step = 5
    tick_vals = list(range(0, len(dates), step))
    if (len(dates) - 1) not in tick_vals:
        tick_vals.append(len(dates) - 1)
    
    tick_text = [dates[i] for i in tick_vals]

    # 动态高度
    row_height = 25
    fig_height = max(600, len(sectors) * row_height + 120)

    fig = go.Figure(data=go.Heatmap(
        z=z_values, x=dates, y=sectors,
        colorscale='RdYlGn', reversescale=True, 
        text=z_values, texttemplate="%{text}", textfont={"size": 11},  
        xgap=1, ygap=1, hoverongaps=False,
        hovertemplate="<b>%{y}</b><br>日期: %{x}<br>宽度: %{z}%<extra></extra>"
    ))

    fig.update_layout(
        height=fig_height,
        margin=dict(l=0, r=0, t=120, b=0),
        xaxis=dict(
            side='top',
            type='category', 
            tickmode='array',
            tickvals=tick_vals, 
            ticktext=tick_text, 
            tickangle=0,
            tickfont=dict(size=12, color='#333'),
        ),
        yaxis=dict(
            autorange=True,
            tickfont=dict(size=12)
        )
    )
    st.plotly_chart(fig, width="stretch")
    
    with st.expander("查看详细数据表"):
        df_grid = pd.DataFrame(z_values, index=sectors, columns=dates)
        # 【修改】因为 dates 已经是倒序的了，这里直接显示即可，不需要再 [::-1]
        st.dataframe(df_grid.style.background_gradient(cmap='RdYlGn_r', axis=None), width="stretch")

# ==========================================
# Tab 4: 策略·实验室
# ==========================================
def render_strategy_tab():
    st.markdown("#### 🧪 量化策略监控")
    tab_etf, tab_conv, tab_cb = st.tabs(["ETF动量", "配债事件", "可转债低估"])

    # === ETF 动量策略 ===
    with tab_etf:
        st.markdown("##### 📈 ETF 动量策略")
        
        # 更新模式选择器和按钮在同一行
        col_etf_mode, col_etf_btn = st.columns([1, 1])
        with col_etf_mode:
            etf_update_mode = st.selectbox(
                "etf_mode_selector",
                ["增量更新", "全量重建"],
                key="etf_update_mode",
                label_visibility="collapsed",
                help="增量更新：仅获取新增交易日数据（推荐）\n全量重建：重新下载近1.5年所有数据"
            )
        with col_etf_btn:
            if st.button("🔄 同步ETF数据", key="btn_etf_refresh", width="stretch"):
                is_incremental = (etf_update_mode == "增量更新")
                with st.spinner(f"{'增量' if is_incremental else '全量'}刷新 ETF 行情 (含复权计算)..."):
                    engine.update_strategy_data(incremental=is_incremental)
                st.rerun()

        df = engine.get_strategy_rank()
        if df.empty:
            st.warning("无策略数据，请点击右上角刷新。")
        else:
            # 顶部指标卡
            df_a_ok = df[df['策略A_入选'] == '是'].sort_values('策略A_得分', ascending=False)
            top_a = df_a_ok.iloc[0] if not df_a_ok.empty else None
            df_b_sort = df.sort_values('策略B_得分', ascending=False)
            top_b = df_b_sort.iloc[0] if not df_b_sort.empty else None
            risk_count_a = len(df[df['策略A_入选'].str.contains('否')])
            
            m1, m2, m3 = st.columns(3)
            if top_a is not None:
                m1.metric("策略A(趋势) 首选", f"{top_a['_raw_name']}", f"{top_a['策略A_得分']:.1f}")
            else: 
                m1.metric("策略A(趋势) 首选", "空仓", "无标的入选")
            if top_b is not None:
                m2.metric("策略B(因子) 首选", f"{top_b['_raw_name']}", f"{top_b['策略B_得分']:.2f}")
            else: 
                m2.metric("策略B(因子) 首选", "无数据", "--")
            m3.metric("策略A风控拦截", f"{risk_count_a} 只", help="策略A因均线/暴跌/趋势弱被剔除的数量")
            
            st.divider()
            
            display_cols = ["标的", "策略A_得分", "策略A_入选", "策略B_得分"]
            st.dataframe(
                df[display_cols],
                column_config={
                    "标的": st.column_config.TextColumn("ETF 标的", width="medium"),
                    "策略A_得分": st.column_config.ProgressColumn(
                        "策略A (趋势分)", format="%.1f", min_value=-20, max_value=20
                    ),
                    "策略A_入选": st.column_config.TextColumn("策略A 状态", width="small"),
                    "策略B_得分": st.column_config.ProgressColumn(
                        "策略B (因子分)", format="%.2f", min_value=-3, max_value=3
                    ),
                },
                width="stretch",
                hide_index=True,
                height=800
            )

    # === 配债事件驱动策略 ===
    with tab_conv:
        c_head, c_btn = st.columns([3,2])
        with c_head:
            st.markdown("##### 🌩️ 配债事件驱动")
            st.caption("待发转债事件筛选，双因子打分")
        with c_btn:
            if st.button("🔄 重新计算配债", key="btn_conv_refresh"):
                with st.spinner("抓取集思录并计算打分..."):
                    engine.update_convertible_strategy()
                st.rerun()

        res = engine.get_convertible_strategy_rank()
        df_c = res.get("df", pd.DataFrame())
        counts = res.get("counts", {})
        plan = res.get("plan", {"sell": [], "buy": [], "hold": []})
        timing_safe = res.get("timing_safe", True)
        timing_msg = res.get("timing_msg", "正常")

        if df_c.empty:
            st.warning("暂无配债标的，请刷新或检查数据源。")
        else:
            top_c = df_c.iloc[0]
            m1c, m2c, m3c = st.columns(3)
            m1c.metric("Top1 标的", f"{top_c['stock_code']} {top_c['stock_name']}", top_c.get('progress', ''))
            m2c.metric("持仓数量", f"{len(plan.get('hold', []))} / 5", help="上限 5 只")
            status_text = "正常" if timing_safe else "避险空仓"
            m3c.metric("择时状态", status_text, timing_msg)

            if counts:
                st.caption(f"筛选流水：原始 {counts.get('raw','-')} → 阶段 {counts.get('stage','-')} → 基本面 {counts.get('fund','-')}")

            display_conv = df_c.copy()
            # 显示列：合并代码+名称，并给前5标星
            if 'rank_group' not in display_conv.columns:
                display_conv['rank_group'] = display_conv.index.to_series().apply(lambda i: "TOP5" if i < 5 else "NEXT5")
            if 'is_star' not in display_conv.columns:
                display_conv['is_star'] = display_conv['rank_group'].eq("TOP5")
            display_conv['display_name'] = display_conv.apply(
                lambda r: f"{r['stock_code']} {r['stock_name']}" + (" ⭐" if bool(r.get('is_star')) else ""),
                axis=1
            )
            if 'progress_date' in display_conv.columns:
                display_conv['progress_date'] = pd.to_datetime(display_conv['progress_date']).dt.strftime('%Y-%m-%d')
            display_conv = display_conv[
                ['display_name','progress','progress_date','issue_size','market_cap','bond_ratio','pb','score','rank_group']
            ]
            col_cfg = {
                "display_name": st.column_config.TextColumn("标的", width="medium"),
                "progress": st.column_config.TextColumn("进度", width="small"),
                "progress_date": st.column_config.TextColumn("进度日期", width="small"),
                "issue_size": st.column_config.NumberColumn("发行规模(亿)", format="%.2f"),
                "market_cap": st.column_config.NumberColumn("推导市值(亿)", format="%.2f"),
                "bond_ratio": st.column_config.NumberColumn("含权量%", format="%.2f"),
                "pb": st.column_config.NumberColumn("PB", format="%.2f"),
                "score": st.column_config.ProgressColumn("打分", format="%.2f", min_value=-1.0, max_value=1.0),
                "rank_group": st.column_config.TextColumn("分组", width="small"),
            }
            st.dataframe(
                display_conv,
                width="stretch",
                hide_index=True,
                height=600,
                column_config=col_cfg
            )

            with st.expander("交易操作计划"):
                st.write(f"卖出: {plan.get('sell', []) or '无'}")
                st.write(f"买入: {plan.get('buy', []) or '无'}")
                st.write(f"持有: {plan.get('hold', []) or '无'}")

    # === 可转债低估策略 ===
    with tab_cb:
        c_head, c_btn = st.columns([3,2])
        with c_head:
            st.markdown("##### 🧊 可转债低估（双低/多普勒三低）")
            st.caption("剔除NR/强赎，按多普勒三低排序，展示前10，前5标星")
        with c_btn:
            if st.button("🔄 重新计算转债", key="btn_cb_refresh"):
                with st.spinner("抓取集思录可转债并计算..."):
                    engine.update_bond_low_strategy()
                st.rerun()

        res_cb = engine.get_bond_low_strategy()
        df_cb = res_cb.get("df", pd.DataFrame())
        counts_cb = res_cb.get("counts", {})

        if df_cb.empty:
            st.warning("暂无可转债低估结果，请刷新或检查数据源。")
        else:
            # 标星与显示列
            display_cb = df_cb.copy()
            if 'rank_group' not in display_cb.columns:
                display_cb['rank_group'] = display_cb.index.to_series().apply(lambda i: "TOP5" if i < 5 else "NEXT5")
            if 'is_star' not in display_cb.columns:
                display_cb['is_star'] = display_cb['rank_group'].eq("TOP5")
            display_cb['display_name'] = display_cb.apply(
                lambda r: f"{r['代码']} {r['名称']}" + (" ⭐" if bool(r.get('is_star')) else ""),
                axis=1
            )
            display_cb['转股溢价率%'] = display_cb['转股溢价率'] * 100
            display_cb = display_cb[['display_name','现价','转股溢价率%','剩余规模','集思录双低','多普勒三低','强赎状态','rank_group']]

            if counts_cb:
                st.caption(f"数据量：原始 {counts_cb.get('raw','-')} → 剔除强赎/NR后 {counts_cb.get('usable','-')}")

            col_cfg_cb = {
                "display_name": st.column_config.TextColumn("标的", width="medium"),
                "现价": st.column_config.NumberColumn("现价", format="%.3f"),
                "转股溢价率%": st.column_config.NumberColumn("转股溢价率(%)", format="%.2f"),
                "剩余规模": st.column_config.NumberColumn("剩余规模", format="%.3f"),
                "集思录双低": st.column_config.NumberColumn("集思录双低", format="%.3f"),
                "多普勒三低": st.column_config.NumberColumn("多普勒三低", format="%.1f"),
                "强赎状态": st.column_config.TextColumn("强赎状态", width="small"),
                "rank_group": st.column_config.TextColumn("分组", width="small")
            }
            st.dataframe(
                display_cb,
                width="stretch",
                hide_index=True,
                height=600,
                column_config=col_cfg_cb
            )


def main():
    st.title("📈 AlphaMonitor Pro")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["日内", "宏观", "板块", "资金曲线", "量化策略", "个股"])
    with tab1: render_intraday_tab()
    with tab2: render_macro_tab()
    with tab3: render_sector_tab()
    with tab4: render_capital_tab()      # 新增：资金曲线
    with tab5: render_strategies_tab()   # 新增：量化策略（整合Info+Xueqiu）
    with tab6: render_stock_tab()        # 原Tab 5

if __name__ == "__main__":
    main()