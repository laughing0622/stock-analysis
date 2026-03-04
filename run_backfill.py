import pandas as pd
import numpy as np
import tushare as ts
import sqlite3
import time
import traceback
import logging
import threading
from datetime import datetime, timedelta
from functools import wraps
from config import TS_TOKEN, INDEX_MAP, DB_PATH

# ============================================
# 日志配置
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 错误日志文件
ERROR_LOG_FILE = 'backfill_errors.log'

# ============================================
# 全局速率控制器
# ============================================
class RateLimiter:
    """
    API 请求速率控制器

    Tushare API 限流规则（经验值）：
    - 普通接口：每分钟约 200 次
    - 高频调用可能触发限流

    策略：控制每分钟请求数在 100 次以内，留有余量
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.requests = []  # 记录请求时间戳
        self.max_requests_per_minute = 80  # 每分钟最多 80 次请求
        self.min_interval = 0.3  # 最小间隔 0.3 秒
        self.last_request_time = 0
        self._internal_lock = threading.Lock()

    def wait(self):
        """等待直到可以发送下一个请求"""
        with self._internal_lock:
            now = time.time()

            # 清理 1 分钟前的请求记录
            self.requests = [t for t in self.requests if now - t < 60]

            # 检查是否需要等待（每分钟请求次数限制）
            if len(self.requests) >= self.max_requests_per_minute:
                oldest = self.requests[0]
                wait_time = 60 - (now - oldest) + 1  # 等到最早的请求过期
                if wait_time > 0:
                    logger.debug(f"[速率控制] 达到每分钟限制，等待 {wait_time:.1f} 秒")
                    time.sleep(wait_time)
                    now = time.time()
                    self.requests = [t for t in self.requests if now - t < 60]

            # 确保最小间隔
            elapsed = now - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)

            # 记录本次请求
            self.last_request_time = time.time()
            self.requests.append(self.last_request_time)

# 全局速率控制器实例
rate_limiter = RateLimiter()

def log_error(error_type, detail, context=None):
    """记录错误到日志文件"""
    with open(ERROR_LOG_FILE, 'a', encoding='utf-8') as f:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"\n{'='*60}\n")
        f.write(f"[{timestamp}] {error_type}\n")
        f.write(f"Detail: {detail}\n")
        if context:
            f.write(f"Context: {context}\n")
        f.write(f"{'='*60}\n")

# ============================================
# 重试机制和错误处理
# ============================================
class APIError(Exception):
    """API 调用错误"""
    def __init__(self, message, error_code=None, http_code=None, response=None):
        super().__init__(message)
        self.error_code = error_code
        self.http_code = http_code
        self.response = response

def retry_on_failure(max_retries=3, base_delay=1, backoff=2, exceptions=(Exception,)):
    """
    重试装饰器

    Args:
        max_retries: 最大重试次数
        base_delay: 初始延迟（秒）
        backoff: 延迟倍数
        exceptions: 需要重试的异常类型
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    delay = base_delay * (backoff ** attempt)

                    # 获取详细错误信息
                    error_info = {
                        'function': func.__name__,
                        'attempt': attempt + 1,
                        'max_retries': max_retries,
                        'error_type': type(e).__name__,
                        'error_msg': str(e),
                    }

                    # 尝试获取更多错误信息
                    if hasattr(e, 'response'):
                        try:
                            error_info['http_status'] = getattr(e.response, 'status_code', 'N/A')
                            error_info['response_text'] = getattr(e.response, 'text', 'N/A')[:500]
                        except:
                            pass

                    if attempt < max_retries - 1:
                        logger.warning(
                            f"[重试 {attempt+1}/{max_retries}] {func.__name__} 失败: {e}, "
                            f"{delay}秒后重试"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"[最终失败] {func.__name__} 重试{max_retries}次后仍失败")
                        log_error(
                            f"API调用失败 - {func.__name__}",
                            str(error_info),
                            context=f"args={args[:2] if args else None}"
                        )

            raise last_exception
        return wrapper
    return decorator

def safe_api_call(api_func, *args, max_retries=3, **kwargs):
    """
    安全的 API 调用包装器

    Args:
        api_func: API 函数
        max_retries: 最大重试次数
        *args, **kwargs: 传递给 API 函数的参数

    Returns:
        API 返回的数据 (DataFrame)

    Raises:
        APIError: 如果所有重试都失败
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            # 使用速率控制器等待
            rate_limiter.wait()

            result = api_func(*args, **kwargs)

            # 检查返回结果
            if result is None:
                raise APIError("API 返回 None", error_code="NULL_RESPONSE")

            # Tushare API 可能返回空 DataFrame 或带有错误信息的 DataFrame
            if isinstance(result, pd.DataFrame):
                return result

            return result

        except Exception as e:
            last_error = e
            error_msg = str(e)

            # 解析错误信息
            error_code = None
            http_code = None

            # 常见的 Tushare 错误
            if '接口限制' in error_msg or 'exceed' in error_msg.lower():
                error_code = 'RATE_LIMIT'
                delay = 60  # 限流时等待更长时间
            elif '网络' in error_msg or 'timeout' in error_msg.lower():
                error_code = 'NETWORK_ERROR'
                delay = 5
            elif 'token' in error_msg.lower() or '权限' in error_msg:
                error_code = 'AUTH_ERROR'
                logger.error(f"认证错误，请检查 Tushare Token: {error_msg}")
                raise APIError(error_msg, error_code=error_code)
            else:
                delay = 2 ** attempt  # 指数退避

            if attempt < max_retries - 1:
                logger.warning(
                    f"[API重试 {attempt+1}/{max_retries}] {api_func.__name__ if hasattr(api_func, '__name__') else 'API'} "
                    f"错误: {error_code or 'UNKNOWN'} - {error_msg[:100]}, {delay}秒后重试"
                )
                time.sleep(delay)
            else:
                log_error(
                    f"API调用最终失败",
                    f"Function: {api_func.__name__ if hasattr(api_func, '__name__') else 'unknown'}\n"
                    f"Error: {error_code} - {error_msg}\n"
                    f"Args: {str(args)[:200]}"
                )

    raise APIError(
        f"API 调用失败，重试 {max_retries} 次后仍失败: {last_error}",
        error_code=error_code
    )

# 1. 设置 Tushare
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 2. 辅助函数：获取区间
def get_quarters(start_date, end_date):
    """将大时间段切割为季度区间 [(start, end), ...]"""
    quarters = pd.date_range(start=start_date, end=end_date, freq='Q') # Quarter End
    intervals = []
    
    # 转换逻辑
    curr = datetime.strptime(start_date, '%Y%m%d')
    for q_end in quarters:
        q_end_str = q_end.strftime('%Y%m%d')
        if q_end_str > end_date:
            break
        intervals.append((curr.strftime('%Y%m%d'), q_end_str))
        curr = q_end + timedelta(days=1)
    
    # 加上最后一段 (如果 end_date 不是季度末)
    last_start = curr.strftime('%Y%m%d')
    if last_start <= end_date:
        intervals.append((last_start, end_date))
        
    return intervals

def get_constituents_safe(index_code, date_str):
    """
    智能获取成分股：
    - 上证指数(000001.SH): 获取当日上交所所有上市股票
    - 其他指数: 查询 index_weight
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # === 特殊处理：上证指数 ===
            if index_code == '000001.SH':
                # 获取该日期仍在上市的 SSE 股票
                df = safe_api_call(
                    pro.stock_basic,
                    exchange='SSE',
                    list_status='L',
                    fields='ts_code,list_date,delist_date',
                    max_retries=2
                )
                # 筛选：上市日期 <= date_str
                df = df[df['list_date'] <= date_str]
                return df['ts_code'].tolist()

            # === 常规指数：沪深300/中证500/创业板等 ===
            # 找最近的一个月内的权重数据
            start_dt = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=31)).strftime('%Y%m%d')
            df = safe_api_call(
                pro.index_weight,
                index_code=index_code,
                start_date=start_dt,
                end_date=date_str,
                max_retries=2
            )

            # 如果没查到，尝试往前找半年（应对半年调仓的指数）
            if df.empty:
                start_dt_long = (datetime.strptime(date_str, '%Y%m%d') - timedelta(days=180)).strftime('%Y%m%d')
                df = safe_api_call(
                    pro.index_weight,
                    index_code=index_code,
                    start_date=start_dt_long,
                    end_date=date_str,
                    max_retries=2
                )

            if df.empty:
                logger.warning(f"成分股查询返回空: {index_code} @ {date_str}")
                return []

            # 取离 target_date 最近的一天
            latest_date = df['trade_date'].max()
            codes = df[df['trade_date'] == latest_date]['con_code'].unique().tolist()
            logger.info(f"获取成分股成功: {index_code} @ {date_str} -> {len(codes)} 只")
            return codes

        except APIError as e:
            logger.warning(f"获取成分股失败 (尝试 {attempt+1}/{max_retries}): {index_code} @ {date_str} - {e.error_code}: {str(e)[:50]}")
            if e.error_code == 'RATE_LIMIT':
                time.sleep(60)
            else:
                time.sleep(5)
        except Exception as e:
            logger.warning(f"获取成分股异常 (尝试 {attempt+1}/{max_retries}): {index_code} @ {date_str} - {str(e)[:50]}")
            time.sleep(5)

    log_error("成分股获取失败", f"Index: {index_code}, Date: {date_str}, Attempts: {max_retries}")
    return []

def process_chunk(index_name, index_code, start_date, end_date, all_stocks_set=None):
    """处理一个时间切片 (季度)

    Args:
        index_name: 指数名称
        index_code: 指数代码
        start_date: 开始日期
        end_date: 结束日期
        all_stocks_set: 全市场股票集合（用于非创业板指数）

    Returns:
        df_stats: 统计结果 DataFrame
        stats_info: 包含成功/失败统计的字典
    """
    print(f"   -> 正在处理区间: {start_date} ~ {end_date}")

    # 统计信息
    stats_info = {
        'total_batches': 0,
        'success_batches': 0,
        'failed_batches': 0,
        'total_stocks_expected': 0,
        'api_errors': [],
        'expected_stock_count': 0
    }
    
    # 1. 获取成分股：每个指数使用各自的成分股
    if index_code == '000001.SH':
        # 上证指数：使用上交所全市场股票
        if all_stocks_set is None:
            print(f"      [跳过] 全市场股票集合未提供")
            return pd.DataFrame()
        # 过滤出上交所股票（以 .SH 结尾）
        stock_list = [s for s in all_stocks_set if s.endswith('.SH')]
        print(f"      上证指数成分股数量: {len(stock_list)}")
    elif index_code == '932000.CSI':
        # 中证2000：需要获取成分股
        stock_list = get_constituents_safe(index_code, start_date)
        if not stock_list:
            print(f"      [跳过] 无法获取中证2000成分股列表")
            return pd.DataFrame()
        print(f"      中证2000成分股数量: {len(stock_list)}")
    elif index_code == '399006.SZ':
        # 创业板：需要获取成分股
        stock_list = get_constituents_safe(index_code, start_date)
        if not stock_list:
            print(f"      [跳过] 无法获取创业板成分股列表")
            return pd.DataFrame()
        print(f"      创业板成分股数量: {len(stock_list)}")
    elif index_code == '000300.SH':
        # 沪深300：需要获取成分股
        stock_list = get_constituents_safe(index_code, start_date)
        if not stock_list:
            print(f"      [跳过] 无法获取沪深300成分股列表")
            return pd.DataFrame()
        print(f"      沪深300成分股数量: {len(stock_list)}")
    else:
        # 其他指数：尝试获取成分股，失败则使用全市场
        stock_list = get_constituents_safe(index_code, start_date)
        if not stock_list:
            if all_stocks_set is None:
                print(f"      [跳过] 无法获取成分股且全市场股票集合未提供")
                return pd.DataFrame()
            stock_list = list(all_stocks_set)
        print(f"      {index_name}成分股数量: {len(stock_list)}")

    # 2. 拉取数据 (含40天 Buffer 算均线)
    real_start_date = (datetime.strptime(start_date, '%Y%m%d') - timedelta(days=50)).strftime('%Y%m%d')

    all_dfs = []
    batch_size = 50  # 50只一批，稳定第一

    # 统计信息
    total_batches = (len(stock_list) + batch_size - 1) // batch_size
    stats_info['total_batches'] = total_batches
    stats_info['expected_stock_count'] = len(stock_list)
    failed_batches = []
    api_errors = []

    print(f"      预期成分股: {len(stock_list)} 只, 分 {total_batches} 批处理")
    logger.info(f"[{index_name}] 开始处理 {start_date}~{end_date}, 共 {total_batches} 批")

    for i in range(0, len(stock_list), batch_size):
        batch_num = i // batch_size + 1
        batch = stock_list[i:i+batch_size]
        codes_str = ','.join(batch)

        # 增强的重试机制
        max_retries = 5  # 增加重试次数
        retry_count = 0
        success = False
        batch_errors = []  # 记录该批次的所有错误

        while retry_count < max_retries and not success:
            try:
                # 使用安全 API 调用 - daily
                df_d = safe_api_call(
                    pro.daily,
                    ts_code=codes_str,
                    start_date=real_start_date,
                    end_date=end_date,
                    max_retries=2
                )

                # 使用安全 API 调用 - adj_factor
                df_f = safe_api_call(
                    pro.adj_factor,
                    ts_code=codes_str,
                    start_date=real_start_date,
                    end_date=end_date,
                    max_retries=2
                )

                # 检查数据是否为空
                if df_d.empty:
                    error_msg = f"批次{batch_num}: daily 返回空数据 (codes: {codes_str[:50]}...)"
                    batch_errors.append(error_msg)
                    logger.warning(error_msg)
                    retry_count += 1
                    time.sleep(2 ** retry_count)  # 指数退避
                    continue
                if df_f.empty:
                    error_msg = f"批次{batch_num}: adj_factor 返回空数据 (codes: {codes_str[:50]}...)"
                    batch_errors.append(error_msg)
                    logger.warning(error_msg)
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue

                df_d = df_d[['ts_code', 'trade_date', 'close', 'pct_chg']]
                df_f = df_f[['ts_code', 'trade_date', 'adj_factor']]

                # 获取换手率数据 - 带重试
                all_basic = []
                basic_failed = 0
                basic_errors = []
                for code in batch:
                    code_retry = 0
                    code_success = False
                    while code_retry < 3 and not code_success:
                        try:
                            df_b = safe_api_call(
                                pro.daily_basic,
                                ts_code=code,
                                start_date=real_start_date,
                                end_date=end_date,
                                fields='ts_code,trade_date,turnover_rate',
                                max_retries=2
                            )
                            if not df_b.empty:
                                all_basic.append(df_b)
                            code_success = True
                        except Exception as e:
                            code_retry += 1
                            basic_errors.append(f"{code}: {str(e)[:30]}")
                            time.sleep(0.5)

                    if not code_success:
                        basic_failed += 1

                if basic_failed > 0:
                    error_msg = f"批次{batch_num}: {basic_failed}/{len(batch)} 只股票换手率获取失败"
                    batch_errors.append(error_msg)
                    api_errors.append(error_msg)
                    logger.warning(f"{error_msg}, 示例: {basic_errors[:3]}")

                # 合并数据
                df_m = pd.merge(df_d, df_f, on=['ts_code', 'trade_date'], how='inner')
                if all_basic:
                    df_b_all = pd.concat(all_basic)
                    df_m = pd.merge(df_m, df_b_all, on=['ts_code', 'trade_date'], how='left')
                    df_m['turnover_rate'] = df_m['turnover_rate'].fillna(0)
                else:
                    df_m['turnover_rate'] = 0.0

                all_dfs.append(df_m)
                stats_info['success_batches'] += 1
                success = True

                # 进度显示
                if batch_num % 10 == 0 or batch_num == total_batches:
                    print(f"      进度: {batch_num}/{total_batches} 批完成")

            except APIError as e:
                # API 错误 - 获取详细错误信息
                retry_count += 1
                error_detail = {
                    'batch': batch_num,
                    'error_type': 'APIError',
                    'error_code': e.error_code,
                    'message': str(e),
                    'retry': retry_count
                }
                batch_errors.append(str(error_detail))

                if e.error_code == 'RATE_LIMIT':
                    logger.warning(f"批次{batch_num} 触发限流，等待60秒后重试...")
                    time.sleep(60)
                elif e.error_code == 'AUTH_ERROR':
                    logger.error(f"认证错误，停止处理")
                    raise
                else:
                    delay = 2 ** retry_count
                    logger.warning(f"批次{batch_num} API错误 [{e.error_code}]: {str(e)[:100]}, {delay}秒后重试")
                    time.sleep(delay)

            except Exception as e:
                # 未知错误 - 记录详细堆栈
                retry_count += 1
                error_msg = f"批次{batch_num} 第{retry_count}次异常: {type(e).__name__}: {str(e)[:100]}"
                batch_errors.append(error_msg)

                # 记录完整堆栈到日志文件
                log_error(
                    f"批次处理异常 - {index_name}",
                    f"Batch: {batch_num}\n"
                    f"Codes: {codes_str[:100]}\n"
                    f"Error: {str(e)}\n"
                    f"Traceback:\n{traceback.format_exc()}"
                )

                if retry_count < max_retries:
                    delay = 2 ** retry_count
                    logger.warning(f"{error_msg}, {delay}秒后重试")
                    time.sleep(delay)
                else:
                    failed_batches.append(batch_num)
                    stats_info['failed_batches'] += 1
                    logger.error(f"批次{batch_num} 最终失败: {batch_errors}")
                    print(f"      [警告] 批次{batch_num} 失败: {str(e)[:50]}")

        # 额外的批次间延迟（全局速率控制器已处理基本限流）
        time.sleep(0.5)  # 每批次之间额外等待 0.5 秒

    # 汇总统计信息
    stats_info['api_errors'] = api_errors
    stats_info['failed_batch_list'] = failed_batches

    if not all_dfs:
        print(f"      [错误] 所有批次均失败，无数据返回")
        return pd.DataFrame(), stats_info

    # 3. 合并与计算
    df_all = pd.concat(all_dfs, ignore_index=True)
    df_all = df_all.sort_values(['ts_code', 'trade_date'])

    # 计算复权价
    df_all['hfq_close'] = df_all['close'] * df_all['adj_factor']

    # 计算 MA20
    df_all['ma20'] = df_all.groupby('ts_code')['hfq_close'].transform(lambda x: x.rolling(20).mean())
    df_all['is_above_ma20'] = (df_all['hfq_close'] > df_all['ma20']).astype(int)

    # 计算 连跌3日 (使用 pct_chg)
    df_all['is_down'] = (df_all['pct_chg'] < 0)
    df_all['down_1'] = df_all.groupby('ts_code')['is_down'].shift(1)
    df_all['down_2'] = df_all.groupby('ts_code')['is_down'].shift(2)
    df_all['is_down_3days'] = (df_all['is_down'] & df_all['down_1'] & df_all['down_2']).astype(int)

    # 换手率指标
    df_all['is_turnover_lt_3'] = (df_all['turnover_rate'] < 3.0).astype(int)
    df_all['is_turnover_gt_5'] = (df_all['turnover_rate'] > 5.0).astype(int)

    # 4. 截取有效时间段 (去掉Buffer)
    df_valid = df_all[df_all['trade_date'] >= start_date].copy()

    # 5. 聚合统计
    df_stats = df_valid.groupby('trade_date').agg(
        total_count=('ts_code', 'count'),
        ma20_count=('is_above_ma20', 'sum'),
        down3_count=('is_down_3days', 'sum'),
        turnover_lt_3_count=('is_turnover_lt_3', 'sum'),
        turnover_gt_5_count=('is_turnover_gt_5', 'sum')
    ).reset_index()

    # 数据完整性校验
    avg_count = df_stats['total_count'].mean()
    expected_count = len(stock_list)
    coverage_ratio = avg_count / expected_count if expected_count > 0 else 0

    stats_info['actual_avg_count'] = round(avg_count)
    stats_info['coverage_ratio'] = round(coverage_ratio * 100, 1)
    stats_info['data_quality'] = 'GOOD' if coverage_ratio > 0.8 else ('WARNING' if coverage_ratio > 0.5 else 'BAD')

    # 打印数据质量报告
    print(f"      数据覆盖率: {stats_info['coverage_ratio']}% (平均 {stats_info['actual_avg_count']} 只/预期 {expected_count} 只)")
    if stats_info['failed_batches'] > 0:
        print(f"      [警告] {stats_info['failed_batches']} 个批次失败")
    if coverage_ratio < 0.8:
        print(f"      [警告] 数据覆盖率偏低，建议检查 API 限流或网络问题")

    return df_stats, stats_info

def calculate_crowd_index(pro, trade_date):
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
        print(f"计算拥挤度异常 ({trade_date}): {e}")
        return 0

def run_full_backfill(index_filter=None):
    """
    全量回填市场宽度数据

    Args:
        index_filter: 指定要重建的指数代码列表，如 ['000300.SH', '932000.CSI']
                     None 表示重建所有指数
    """
    # === 配置区域 ===
    from datetime import datetime, timedelta
    START_DATE = '20190101'  # 全量回填起始日期
    END_DATE   = datetime.now().strftime('%Y%m%d')
    # =============

    # 不同指数的起始时间
    INDEX_START_DATE = {
        '000001.SH': '20190101',   # 上证指数
        '000300.SH': '20190101',   # 沪深300
        '399006.SZ': '20190101',   # 创业板指
        '932000.CSI': '20200101',  # 中证2000（2020年发布）
    }

    # 指数名称映射
    INDEX_NAMES = {
        '000001.SH': '上证指数',
        '000300.SH': '沪深300',
        '399006.SZ': '创业板指',
        '932000.CSI': '中证2000',
    }

    # 确定要处理的指数
    if index_filter:
        indices_to_process = {name: code for name, code in INDEX_MAP.items() if code in index_filter}
        print(f" 启动单指数重建: {[INDEX_NAMES.get(c, c) for c in index_filter]}")
    else:
        indices_to_process = INDEX_MAP
        print(f" 启动全量回填任务: {START_DATE} ~ {END_DATE}")
    print(f" 待处理指数: {list(indices_to_process.keys())}")
    
    conn = sqlite3.connect(DB_PATH)
    intervals = get_quarters(START_DATE, END_DATE)
    
    # 获取全市场A股股票列表（用于非创业板指数）
    print(f"\n====== 获取全市场A股列表 ======")
    try:
        df_all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
        all_stocks_set = set(df_all_stocks['ts_code'].tolist())
        print(f"   -> 全市场A股数量: {len(all_stocks_set)}")
    except Exception as e:
        print(f"   [Error] 获取全市场股票列表失败: {e}")
        all_stocks_set = set()
    
    # 先获取所有需要计算拥挤度的日期
    all_dates = []
    for (s_date, e_date) in intervals:
        try:
            df_cal = pro.trade_cal(exchange='SSE', is_open='1', start_date=s_date, end_date=e_date)
            all_dates.extend(df_cal['cal_date'].values)
        except:
            pass
    
    # 去重并排序
    all_dates = sorted(list(set(all_dates)))
    
    # 先计算所有日期的拥挤度，存储到字典中，避免重复计算
    crowd_index_dict = {}
    print(f"\n====== 计算所有日期的拥挤度 ======")
    for i, trade_date in enumerate(all_dates):
        if i % 10 == 0:  # 每10天显示一次进度
            print(f"   计算拥挤度: {i+1}/{len(all_dates)} - {trade_date}")
        crowd_index = calculate_crowd_index(pro, trade_date)
        crowd_index_dict[trade_date] = crowd_index
    
    for index_name, index_code in indices_to_process.items():
        print(f"\n====== 处理指数: {index_name} ======")

        # 获取该指数的起始时间
        index_start = INDEX_START_DATE.get(index_code, START_DATE)
        print(f"   指数起始时间: {index_start}")

        # 生成该指数的时间区间
        index_intervals = get_quarters(index_start, END_DATE)

        # 1. 获取指数自身价格 (用来做主图)
        try:
            print("   -> 拉取指数行情...")
            df_idx_price = pro.index_daily(ts_code=index_code, start_date=index_start, end_date=END_DATE)
            if df_idx_price.empty:
                print(f"   [Skip] {index_name} 指数无行情数据，跳过处理")
                continue

            df_idx_price = df_idx_price[['trade_date', 'close']].rename(columns={'close': 'idx_close'})
            print(f"   -> 成功获取 {len(df_idx_price)} 条指数行情数据")
        except Exception as e:
            print(f"   [Error] 指数行情拉取失败: {e}")
            continue

        # 2. 按季度循环处理
        index_quality_report = []  # 该指数的质量报告

        for (s_date, e_date) in index_intervals:
            try:
                result = process_chunk(index_name, index_code, s_date, e_date, all_stocks_set)

                # 处理新的返回值（元组）
                if isinstance(result, tuple):
                    df_breadth, chunk_stats = result
                else:
                    df_breadth = result
                    chunk_stats = {}

                if not df_breadth.empty:
                    # 合并指数价格
                    df_final = pd.merge(df_breadth, df_idx_price, on='trade_date', how='inner')

                    # 算百分比
                    df_final['pct_above_ma20'] = (df_final['ma20_count'] / df_final['total_count']) * 100
                    df_final['pct_down_3days'] = (df_final['down3_count'] / df_final['total_count']) * 100
                    df_final['pct_turnover_lt_3'] = (df_final['turnover_lt_3_count'] / df_final['total_count']) * 100
                    df_final['pct_turnover_gt_5'] = (df_final['turnover_gt_5_count'] / df_final['total_count']) * 100

                    # 入库
                    data_tuples = []
                    for _, row in df_final.iterrows():
                        trade_date = row['trade_date']
                        # 获取预先计算好的拥挤度，如果没有则为0.0
                        crowd_index = crowd_index_dict.get(trade_date, 0.0)
                        data_tuples.append((
                            trade_date, index_code, index_name,
                            row['idx_close'], row['pct_above_ma20'], row['pct_down_3days'], crowd_index,
                            row['pct_turnover_lt_3'], row['pct_turnover_gt_5']
                        ))

                    c = conn.cursor()
                    c.executemany('INSERT OR REPLACE INTO market_breadth VALUES (?,?,?,?,?,?,?,?,?)', data_tuples)
                    conn.commit()
                    print(f"      [√] 已入库 {len(df_final)} 天数据")

                    # 记录质量信息
                    if chunk_stats:
                        index_quality_report.append({
                            'period': f"{s_date}-{e_date}",
                            'quality': chunk_stats.get('data_quality', 'UNKNOWN'),
                            'coverage': chunk_stats.get('coverage_ratio', 0),
                            'failed_batches': chunk_stats.get('failed_batches', 0)
                        })

            except Exception as e:
                print(f"      [!!!] 区间处理异常: {e}")
                time.sleep(5)

        # 打印该指数的质量报告摘要
        if index_quality_report:
            bad_periods = [r for r in index_quality_report if r['quality'] == 'BAD']
            warning_periods = [r for r in index_quality_report if r['quality'] == 'WARNING']
            if bad_periods:
                print(f"\n   [质量报告] {index_name} 有 {len(bad_periods)} 个时间段数据质量差:")
                for r in bad_periods:
                    print(f"      - {r['period']}: 覆盖率 {r['coverage']}%, 失败批次 {r['failed_batches']}")
            elif warning_periods:
                print(f"\n   [质量报告] {index_name} 有 {len(warning_periods)} 个时间段数据质量警告")

    conn.close()

    # 最终质量报告
    print("\n" + "=" * 50)
    print("全量历史回填完成！")
    print("=" * 50)
    print("\n数据质量校验方法:")
    print("   1. 运行 python check_data_quality.py 检查数据质量")
    print("   2. 检查 market_breadth 表中各指数的数据覆盖率")
    print("   3. 如发现异常，可单独重建某个指数")

def rebuild_single_index(index_code):
    """
    重建单个指数的数据

    Args:
        index_code: 指数代码，如 '000300.SH', '932000.CSI'
    """
    INDEX_NAMES = {
        '000001.SH': '上证指数',
        '000300.SH': '沪深300',
        '399006.SZ': '创业板指',
        '932000.CSI': '中证2000',
    }

    if index_code not in INDEX_NAMES:
        print(f"[错误] 未知的指数代码: {index_code}")
        print(f"支持的指数: {list(INDEX_NAMES.keys())}")
        return

    print(f"\n{'='*50}")
    print(f"单指数重建: {INDEX_NAMES[index_code]} ({index_code})")
    print(f"{'='*50}")

    run_full_backfill(index_filter=[index_code])

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # 命令行参数模式
        arg = sys.argv[1]

        if arg in ['--help', '-h']:
            print("用法:")
            print("  python run_backfill.py              # 重建所有指数")
            print("  python run_backfill.py 000300.SH    # 只重建沪深300")
            print("  python run_backfill.py 932000.CSI   # 只重建中证2000")
            print("  python run_backfill.py 000001.SH    # 只重建上证指数")
            print("  python run_backfill.py 399006.SZ    # 只重建创业板指")
            print("\n支持的指数代码:")
            print("  000001.SH  - 上证指数")
            print("  000300.SH  - 沪深300")
            print("  399006.SZ  - 创业板指")
            print("  932000.CSI - 中证2000")
        else:
            # 单指数重建
            rebuild_single_index(arg)
    else:
        # 全量重建
        run_full_backfill()