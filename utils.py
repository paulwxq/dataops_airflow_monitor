import datetime
import pytz
import logging
import os
from config import TIMEZONE, UTC_OFFSET, TASK_STATES, LOG_LEVEL, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT

# 转换字符串日志级别为logging对象
def get_log_level(level_str):
    """
    将字符串日志级别转换为logging模块的日志级别常量
    
    Args:
        level_str: 日志级别字符串，如 'INFO', 'DEBUG' 等
        
    Returns:
        log_level: logging模块的日志级别常量
    """
    levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return levels.get(level_str.upper(), logging.INFO)

# 日志配置
def setup_logger(name=None, log_level=None, log_file=None):
    """
    创建并配置日志器
    
    Args:
        name: 日志器名称，默认为None表示使用root logger
        log_level: 日志级别，默认为None表示使用配置中的级别
        log_file: 日志文件路径，默认为None表示使用配置中的文件路径
        
    Returns:
        logger: 配置好的日志器对象
    """
    # 使用配置中的值
    if log_level is None:
        log_level = get_log_level(LOG_LEVEL)
    if log_file is None:
        log_file = LOG_FILE
    
    # 创建日志器
    if name:
        logger = logging.getLogger(name)
    else:
        logger = logging.getLogger()
    
    # 避免重复配置
    if logger.handlers:
        return logger
    
    # 设置日志级别
    logger.setLevel(log_level)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # 创建格式化器
    formatter = logging.Formatter(
        LOG_FORMAT,
        LOG_DATE_FORMAT
    )
    
    # 设置控制台处理器的格式
    console_handler.setFormatter(formatter)
    
    # 添加控制台处理器到日志器
    logger.addHandler(console_handler)
    
    # 如果指定了日志文件，创建文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        
        # 添加文件处理器到日志器
        logger.addHandler(file_handler)
    
    return logger

# 创建默认日志器
logger = setup_logger('dataops_airflow_monitor')

def convert_cn_date_to_utc_range(execution_date):
    """
    将中国时区的日期转换为UTC时区的日期范围
    
    Args:
        execution_date: 格式为 'YYYY-MM-DD' 的中国时区日期字符串
        
    Returns:
        start_date_gte: 当天00:00:00的UTC时间
        end_date_lte: 当天23:59:59的UTC时间
    """
    # 创建中国时区对象
    cn_tz = pytz.timezone(TIMEZONE)
    
    # 解析输入日期并设置为中国时区的当天开始时间
    local_date = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    local_start = cn_tz.localize(datetime.datetime.combine(local_date, datetime.time.min))
    local_end = cn_tz.localize(datetime.datetime.combine(local_date, datetime.time.max))
    
    # 转换为UTC时间
    utc_start = local_start.astimezone(pytz.UTC)
    utc_end = local_end.astimezone(pytz.UTC)
    
    return utc_start, utc_end

def convert_utc_to_cn_time(utc_time):
    """
    将UTC时区的时间转换为中国时区的时间字符串
    
    Args:
        utc_time: UTC时区的datetime对象
        
    Returns:
        cn_time_str: 格式为'YYYY-MM-DDTHH:MM:SS.ssssss+08:00'的中国时区时间字符串
    """
    if not utc_time:
        return None
    
    # 确保输入时间有时区信息
    if utc_time.tzinfo is None:
        utc_time = pytz.UTC.localize(utc_time)
    
    # 转换为中国时区
    cn_tz = pytz.timezone(TIMEZONE)
    cn_time = utc_time.astimezone(cn_tz)
    
    # 格式化时间字符串
    return cn_time.isoformat()

def categorize_task_state(state):
    """
    根据任务状态进行分类
    
    Args:
        state: Airflow任务状态字符串
        
    Returns:
        category: 状态分类 ('success', 'failed', 'running', 'stopped')
    """
    if state in TASK_STATES['success_states']:
        return 'success'
    elif state in TASK_STATES['failed_states']:
        return 'failed'
    elif state in TASK_STATES['running_states']:
        return 'running'
    elif state in TASK_STATES['stopped_states']:
        return 'stopped'
    else:
        return 'unknown'  # 处理未知状态

def format_dag_run_result(dag_run_data, task_data):
    """
    格式化DAG Run和Task执行数据为API响应格式
    
    Args:
        dag_run_data: 包含DAG Run信息的字典
        task_data: 包含任务执行信息的列表
        
    Returns:
        formatted_result: 格式化后的结果字典
    """
    # 统计任务状态
    task_summary = {
        'success': 0,
        'failed': 0,
        'running': 0,
        'stopped': 0,
        'scheduled_total': len(task_data)  # 直接使用scheduled_total，不再使用total
    }
    
    for task in task_data:
        state_category = categorize_task_state(task['task_state'])
        if state_category in task_summary:
            task_summary[state_category] += 1
    
    # 格式化单个DAG Run结果
    run_result = {
        'run_id': dag_run_data['dag_run_id'],
        'logical_date_local': convert_utc_to_cn_time(dag_run_data['logical_date']),
        'state': dag_run_data['dag_run_state'],
        'PythonOperator': {
            'summary': task_summary
        }
    }
    
    return run_result

# utils.py 添加的函数
def parse_state_parameter(state_param):
    """
    解析状态参数，返回状态列表
    
    Args:
        state_param: 状态参数字符串，如 'success,failed' 或 'all'
        
    Returns:
        state_list: 包含状态的列表
    """
    if not state_param:
        return []
    
    if state_param.lower() == 'all':
        return ['success', 'failed', 'running', 'stopped']
    
    return [s.strip().lower() for s in state_param.split(',')]

def get_actual_states_by_category(state_categories):
    """
    根据状态类别返回实际的 Airflow 状态列表
    
    Args:
        state_categories: 状态类别列表，如 ['success', 'failed']
        
    Returns:
        actual_states: 实际 Airflow 状态列表
    """
    actual_states = []
    
    for category in state_categories:
        if category == 'success':
            actual_states.extend(TASK_STATES['success_states'])
        elif category == 'failed':
            actual_states.extend(TASK_STATES['failed_states'])
        elif category == 'running':
            actual_states.extend(TASK_STATES['running_states'])
        elif category == 'stopped':
            actual_states.extend(TASK_STATES['stopped_states'])
    
    return actual_states