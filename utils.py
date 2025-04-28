# utils.py
import datetime
import pytz
from config import TIMEZONE, UTC_OFFSET, TASK_STATES

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
        'total': len(task_data)
    }
    
    for task in task_data:
        state_category = categorize_task_state(task['task_state'])
        if state_category in task_summary:
            task_summary[state_category] += 1
    
    # 格式化结果
    result = {
        'dag_run_id': dag_run_data['dag_run_id'],
        'logical_date_local': convert_utc_to_cn_time(dag_run_data['logical_date']),
        'state': dag_run_data['dag_run_state'],
        'PythonOperator': {
            'summary': task_summary
        }
    }
    
    return result