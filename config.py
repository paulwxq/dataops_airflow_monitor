import os

# 数据库配置
DB_CONFIG = {
    'host': os.environ.get('AIRFLOW_DB_HOST', '192.168.67.10'),
    'port': os.environ.get('AIRFLOW_DB_PORT', 5432),
    'database': os.environ.get('AIRFLOW_DB_NAME', 'airflow'),
    'user': os.environ.get('AIRFLOW_DB_USER', 'admin'),
    'password': os.environ.get('AIRFLOW_DB_PASSWORD', 'admin')
}

# 默认 DAG 配置
DEFAULT_DAG_ID = 'dataops_productline_execute_dag'

# 状态映射配置
TASK_STATES = {
    'success_states': ['success'],
    'failed_states': ['failed'],
    'running_states': ['queued', 'running', 'restarting', 'up_for_retry'],
    'stopped_states': ['shutdown', 'upstream_failed', 'skipped']
}

# 时区配置
TIMEZONE = 'Asia/Shanghai'  # 中国标准时间
UTC_OFFSET = '+08:00'       # UTC 偏移量

# config.py 添加的配置

# 日志路径配置
LOG_DIRECTORY = os.environ.get('AIRFLOW_LOG_DIR', '/opt/airflow/logs')

# 是否使用对象存储 (如 S3) 存储日志
USE_REMOTE_LOGS = os.environ.get('USE_REMOTE_LOGS', 'False').lower() == 'true'

# 对象存储配置 (如果使用)
REMOTE_LOG_CONN = {
    'provider': os.environ.get('REMOTE_LOG_PROVIDER', 's3'),  # 'gcs', 'azure', etc.
    'bucket': os.environ.get('REMOTE_LOG_BUCKET', 'airflow-logs'),
    'region': os.environ.get('REMOTE_LOG_REGION', 'us-east-1'),
    'key': os.environ.get('REMOTE_LOG_KEY', ''),
    'secret': os.environ.get('REMOTE_LOG_SECRET', '')
}