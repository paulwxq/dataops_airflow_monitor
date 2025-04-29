import os

# 数据库配置
DB_CONFIG = {
    'host': os.environ.get('AIRFLOW_DB_HOST', '192.168.67.10'),
    'port': os.environ.get('AIRFLOW_DB_PORT', 5432),
    'database': os.environ.get('AIRFLOW_DB_NAME', 'airflow'),
    'user': os.environ.get('AIRFLOW_DB_USER', 'postgres'),
    'password': os.environ.get('AIRFLOW_DB_PASSWORD', 'postgres')
}

# Neo4j数据库配置
NEO4J_CONFIG = {
    'uri': os.environ.get('NEO4J_URI', 'bolt://192.168.67.1:7687'),
    'user': os.environ.get('NEO4J_USER', 'neo4j'),
    'password': os.environ.get('NEO4J_PASSWORD', 'Passw0rd')
}

# 日志配置
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')  # 可选：DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILE = os.environ.get('LOG_FILE', None)      # 日志文件路径，None表示仅控制台输出
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

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