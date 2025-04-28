# services/log_service.py
import os
from config import LOG_DIRECTORY

class LogService:
    def __init__(self):
        self.log_directory = LOG_DIRECTORY
    
    def get_log_path(self, dag_id, task_id, dag_run_id, try_number=1):
        """
        获取日志文件路径
        
        Args:
            dag_id: DAG ID
            task_id: 任务 ID
            dag_run_id: DAG Run ID
            try_number: 尝试次数，默认为1
            
        Returns:
            log_path: 日志文件路径
        """
        # 对于 task_id 中包含 task group 的情况（如 group1.task_a），要特殊处理路径
        task_path = task_id.replace('.', '/')
        
        # 构建日志路径
        log_path = os.path.join(
            self.log_directory,
            dag_id,
            task_path,
            dag_run_id,
            f"{try_number}.log"
        )
        
        return log_path
    
    def get_task_log(self, dag_id, task_id, dag_run_id, try_number=1):
        """
        获取任务的日志内容
        
        Args:
            dag_id: DAG ID
            task_id: 任务 ID
            dag_run_id: DAG Run ID
            try_number: 尝试次数，默认为1
            
        Returns:
            log_content: 日志内容
            error: 错误信息（如果有）
        """
        try:
            log_path = self.get_log_path(dag_id, task_id, dag_run_id, try_number)
            
            with open(log_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
                return log_content, None
        except FileNotFoundError:
            return None, f"日志文件不存在: {log_path}"
        except Exception as e:
            return None, f"读取日志文件时发生错误: {str(e)}"