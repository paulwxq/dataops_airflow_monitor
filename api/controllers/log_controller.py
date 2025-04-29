# api/controllers/log_controller.py
from services.log_service import LogService

class LogController:
    def __init__(self):
        self.log_service = LogService()
    
    def get_task_log(self, dag_id, dag_run_id, task_id, try_number=1):
        """
        获取任务的日志内容
        
        Args:
            dag_id: DAG ID
            dag_run_id: DAG Run ID
            task_id: 任务 ID
            try_number: 尝试次数，默认为1
            
        Returns:
            result: 包含日志内容和元数据的字典
            error: 错误信息（如果有）
        """
        log_content, error = self.log_service.get_task_log(dag_id, task_id, dag_run_id, try_number)
        
        if error:
            return None, error
        
        # 构建结果
        result = {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "try_number": try_number,
            "log": log_content
        }
        
        return result, None