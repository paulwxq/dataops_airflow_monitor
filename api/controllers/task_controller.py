from services.db_service import DBService
from utils import convert_cn_date_to_utc_range, parse_state_parameter, get_actual_states_by_category

class TaskController:
    def __init__(self):
        self.db_service = DBService()
    
    def get_tasks_by_state(self, dag_id, execution_date, state_param):
        """
        获取指定状态的任务列表
        
        Args:
            dag_id: DAG ID
            execution_date: 执行日期（中国时区，格式YYYY-MM-DD）
            state_param: 状态参数（如'success,failed'或'all'）
            
        Returns:
            result: 包含状态和任务列表的字典
        """
        # 转换日期范围
        start_date, end_date = convert_cn_date_to_utc_range(execution_date)
        
        # 解析状态参数
        state_categories = parse_state_parameter(state_param)
        
        # 如果状态为'all'或为空，不需要过滤状态
        if 'all' in state_categories or not state_categories:
            task_ids = self.db_service.get_tasks_by_state(dag_id, start_date, end_date)
            return {
                'state': 'all',
                'tasks': task_ids
            }
        
        # 获取具体的Airflow状态
        actual_states = get_actual_states_by_category(state_categories)
        
        # 查询数据库
        task_ids = self.db_service.get_tasks_by_state(dag_id, start_date, end_date, actual_states)
        
        # 构建结果
        return {
            'state': state_param,
            'tasks': task_ids
        }