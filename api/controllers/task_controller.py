from services.db_service import DBService
from services.neo4j_service import Neo4jService
from utils import parse_state_parameter, get_actual_states_by_category

class TaskController:
    def __init__(self):
        self.db_service = DBService()
        self.neo4j_service = Neo4jService()
    
    def get_tasks_by_state(self, dag_id, run_id, state_param):
        """
        获取指定状态的任务列表
        
        Args:
            dag_id: DAG ID
            run_id: DAG Run ID
            state_param: 状态参数（如'success,failed'或'all'）
            
        Returns:
            result: 包含状态和任务列表的字典
        """
        # 解析状态参数
        state_categories = parse_state_parameter(state_param)
        
        # 如果状态为'all'或为空，不需要过滤状态
        if 'all' in state_categories or not state_categories:
            tasks = self.db_service.get_tasks_by_run_id(dag_id, run_id)
        else:
            # 获取具体的Airflow状态
            actual_states = get_actual_states_by_category(state_categories)
            
            # 查询数据库
            tasks = self.db_service.get_tasks_by_run_id(dag_id, run_id, actual_states)
        
        # 过滤和处理任务列表
        filtered_tasks = []
        for task in tasks:
            # 从task_id中提取英文名
            task_id = task.get('task_id', '')
            en_name = self._extract_table_name(task_id)
            
            if en_name:
                # 查询Neo4j获取节点信息
                node_exists, cn_name = self.neo4j_service.check_node_by_en_name(en_name)
                
                # 如果节点存在
                if node_exists:
                    # 如果中文名存在，设置为target_table
                    if cn_name:
                        task['target_table'] = cn_name
                    # 否则使用英文名作为target_table
                    else:
                        task['target_table'] = en_name
                    
                    # 将任务添加到过滤后的列表
                    filtered_tasks.append(task)
                # 如果节点不存在，跳过该任务（即从结果中删除）
        
        # 构建结果
        return {
            'dag_id': dag_id,
            'run_id': run_id,
            'tasks': filtered_tasks
        }
    
    def _extract_table_name(self, task_id):
        """
        从task_id中提取表名
        
        Args:
            task_id: 任务ID，例如 "execution_phase.book_sale_amt_daily_clean.py-TO-book_sale_amt_daily_clean"
            
        Returns:
            table_name: 提取的表名，例如 "book_sale_amt_daily_clean"
        """
        if not task_id:
            return None
        
        try:
            # 尝试提取最后一部分，即 book_sale_amt_daily_clean
            parts = task_id.split('-TO-')
            if len(parts) > 1:
                return parts[-1]
            return None
        except Exception:
            return None