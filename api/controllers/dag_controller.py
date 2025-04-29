
from services.db_service import DBService
from utils import convert_cn_date_to_utc_range, convert_utc_to_cn_time, format_dag_run_result

class DAGController:
    def __init__(self):
        self.db_service = DBService()
    
    def get_execution_results(self, dag_id, execution_date):
        """
        获取指定DAG在指定执行日期的执行结果
        
        Args:
            dag_id: DAG ID
            execution_date: 执行日期（中国时区，格式YYYY-MM-DD）
            
        Returns:
            results: API响应结果
        """
        # 转换日期范围
        start_date, end_date = convert_cn_date_to_utc_range(execution_date)
        
        # 查询数据库
        dag_runs, tasks = self.db_service.get_dag_runs_with_tasks(dag_id, start_date, end_date)
        
        if not dag_runs or not tasks:
            return {}
        
        # 构建结果
        results = {}
        for run_id, dag_run in dag_runs.items():
            # 将dag_run_start_date转换为中国时区，作为结果字典的key
            start_date_local = convert_utc_to_cn_time(dag_run['dag_run_start_date'])
            
            # 格式化该DAG Run的结果
            results[start_date_local] = format_dag_run_result(dag_run, tasks.get(run_id, []))
        
        return results