from services.db_service import DBService
from services.neo4j_service import Neo4jService
from utils import convert_cn_date_to_utc_range, convert_utc_to_cn_time, format_dag_run_result, logger

class DAGController:
    def __init__(self):
        self.db_service = DBService()
        self.neo4j_service = Neo4jService()
    
    def get_execution_results(self, dag_ids, execution_date):
        """
        获取指定DAG在指定执行日期的执行结果
        
        Args:
            dag_ids: DAG ID 列表
            execution_date: 执行日期（中国时区，格式YYYY-MM-DD）
            
        Returns:
            results: API响应结果
        """
        logger.info(f"获取DAG执行结果: dag_ids={dag_ids}, execution_date={execution_date}")
        
        # 转换日期范围
        start_date, end_date = convert_cn_date_to_utc_range(execution_date)
        logger.debug(f"转换后的UTC时间范围: {start_date} - {end_date}")
        
        # 查询Neo4j中未调度节点的数量
        logger.info("开始查询Neo4j中未调度节点的数量")
        unscheduled_count = self.neo4j_service.get_unscheduled_count()
        logger.info(f"未调度节点数量: {unscheduled_count}")
        
        # 最终结果数组
        results = []
        
        # 对每个DAG ID进行处理
        for dag_id in dag_ids:
            # 查询数据库
            dag_runs, tasks = self.db_service.get_dag_runs_with_tasks(dag_id, start_date, end_date)
            
            # 构建结果
            scheduled_runs = {}
            scheduled_total = 0  # 初始化为0
            
            if dag_runs and tasks:
                for run_id, dag_run in dag_runs.items():
                    # 将dag_run_start_date转换为中国时区，作为结果字典的key
                    start_date_local = convert_utc_to_cn_time(dag_run['dag_run_start_date'])
                    
                    # 获取该DAG Run的任务列表
                    task_list = tasks.get(run_id, [])
                    
                    # 计算任务总数（用于提取scheduled_total）
                    task_count = len(task_list)
                    if task_count > scheduled_total:
                        scheduled_total = task_count  # 使用最大的任务数作为scheduled_total
                    
                    # 格式化该DAG Run的结果
                    formatted_result = format_dag_run_result(dag_run, task_list)
                    
                    # 确保PythonOperator.summary中有scheduled_total而不是total
                    if 'PythonOperator' in formatted_result and 'summary' in formatted_result['PythonOperator']:
                        summary = formatted_result['PythonOperator']['summary']
                        if 'total' in summary:
                            summary['scheduled_total'] = summary.pop('total')
                    
                    scheduled_runs[start_date_local] = formatted_result
            
            # 计算总数 (确保scheduled_total不为None)
            total = (scheduled_total or 0) + unscheduled_count
            
            # 构建单个DAG的响应
            dag_result = {
                "dag_id": dag_id,
                "scheduled_runs": scheduled_runs,
                "unscheduled_total": unscheduled_count,  # 将unscheduled_summary改为unscheduled_total
                "total": total  # 添加新字段 total
            }
            
            # 添加到结果数组
            results.append(dag_result)
        
        logger.info("API响应结果构建完成")
        return results