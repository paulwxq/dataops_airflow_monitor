# services/db_service.py
import psycopg2
import psycopg2.extras
from config import DB_CONFIG

class DBService:
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def get_dag_runs_with_tasks(self, dag_id, start_date, end_date):
        """
        查询指定DAG在时间范围内的所有DAG Run及其任务执行情况
        
        Args:
            dag_id: DAG ID
            start_date: 开始时间（UTC）
            end_date: 结束时间（UTC）
            
        Returns:
            dag_runs: 包含DAG Run信息的字典
            tasks: 包含任务执行信息的字典
        """
        if not self.connect():
            return None, None
        
        try:
            # 执行SQL查询
            sql = """
            SELECT 
              dr.dag_run_id,
              dr.run_id,
              dr.logical_date,
              dr.start_date AS dag_run_start_date,
              dr.state AS dag_run_state,
              ti.task_id,
              ti.state AS task_state
            FROM
              dag_run dr
            JOIN
              task_instance ti
            ON
              dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
            WHERE
              dr.dag_id = %s
              AND dr.start_date BETWEEN %s AND %s
              AND ti.operator = 'PythonOperator'
            ORDER BY
              dr.start_date ASC, ti.task_id ASC;
            """
            
            self.cursor.execute(sql, (dag_id, start_date, end_date))
            results = self.cursor.fetchall()
            
            # 整理数据结构
            dag_runs = {}
            tasks = {}
            
            for row in results:
                row_dict = dict(row)
                run_id = row_dict['run_id']
                
                # 记录DAG Run信息
                if run_id not in dag_runs:
                    dag_runs[run_id] = {
                        'dag_run_id': row_dict['dag_run_id'],
                        'logical_date': row_dict['logical_date'],
                        'dag_run_start_date': row_dict['dag_run_start_date'],
                        'dag_run_state': row_dict['dag_run_state']
                    }
                
                # 记录任务信息
                if run_id not in tasks:
                    tasks[run_id] = []
                
                tasks[run_id].append({
                    'task_id': row_dict['task_id'],
                    'task_state': row_dict['task_state']
                })
            
            return dag_runs, tasks
            
        except Exception as e:
            print(f"查询失败: {e}")
            return None, None
        finally:
            self.disconnect()

# services/db_service.py 添加的方法

def get_tasks_by_state(self, dag_id, start_date, end_date, states=None):
    """
    查询指定状态的任务列表
    
    Args:
        dag_id: DAG ID
        start_date: 开始时间（UTC）
        end_date: 结束时间（UTC）
        states: 状态列表，如果为None则查询所有状态
        
    Returns:
        task_ids: 符合条件的任务ID列表
    """
    if not self.connect():
        return []
    
    try:
        # 构建基础SQL
        sql = """
        SELECT DISTINCT
          ti.task_id
        FROM
          dag_run dr
        JOIN
          task_instance ti
        ON
          dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id
        WHERE
          dr.dag_id = %s
          AND dr.start_date BETWEEN %s AND %s
          AND ti.operator = 'PythonOperator'
        """
        
        params = [dag_id, start_date, end_date]
        
        # 如果指定了状态，添加状态过滤条件
        if states and len(states) > 0:
            placeholders = ','.join(['%s'] * len(states))
            sql += f" AND ti.state IN ({placeholders})"
            params.extend(states)
        
        sql += " ORDER BY ti.task_id ASC"
        
        self.cursor.execute(sql, params)
        results = self.cursor.fetchall()
        
        # 提取任务ID
        task_ids = [row[0] for row in results]
        return task_ids
        
    except Exception as e:
        print(f"查询失败: {e}")
        return []
    finally:
        self.disconnect()