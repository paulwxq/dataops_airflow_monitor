from flask import Blueprint, request, jsonify
from api.controllers.dag_controller import DAGController
from api.controllers.task_controller import TaskController
from api.controllers.log_controller import LogController
from api.controllers.script_controller import ScriptController
from config import MONITOR_DAG_ID

# 创建Blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')

# 控制器实例
dag_controller = DAGController()
task_controller = TaskController()
log_controller = LogController()
script_controller = ScriptController()

@api_bp.route('/dags/exec-results', methods=['GET'])
def get_dag_execution_results():
    """
    获取配置的DAG在指定执行日期的执行结果
    
    URL参数:
        exec_date: 执行日期（中国时区，格式YYYY-MM-DD）
    """
    # 获取查询参数
    exec_date = request.args.get('exec_date')
    
    # 参数验证
    if not exec_date:
        return jsonify({'error': '缺少必需的参数exec_date'}), 400
    
    try:
        # 调用控制器方法
        results = dag_controller.get_execution_results(MONITOR_DAG_ID, exec_date)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': f'处理请求时发生错误: {str(e)}'}), 500

@api_bp.route('/dags/exec-results/tasks', methods=['POST'])
def get_tasks_by_state():
    """
    获取指定状态的任务列表
    
    请求体参数:
        dag_id: DAG ID (必需)
        run_id: DAG Run ID (必需)
        state: 状态参数（如'success,failed'或'all'），可选，默认为'all'
    """
    # 获取请求体数据
    data = request.json
    
    # 参数验证
    if not data:
        return jsonify({'error': '缺少请求体数据'}), 400
    
    if 'dag_id' not in data:
        return jsonify({'error': '缺少必需的参数dag_id'}), 400
    
    if 'run_id' not in data:
        return jsonify({'error': '缺少必需的参数run_id'}), 400
    
    # 获取参数
    dag_id = data['dag_id']
    run_id = data['run_id']
    state = data.get('state', 'all')  # 默认为'all'
    
    try:
        # 调用控制器方法
        results = task_controller.get_tasks_by_state(dag_id, run_id, state)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': f'处理请求时发生错误: {str(e)}'}), 500

@api_bp.route('/dags/exec-results/task-logs', methods=['POST'])
def get_task_logs():
    """
    获取指定任务的日志内容
    
    请求体参数:
        dag_id: DAG ID (必需)
        run_id: DAG Run ID (必需)
        task_id: 任务 ID (必需)
        try_number: 尝试次数，可选，默认为1
    """
    # 获取请求体数据
    data = request.json
    
    # 参数验证
    if not data:
        return jsonify({'error': '缺少请求体数据'}), 400
    
    if 'dag_id' not in data:
        return jsonify({'error': '缺少必需的参数dag_id'}), 400
    
    if 'run_id' not in data:
        return jsonify({'error': '缺少必需的参数run_id'}), 400
    
    if 'task_id' not in data:
        return jsonify({'error': '缺少必需的参数task_id'}), 400
    
    # 获取参数
    dag_id = data['dag_id']
    run_id = data['run_id']
    task_id = data['task_id']
    try_number = data.get('try_number', 1)  # 默认为1
    
    try:
        # 调用控制器方法
        result, error = log_controller.get_task_log(dag_id, run_id, task_id, try_number)
        
        if error:
            return jsonify({'error': error}), 404
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'处理请求时发生错误: {str(e)}'}), 500

@api_bp.route('/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances/<task_id>/log', methods=['GET'])
def get_task_log(dag_id, dag_run_id, task_id):
    """
    获取指定任务的日志内容
    
    URL参数:
        dag_id: DAG ID
        dag_run_id: DAG Run ID
        task_id: 任务 ID
        try_number: 尝试次数，默认为1
    """
    # 获取查询参数
    try_number = request.args.get('try_number', 1, type=int)
    
    try:
        # 调用控制器方法
        result, error = log_controller.get_task_log(dag_id, dag_run_id, task_id, try_number)
        
        if error:
            return jsonify({'error': error}), 404
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f'处理请求时发生错误: {str(e)}'}), 500

@api_bp.route('/dags/unscheduled-scripts', methods=['GET'])
def get_unscheduled_scripts():
    """
    获取所有未调度的脚本及其目标表信息
    
    返回:
        包含未调度脚本及目标表信息的列表
    """
    try:
        # 调用控制器方法
        scripts_list = script_controller.get_unscheduled_scripts()
        return jsonify(scripts_list)
    except Exception as e:
        return jsonify({'error': f'处理请求时发生错误: {str(e)}'}), 500