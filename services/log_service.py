import os
import requests
import base64
from config import LOG_DIRECTORY, AIRFLOW_API_CONFIG
from utils import logger

class LogService:
    def __init__(self):
        self.log_directory = LOG_DIRECTORY
        self.airflow_api_config = AIRFLOW_API_CONFIG
    
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
        获取任务的日志内容，优先通过Airflow API获取，如失败则尝试从本地文件读取
        
        Args:
            dag_id: DAG ID
            task_id: 任务 ID
            dag_run_id: DAG Run ID
            try_number: 尝试次数，默认为1
            
        Returns:
            log_content: 日志内容
            error: 错误信息（如果有）
        """
        # 首先尝试通过Airflow API获取日志
        log_content, error = self.fetch_airflow_log(dag_id, dag_run_id, task_id, try_number)
        if log_content is not None:
            return log_content, None
            
        # 如果API获取失败，记录并尝试从本地文件读取
        logger.warning(f"回退到本地文件系统获取日志: dag_id={dag_id}, task_id={task_id}, run_id={dag_run_id}, try_number={try_number}")
        try:
            log_path = self.get_log_path(dag_id, task_id, dag_run_id, try_number)
            
            with open(log_path, 'r', encoding='utf-8') as f:
                log_content = f.read()
                logger.info(f"成功从本地文件系统获取日志: {log_path}")
                return log_content, None
        except FileNotFoundError:
            error_msg = f"日志文件不存在: {log_path}"
            logger.error(error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"读取日志文件时发生错误: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
            
    def fetch_airflow_log(self, dag_id, run_id, task_id, try_number):
        """
        通过Airflow API获取任务日志
        
        Args:
            dag_id: DAG ID
            run_id: DAG Run ID
            task_id: 任务 ID
            try_number: 尝试次数
            
        Returns:
            log_content: 日志内容
            error: 错误信息（如果有）
        """
        try:
            # 构建API URL
            url = f"{self.airflow_api_config['base_url']}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
            
            # 添加查询参数 - 一次获取全部内容
            params = {"full_content": "true"}
            
            # 记录完整URL（包括参数）
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{url}?{query_string}" if query_string else url
            logger.info(f"发送请求到Airflow REST API: URL={full_url}")
            
            # 构建认证信息
            auth_str = f"{self.airflow_api_config['username']}:{self.airflow_api_config['password']}"
            encoded_auth = base64.b64encode(auth_str.encode()).decode()
            
            # 设置请求头
            headers = {
                "Authorization": f"Basic {encoded_auth}",
                "Accept": "application/json"
            }
            
            # 发送请求
            response = requests.get(url, params=params, headers=headers)
            
            # 记录响应状态和内容大小
            logger.info(f"Airflow API响应状态: {response.status_code}, 内容大小: {len(response.text)} 字节")
            
            # 检查响应状态
            if response.status_code == 200:
                # 尝试解析JSON响应
                try:
                    data = response.json()
                    
                    # 记录原始响应数据以便调试
                    logger.debug(f"Airflow API原始响应数据: {data}")
                    
                    # 提取日志内容
                    log_content = None
                    
                    if isinstance(data, list) and len(data) > 0:
                        # 打印更多调试信息
                        logger.info(f"响应是数组类型，长度为 {len(data)}")
                        if len(data) > 0:
                            logger.info(f"第一个元素类型: {type(data[0]).__name__}")
                            if isinstance(data[0], dict):
                                logger.info(f"第一个元素字典键: {list(data[0].keys())}")

                        if isinstance(data[0], dict):
                            log_content = data[0].get("content", "")
                            
                            # 如果内容是元组字符串表示，需要特殊处理
                            if isinstance(log_content, str) and log_content.startswith("[('") and log_content.endswith("')]"):
                                # 检查是否是空内容，如 "[('ubuntu', '')]"
                                if "('')" in log_content or "(\"\")'" in log_content:
                                    logger.warning(f"收到空日志内容: {log_content}")
                                    if len(log_content.strip()) <= 15:  # 大约 "[('ubuntu', '')]" 的长度
                                        log_content = ""  # 返回真正的空字符串而不是格式化的空元组
                                        
                    elif isinstance(data, dict):
                        log_content = data.get("content", "")
                    else:
                        log_content = str(data)
                    
                    return log_content, None
                    
                except Exception as e:
                    logger.warning(f"解析Airflow API响应失败: {e}")
                    # 在解析失败时，尝试将原始响应返回给客户端
                    return response.text, None
            else:
                error_msg = f"获取日志失败: {response.status_code} - {response.text}"
                logger.warning(f"通过Airflow REST API获取日志失败: URL={full_url}, 状态码={response.status_code}")
                return None, error_msg
                
        except Exception as e:
            error_msg = f"请求Airflow API时发生错误: {str(e)}"
            logger.warning(f"访问Airflow REST API失败: URL={(url if 'url' in locals() else 'unknown') + (('?' + query_string) if 'query_string' in locals() else '')}, 错误={str(e)}")
            return None, error_msg
    
    def _extract_log_text(self, data):
        """
        从Airflow API返回的数据中提取实际的日志文本
        
        Args:
            data: Airflow API返回的数据
            
        Returns:
            log_text: 提取出的日志文本
        """
        try:
            # 记录原始数据类型和部分内容以便调试
            data_type = type(data).__name__
            data_preview = str(data)[:500] + '...' if len(str(data)) > 500 else str(data)
            logger.debug(f"从Airflow API获取的原始数据类型: {data_type}, 预览: {data_preview}")
            
            # 首先尝试直接返回完整响应（JSON格式），避免内容丢失
            import json
            if isinstance(data, (dict, list)):
                return json.dumps(data, ensure_ascii=False, indent=2)
                
            # 如果是字符串，直接返回
            if isinstance(data, str):
                return data
                
            # 其他类型，转换为字符串
            return str(data)
            
        except Exception as e:
            logger.error(f"提取日志文本时出错: {e}")
            return str(data)