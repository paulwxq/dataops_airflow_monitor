from services.neo4j_service import Neo4jService
from utils import logger

class ScriptController:
    def __init__(self):
        self.neo4j_service = Neo4jService()
    
    def get_unscheduled_scripts(self):
        """
        获取所有未调度的脚本及其目标表信息
        
        Returns:
            scripts_list: 包含未调度脚本及目标表信息的列表
        """
        logger.info("获取未调度脚本列表")
        scripts_list = self.neo4j_service.get_unscheduled_list()
        logger.info(f"找到 {len(scripts_list)} 个未调度脚本")
        return scripts_list