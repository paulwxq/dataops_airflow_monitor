from neo4j import GraphDatabase
from config import NEO4J_CONFIG
from utils import logger

class Neo4jService:
    def __init__(self):
        self.driver = None
    
    def connect(self):
        """建立Neo4j数据库连接"""
        try:
            self.driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'],
                auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
            )
            return True
        except Exception as e:
            logger.error(f"Neo4j数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """关闭Neo4j数据库连接"""
        if self.driver:
            self.driver.close()
    
    def get_unscheduled_count(self):
        """
        查询未调度节点的数量
        
        Returns:
            count: 未调度节点的数量
        """
        if not self.connect():
            return 0
        
        try:
            with self.driver.session() as session:
                logger.debug("执行Neo4j查询获取未调度关系数量")
                result = session.run("""
                    MATCH (target)-[rel:DERIVED_FROM|ORIGINATES_FROM]->(source)
                    WHERE rel.schedule_status IS NOT NULL AND rel.schedule_status = false
                    RETURN COUNT(DISTINCT rel) AS count
                """)
                
                # 获取结果
                record = result.single()
                if record:
                    count = record["count"]
                    logger.info(f"未调度关系数量: {count}")
                    return count
                logger.info("未找到符合条件的未调度关系")
                return 0
        except Exception as e:
            logger.error(f"查询Neo4j未调度节点数量失败: {e}")
            return 0
        finally:
            self.disconnect() 