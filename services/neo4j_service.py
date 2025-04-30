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

    def get_unscheduled_list(self):
        """
        查询未调度关系的详细列表
        
        Returns:
            unscheduled_list: 包含未调度关系的详细信息的列表
        """
        if not self.connect():
            return []
        
        try:
            with self.driver.session() as session:
                logger.debug("执行Neo4j查询获取未调度关系详细列表")
                result = session.run("""
                    MATCH (target)-[rel:DERIVED_FROM|ORIGINATES_FROM]->(source)
                    WHERE rel.schedule_status IS NOT NULL AND rel.schedule_status = false
                    RETURN target.name as target_name, target.en_name as target_en_name,
                           source.name as source_name, source.en_name as source_en_name,
                           type(rel) as relation_type
                """)
                
                # 获取结果
                unscheduled_list = []
                for record in result:
                    item = {
                        "target": {
                            "cn_name": record["target_name"],
                            "en_name": record["target_en_name"]
                        },
                        "source": {
                            "cn_name": record["source_name"],
                            "en_name": record["source_en_name"]
                        },
                        "relation_type": record["relation_type"]
                    }
                    unscheduled_list.append(item)
                
                logger.info(f"查询到 {len(unscheduled_list)} 条未调度关系")
                return unscheduled_list
        except Exception as e:
            logger.error(f"查询Neo4j未调度关系详细列表失败: {e}")
            return []
        finally:
            self.disconnect()

    def get_cn_name_by_en_name(self, en_name):
        """
        根据英文名查询节点的中文名
        
        Args:
            en_name: 节点的英文名称
            
        Returns:
            cn_name: 节点的中文名称，如果未找到则返回None
        """
        if not self.connect():
            return None
        
        try:
            with self.driver.session() as session:
                logger.debug(f"执行Neo4j查询获取节点中文名，英文名: {en_name}")
                result = session.run("""
                    MATCH (n)
                    WHERE n.en_name = $en_name
                    RETURN n.name AS cn_name
                """, en_name=en_name)
                
                # 获取结果
                record = result.single()
                if record:
                    cn_name = record["cn_name"]
                    logger.info(f"找到节点中文名: {cn_name}")
                    return cn_name
                logger.info(f"未找到英文名为 {en_name} 的节点")
                return None
        except Exception as e:
            logger.error(f"查询Neo4j节点中文名失败: {e}")
            return None
        finally:
            self.disconnect()

    def check_node_by_en_name(self, en_name):
        """
        根据英文名查询节点是否存在及其中文名
        
        Args:
            en_name: 节点的英文名称
            
        Returns:
            exists: 节点是否存在
            cn_name: 节点的中文名称，如果未找到或为空则返回None
        """
        if not self.connect():
            return False, None
        
        try:
            with self.driver.session() as session:
                logger.debug(f"执行Neo4j查询检查节点，英文名: {en_name}")
                result = session.run("""
                    MATCH (n)
                    WHERE n.en_name = $en_name
                    RETURN n.name AS cn_name
                """, en_name=en_name)
                
                # 获取结果
                record = result.single()
                if record:
                    logger.info(f"找到英文名为 {en_name} 的节点")
                    cn_name = record["cn_name"]  # 可能为None
                    return True, cn_name
                
                logger.info(f"未找到英文名为 {en_name} 的节点")
                return False, None
        except Exception as e:
            logger.error(f"查询Neo4j节点信息失败: {e}")
            return False, None
        finally:
            self.disconnect() 