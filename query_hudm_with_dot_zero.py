#!/usr/bin/env python3
"""
查询调查点台账合并表中包含'.0'字符的hudm记录
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.append('/home/zpf/mycode/HOUSEHOLD_DATA_SYSTEM_FLASK')

from src.database_pool import get_connection_pool
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def query_hudm_with_dot_zero():
    """查询包含'.0'字符的hudm记录"""
    db = None
    try:
        # 创建数据库连接
        db = DatabasePool()
        logger.info("数据库连接成功")
        
        # 查询包含'.0'的hudm记录
        sql = """
        SELECT DISTINCT hudm 
        FROM 调查点台账合并 
        WHERE hudm LIKE '%.0%'
        ORDER BY hudm
        """
        
        logger.info("开始查询包含'.0'的hudm记录...")
        result = db.execute_query_safe(sql)
        
        if result:
            print(f"\n找到 {len(result)} 个包含'.0'的hudm记录:")
            print("=" * 60)
            for i, row in enumerate(result, 1):
                print(f"{i:3d}. {row[0]}")
            print("=" * 60)
            print(f"总计: {len(result)} 个记录")
        else:
            print("没有找到包含'.0'的hudm记录")
            
    except Exception as e:
        logger.error(f"查询出错: {e}")
        print(f"查询出错: {e}")
    finally:
        if db:
            db.close()
            logger.info("数据库连接已关闭")

if __name__ == "__main__":
    query_hudm_with_dot_zero()
