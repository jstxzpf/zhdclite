#!/usr/bin/env python3
"""
迁移失败表的专用脚本：处理主键冲突问题
"""

import os
import sys
import logging
import sqlite3
import pandas as pd

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database_pool_mssql_backup import get_mssql_connection_pool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_failed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def migrate_failed_tables():
    """迁移之前失败的表"""
    failed_tables = ['调查点户名单', '调查品种编码']
    
    try:
        # 连接数据库
        logger.info("连接数据库...")
        mssql_pool = get_mssql_connection_pool()
        sqlite_conn = sqlite3.connect('database.db')
        
        for table_name in failed_tables:
            try:
                logger.info(f"开始迁移表: {table_name}")
                
                # 从MSSQL读取数据
                with mssql_pool.get_cursor() as cursor:
                    cursor.execute(f"SELECT * FROM [{table_name}]")
                    column_names = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    data = [list(row) for row in rows]
                    df = pd.DataFrame(data, columns=column_names)

                    # 处理重复数据 - 保留每个主键的第一条记录
                    primary_key = column_names[0]  # 假设第一列是主键
                    original_count = len(df)
                    df = df.drop_duplicates(subset=[primary_key], keep='first')
                    deduplicated_count = len(df)

                    if original_count != deduplicated_count:
                        logger.info(f"去重处理：原始记录 {original_count} 条，去重后 {deduplicated_count} 条")
                    
                logger.info(f"从MSSQL读取了 {len(df)} 条记录")
                
                # 获取SQLite表结构
                cursor = sqlite_conn.cursor()
                cursor.execute(f"PRAGMA table_info([{table_name}])")
                sqlite_columns = [col[1] for col in cursor.fetchall()]
                
                # 只保留SQLite表中存在的列
                df_filtered = df[[col for col in df.columns if col in sqlite_columns]]
                
                # 删除SQLite表中的所有数据
                cursor.execute(f"DELETE FROM [{table_name}]")
                sqlite_conn.commit()
                logger.info(f"已清空SQLite表: {table_name}")
                
                # 批量插入数据
                logger.info(f"开始插入 {len(df_filtered)} 条记录...")
                
                # 准备插入语句
                placeholders = ','.join(['?' for _ in df_filtered.columns])
                columns_str = ','.join([f'[{col}]' for col in df_filtered.columns])
                insert_sql = f"INSERT INTO [{table_name}] ({columns_str}) VALUES ({placeholders})"
                
                # 转换数据为列表
                data_to_insert = df_filtered.values.tolist()
                
                # 批量插入
                cursor.executemany(insert_sql, data_to_insert)
                sqlite_conn.commit()
                
                logger.info(f"成功迁移表 {table_name}，插入了 {len(df_filtered)} 条记录")
                
            except Exception as e:
                logger.error(f"迁移表 {table_name} 失败: {e}")
                sqlite_conn.rollback()
                continue
        
        # 验证结果
        logger.info("验证迁移结果...")
        cursor = sqlite_conn.cursor()
        for table_name in failed_tables:
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            count = cursor.fetchone()[0]
            logger.info(f"表 {table_name} 现有记录数: {count}")
        
        sqlite_conn.close()
        logger.info("迁移完成！")
        
    except Exception as e:
        logger.error(f"迁移过程中发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    migrate_failed_tables()
