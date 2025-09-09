import sqlite3
import pandas as pd
import logging
import os
import gc
from .database_pool import get_connection_pool

class Database:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化SQLite连接池
        self.pool = get_connection_pool()

    def execute_query(self, query, params=None):
        """
        使用连接池执行SQL查询。

        Args:
            query (str): 要执行的SQL查询语句。
            params (tuple, optional): 查询参数. Defaults to None.

        Returns:
            sqlite3.Cursor: 执行查询后的游标对象。
        """
        try:
            # get_cursor() 是一个上下文管理器，会自动处理连接的获取和释放
            with self.pool.get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                # 注意：上下文管理器退出时会自动提交事务
                # 对于需要返回结果集的查询，需要在调用方 fetch 数据
                return cursor
        except Exception as e:
            self.logger.error(f"数据库查询失败: {query[:100]}... - {e}")
            raise

    def execute_query_safe(self, query, params=None):
        """
        安全地执行一个SELECT查询并立即返回所有结果。
        这避免了在将游标传递给其他函数时可能出现的“连接已关闭”的问题。

        Args:
            query (str): 要执行的SQL SELECT查询。
            params (tuple, optional): 查询参数. Defaults to None.

        Returns:
            list: 查询结果的行列表。
        """
        try:
            with self.pool.get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = cursor.fetchall()
                
                # 对大结果集进行垃圾回收
                if len(result) > 1000:
                    gc.collect()
                
                return result
        except Exception as e:
            self.logger.error(f"安全查询失败: {query[:100]}... - {e}")
            raise



    def import_data(self, df, table_name):
        """
        使用连接池将DataFrame数据高效导入到指定表中。
        """
        self.logger.info(f"开始使用连接池导入数据到表: {table_name}")
        
        try:
            with self.pool.get_cursor() as cursor:
                # 1. 清理旧表
                cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                self.logger.info(f"已清理旧表: {table_name}")

                # 2. 根据表名创建新表
                if table_name == '已经编码完成':
                    create_table_sql = """
                    CREATE TABLE [已经编码完成] (
                        [户代码] [nvarchar](max) NULL, [户主姓名] [nvarchar](max) NULL, [type_name] [nvarchar](max) NULL,
                        [数量] [nvarchar](max) NULL, [日期] [nvarchar](max) NULL, [金额] [nvarchar](max) NULL,
                        [备注] [nvarchar](max) NULL, [收支] [nvarchar](max) NULL, [id] [int] NOT NULL,
                        [code] [nvarchar](max) NULL, [年度] [nvarchar](max) NULL, [月份] [nvarchar](max) NULL
                    )"""
                elif table_name == '国家点待导入':
                    create_table_sql = """
                    CREATE TABLE [国家点待导入] (
                        [SID] NVARCHAR(MAX) NULL, [县码] NVARCHAR(MAX) NULL, [样本编码] NVARCHAR(MAX) NULL,
                        [年] NVARCHAR(MAX) NULL, [月] NVARCHAR(MAX) NULL, [页码] NVARCHAR(MAX) NULL,
                        [行码] NVARCHAR(MAX) NULL, [编码] NVARCHAR(MAX) NULL, [数量] REAL NULL, [金额] REAL NULL,
                        [数量2] REAL NULL, [人码] NVARCHAR(MAX) NULL, [是否网购] NVARCHAR(MAX) NULL,
                        [记账方式] NVARCHAR(MAX) NULL, [品名] NVARCHAR(MAX) NULL, [问题类型] NVARCHAR(MAX) NULL,
                        [记账说明] NVARCHAR(MAX) NULL, [记账审核说明] NVARCHAR(MAX) NULL, [记账日期] NVARCHAR(MAX) NULL,
                        [创建时间] NVARCHAR(MAX) NULL, [更新时间] NVARCHAR(MAX) NULL, [账页生成设备标识] NVARCHAR(MAX) NULL,
                        [人代码] NVARCHAR(MAX) NULL
                    )"""
                else:
                    columns = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
                    create_table_sql = f"CREATE TABLE [{table_name}] ({columns})"
                
                cursor.execute(create_table_sql)
                self.logger.info(f"表 {table_name} 创建成功")

                # 3. 准备并执行批量插入
                df_prepared = df.astype(object).where(pd.notna(df), None)
                batch_data = [tuple(row) for row in df_prepared.itertuples(index=False)]

                if not batch_data:
                    self.logger.info("没有数据需要导入。")
                    return {'successful_rows': 0, 'failed_rows': 0, 'total_rows': 0}

                placeholders = ', '.join(['?'] * len(df.columns))
                insert_sql = f"INSERT INTO [{table_name}] ({', '.join(f'[{col}]' for col in df.columns)}) VALUES ({placeholders})"
                
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, batch_data)
                
                successful_rows = cursor.rowcount if cursor.rowcount != -1 else len(batch_data)
                self.logger.info(f"数据导入完成 - 成功: {successful_rows} 行")

                return {
                    'successful_rows': successful_rows,
                    'failed_rows': 0, # fast_executemany 是原子性的
                    'total_rows': len(df)
                }

        except Exception as e:
            self.logger.error(f"导入数据到表 {table_name} 失败: {e}")
            # 异常将在 get_cursor 上下文管理器中被处理（回滚等）
            raise

    def ensure_performance_indexes(self):
        """确保关键表有必要的性能索引"""
        self.logger.info("开始检查和创建性能索引")
        indexes_to_create = [
            ("调查点台账合并", "id", "IX_main_table_id"),
            ("调查点台账合并", "code", "IX_main_table_code"),
            ("调查点台账合并", "hudm", "IX_main_table_hudm"),
            ("调查点台账合并", "year, month", "IX_main_table_year_month"),
            ("调查品种编码", "帐目编码", "IX_coding_table_code")
        ]

        for table, columns, index_name in indexes_to_create:
            try:
                with self.pool.get_cursor() as cursor:
                    check_sql = "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = ?"
                    cursor.execute(check_sql, (table, index_name))
                    if cursor.fetchone()[0] == 0:
                        create_sql = f"CREATE NONCLUSTERED INDEX {index_name} ON {table} ({columns})"
                        cursor.execute(create_sql)
                        self.logger.info(f"成功创建索引: {index_name} on {table}")
            except Exception as e:
                self.logger.warning(f"创建索引 {index_name} 失败 (可能已存在或表不存在): {e}")

    def optimize_table_statistics(self, table_name):
        """更新表的统计信息以优化查询性能"""
        self.logger.info(f"开始更新表统计信息: {table_name}")
        try:
            with self.pool.get_cursor() as cursor:
                cursor.execute(f"UPDATE STATISTICS {table_name}")
            self.logger.info(f"表统计信息更新完成: {table_name}")
        except Exception as e:
            self.logger.warning(f"更新表统计信息失败 {table_name}: {e}")


    def check_table_has_identity_column(self, table_name: str) -> bool:
        """检测指定表是否存在标识（IDENTITY）列"""
        try:
            with self.pool.get_cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND is_identity = 1",
                    (table_name,)
                )
                return (cursor.fetchone()[0] or 0) > 0
        except Exception as e:
            self.logger.warning(f"检查表标识列失败 {table_name}: {e}")
            return False