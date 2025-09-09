#!/usr/bin/env python3
"""
数据迁移脚本：从MSSQL数据库复制数据到SQLite数据库
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
from datetime import datetime
import json

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from database_pool_mssql_backup import get_mssql_connection_pool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataMigrator:
    """数据迁移器"""

    def __init__(self):
        self.mssql_pool = None
        self.sqlite_conn = None
        # 仅迁移实际使用的基础表；视图在SQLite侧重新创建
        self.tables_to_migrate = [
            '调查点户名单',
            '调查品种编码',
            '调查点台账合并',
            '国家点待导入',
            '调查点村名单',
            # 视图不直接迁移
        ]

    def connect_databases(self):
        """连接数据库"""
        try:
            # 连接MSSQL
            logger.info("连接MSSQL数据库...")
            self.mssql_pool = get_mssql_connection_pool()
            logger.info("MSSQL数据库连接成功")

            # 连接SQLite
            logger.info("连接SQLite数据库...")
            self.sqlite_conn = sqlite3.connect('database.db')
            logger.info("SQLite数据库连接成功")

            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def get_mssql_table_data(self, table_name):
        """从MSSQL获取表数据"""
        try:
            logger.info(f"从MSSQL读取表: {table_name}")

            with self.mssql_pool.get_cursor() as cursor:
                # 获取数据
                cursor.execute(f"SELECT * FROM [{table_name}]")

                # 获取列名
                column_names = [desc[0] for desc in cursor.description]
                logger.info(f"表 {table_name} 的列: {column_names}")

                # 获取数据
                rows = cursor.fetchall()

                if not rows:
                    logger.warning(f"表 {table_name} 无数据")
                    return pd.DataFrame()

                # 将pyodbc.Row对象转换为列表
                data = [list(row) for row in rows]

                # 转换为DataFrame
                df = pd.DataFrame(data, columns=column_names)
                logger.info(f"从 {table_name} 读取了 {len(df)} 条记录，{len(df.columns)} 列")

                return df

        except Exception as e:
            logger.error(f"读取MSSQL表 {table_name} 失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def clear_sqlite_table(self, table_name):
        """清空SQLite表数据"""
        try:
            cursor = self.sqlite_conn.cursor()
            cursor.execute(f"DELETE FROM [{table_name}]")
            self.sqlite_conn.commit()
            logger.info(f"已清空SQLite表: {table_name}")
        except Exception as e:
            logger.error(f"清空SQLite表 {table_name} 失败: {e}")
            raise

    def insert_sqlite_data(self, table_name, df):
        """将数据插入SQLite表"""
        try:
            logger.info(f"向SQLite表 {table_name} 插入 {len(df)} 条记录")

            # 获取SQLite表结构
            cursor = self.sqlite_conn.cursor()
            cursor.execute(f"PRAGMA table_info([{table_name}])")
            sqlite_columns = [col[1] for col in cursor.fetchall()]

            # 只保留SQLite表中存在的列
            df_filtered = df[[col for col in df.columns if col in sqlite_columns]].copy()

            # 如果目标表有主键列，先按主键去重，避免UNIQUE冲突
            pk_col = None
            for col_info in cursor.execute(f"PRAGMA table_info([{table_name}])").fetchall():
                # PRAGMA table_info 返回列：cid, name, type, notnull, dflt_value, pk
                if col_info[5] == 1:
                    pk_col = col_info[1]
                    break
            if pk_col and pk_col in df_filtered.columns:
                before = len(df_filtered)
                df_filtered = df_filtered.drop_duplicates(subset=[pk_col], keep='last')
                logger.info(f"检测到主键列 {pk_col}，已按主键去重：{before} -> {len(df_filtered)} 条")

            # 处理数据类型转换
            df_filtered = self.convert_data_types(df_filtered, table_name)

            # 插入数据
            df_filtered.to_sql(table_name, self.sqlite_conn, if_exists='append', index=False)
            self.sqlite_conn.commit()

            logger.info(f"成功插入 {len(df_filtered)} 条记录到 {table_name}")
            return len(df_filtered)

        except Exception as e:
            logger.error(f"插入数据到SQLite表 {table_name} 失败: {e}")
            self.sqlite_conn.rollback()
            raise

    def convert_data_types(self, df, table_name):
        """转换数据类型以适配SQLite"""
        try:
            # 处理日期时间列
            datetime_columns = ['创建时间', '更新时间', 'date', '日期']
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # 处理数值列
            numeric_columns = ['amount', 'money', '数量', '金额', 'type', '收支类别', '人数', '家庭人口', '是否退出']
            for col in numeric_columns:
                if col in df.columns:
                    df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')

            # 处理文本列，确保不为None
            text_columns = df.select_dtypes(include=['object']).columns
            for col in text_columns:
                df.loc[:, col] = df[col].astype(str).replace('nan', '').replace('None', '')

            return df
        except Exception as e:
            logger.error(f"数据类型转换失败: {e}")
            raise

    def ensure_view_for_town_village(self):
        """在SQLite中创建或替换 v_town_village_list 视图，使其直接基于 调查点村名单"""
        cur = self.sqlite_conn.cursor()
        # 删除旧视图（如果存在）
        try:
            cur.execute("DROP VIEW IF EXISTS v_town_village_list")
        except Exception:
            pass
        # 创建新视图
        cur.execute(
            """
            CREATE VIEW v_town_village_list AS
            SELECT DISTINCT 户代码前12位 AS 村代码, 所在乡镇街道, 村居名称
            FROM 调查点村名单
            WHERE 所在乡镇街道 IS NOT NULL AND 村居名称 IS NOT NULL
            """
        )
        self.sqlite_conn.commit()
        logger.info("视图 v_town_village_list 已创建/更新")

    def enrich_household_town_village(self):
        """根据 v_town_village_list_base 用户代码前12位回填 户名单 的 所在乡镇街道/村居名称 字段"""
        try:
            cur = self.sqlite_conn.cursor()
            # 使用一个 UPDATE...FROM 风格的兼容写法（SQLite 用子查询）回填乡镇
            cur.execute(
                """
                UPDATE 调查点户名单
                SET 所在乡镇街道 = (
                    SELECT v.所在乡镇街道 FROM v_town_village_list_base v
                    WHERE v.村代码 = SUBSTR(户代码, 1, 12)
                    LIMIT 1
                ),
                村居名称 = (
                    SELECT v.村居名称 FROM v_town_village_list_base v
                    WHERE v.村代码 = SUBSTR(户代码, 1, 12)
                    LIMIT 1
                )
                WHERE (所在乡镇街道 IS NULL OR TRIM(IFNULL(所在乡镇街道,''))='')
                   OR (村居名称 IS NULL OR TRIM(IFNULL(村居名称,''))='')
                """
            )
            self.sqlite_conn.commit()
            # 统计非空条数作为回填效果
            cur2 = self.sqlite_conn.cursor()
            non_null = cur2.execute(
                "SELECT COUNT(*) FROM 调查点户名单 WHERE TRIM(IFNULL(所在乡镇街道,''))<>'' AND TRIM(IFNULL(村居名称,''))<>''"
            ).fetchone()[0]
            return non_null
        except Exception as e:
            self.sqlite_conn.rollback()
            raise
    def rebuild_v_town_village_list_base(self):
        """从MSSQL的调查点村名单重建SQLite基础表 v_town_village_list_base"""
        # SQLite 侧基础表名
        base_table = 'v_town_village_list_base'
        try:
            logger.info("开始重建 v_town_village_list_base 基础表数据")
            # 读取 MSSQL 源数据（字段名参考需求：户代码前12位、所在乡镇街道、村居名称）
            with self.mssql_pool.get_cursor() as cursor:
                cursor.execute("SELECT DISTINCT [户代码前12位] AS 村代码, [所在乡镇街道], [村居名称] FROM [dbo].[调查点村名单] WHERE [所在乡镇街道] IS NOT NULL AND [村居名称] IS NOT NULL")
                rows = cursor.fetchall()
            if not rows:
                logger.warning("MSSQL 源表 调查点村名单 无数据，跳过重建")
                return 0
            # 清空并重建 SQLite 基础表
            cur = self.sqlite_conn.cursor()
            cur.execute(f"DELETE FROM {base_table}")
            # 批量插入
            insert_sql = f"INSERT INTO {base_table} (所在乡镇街道, 村居名称, 村代码) VALUES (?, ?, ?)"
            data = [(r[1], r[2], r[0]) for r in rows]
            cur.executemany(insert_sql, data)
            self.sqlite_conn.commit()
            logger.info(f"v_town_village_list_base 重建完成，写入 {len(data)} 条")

            # 基础表建立完毕后，回填户名单的乡镇/村字段
            try:
                enriched = self.enrich_household_town_village()
                logger.info(f"户名单乡镇/村字段回填完成，非空记录数：{enriched}")
            except Exception as e:
                logger.error(f"回填户名单乡镇/村字段失败: {e}")
            return len(data)
            data = [(r[1], r[2], r[0]) for r in rows]
            cur.executemany(insert_sql, data)
            self.sqlite_conn.commit()
            logger.info(f"v_town_village_list_base 重建完成，写入 {len(data)} 条")
            return len(data)
        except Exception as e:
            logger.error(f"重建 v_town_village_list_base 失败: {e}")
            self.sqlite_conn.rollback()
            raise
            raise

    def migrate_table(self, table_name):
        """迁移单个表"""
        try:
            logger.info(f"开始迁移表: {table_name}")

            # 从MSSQL读取数据
            df = self.get_mssql_table_data(table_name)
            if df is None or df.empty:
                logger.warning(f"表 {table_name} 无数据或读取失败，跳过迁移")
                return 0

            # 清空SQLite表
            self.clear_sqlite_table(table_name)

            # 插入数据到SQLite
            count = self.insert_sqlite_data(table_name, df)

            logger.info(f"表 {table_name} 迁移完成，共迁移 {count} 条记录")
            return count

        except Exception as e:
            logger.error(f"迁移表 {table_name} 失败: {e}")
            raise

    def migrate_all_tables(self):
        """迁移所有表，并重建SQLite侧视图所需基础数据"""
        try:
            logger.info("开始数据迁移...")
            total_records = 0

            # 先迁移核心业务表
            for table_name in self.tables_to_migrate:
                try:
                    count = self.migrate_table(table_name)
                    total_records += count
                except Exception as e:
                    logger.error(f"迁移表 {table_name} 失败: {e}")
                    # 继续迁移其他表
                    continue

            # 迁移完成后不再重建基础表，视图直接基于 调查点村名单
            try:
                self.ensure_view_for_town_village()
            except Exception as e:
                logger.error(f"创建/校验视图 v_town_village_list 失败: {e}")
                # 不终止整体迁移

            logger.info(f"数据迁移完成！总共迁移了 {total_records} 条记录")
            return total_records

        except Exception as e:
            logger.error(f"数据迁移失败: {e}")
            raise

    def close_connections(self):
        """关闭数据库连接"""
        try:
            if self.sqlite_conn:
                self.sqlite_conn.close()
                logger.info("SQLite连接已关闭")
        except Exception as e:
            logger.error(f"关闭连接失败: {e}")

def main():
    """主函数"""
    migrator = DataMigrator()

    try:
        # 检查配置文件
        if not os.path.exists('config/mssql.json'):
            logger.error("MSSQL配置文件不存在: config/mssql.json")
            return False

        # 检查SQLite数据库
        if not os.path.exists('database.db'):
            logger.error("SQLite数据库不存在: database.db")
            logger.info("请先运行 create_sqlite_database.py 创建数据库")
            return False

        # 连接数据库
        if not migrator.connect_databases():
            return False

        # 执行迁移
        total_records = migrator.migrate_all_tables()

        logger.info(f"数据迁移成功完成！总共迁移了 {total_records} 条记录")
        return True

    except Exception as e:
        logger.error(f"数据迁移过程中发生错误: {e}")
        return False

    finally:
        migrator.close_connections()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
