import pyodbc
import pandas as pd
import logging
import gc
from .database_pool import get_connection_pool, get_external_connection_pool

class Database:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化连接池
        self.pool = get_connection_pool()
        self.external_pool = get_external_connection_pool()

    def execute_query(self, query, params=None):
        """
        使用连接池执行SQL查询。

        Args:
            query (str): 要执行的SQL查询语句。
            params (tuple, optional): 查询参数. Defaults to None.

        Returns:
            pyodbc.Cursor: 执行查询后的游标对象。
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

    def execute_external_query_safe(self, query, params=None):
        """
        在外部数据库上安全地执行一个SELECT查询并立即返回所有结果。
        """
        try:
            with self.external_pool.get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                result = cursor.fetchall()
                
                if len(result) > 1000:
                    gc.collect()
                    
                return result
        except Exception as e:
            self.logger.error(f"外部数据库安全查询失败: {query[:100]}... - {e}")
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

    # ====== 同步相关的兼容方法（为 database_sync 蓝图提供） ======
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

    def sync_external_db(self) -> bool:
        """
        从外部数据库读取台账数据并写入内部临时表[外部待导入]。
        仅同步 wton <> '1' 的记录（wton='1' 视为已同步，跳过）。
        注意：后续插入主表的去重由调用方负责。
        """
        self.logger.info("开始从外部数据库同步到内部表[外部待导入]")
        columns = [
            'hudm','code','amount','money','note','person','year','month',
            'z_guid','date','type','id','type_name','unit_name','ybm','ybz','wton','ntow'
        ]
        base_select = f"SELECT {', '.join(columns)} FROM {{table}} WHERE (wton IS NULL OR wton <> '1')"
        select_sql_candidates = [
            base_select.format(table='调查点台账合并'),
            base_select.format(table='外部待导入')
        ]
        rows = None
        last_error = None
        for sql in select_sql_candidates:
            try:
                rows = self.execute_external_query_safe(sql)
                self.logger.info(f"外部数据源读取成功: {sql.split('FROM')[-1].strip()}")
                break
            except Exception as e:
                last_error = e
                self.logger.warning(f"外部数据源读取失败，尝试下一个候选: {e}")
                continue
        if rows is None:
            raise RuntimeError(f"无法从任何外部数据源读取到台账数据: {last_error}")

        df = pd.DataFrame([tuple(r) for r in rows], columns=columns)
        self.logger.info(f"外部数据读取完成，共 {len(df)} 行（wton <> '1'）。准备写入内部临时表[外部待导入]")

        self.import_data(df, '外部待导入')
        self.logger.info("已将外部数据写入[外部待导入]")
        return True

    def mark_external_records_synced(self, ids):
        """将外部数据库中指定 id 的记录标志为已同步（wton='1'）"""
        if not ids:
            return 0
        updated_total = 0
        chunk_size = 1000
        try:
            with self.external_pool.get_cursor() as cursor:
                for i in range(0, len(ids), chunk_size):
                    chunk = ids[i:i+chunk_size]
                    placeholders = ','.join(['?'] * len(chunk))
                    sql = f"UPDATE 调查点台账合并 SET wton = '1' WHERE id IN ({placeholders})"
                    cursor.execute(sql, tuple(chunk))
                    updated_total += cursor.rowcount if cursor.rowcount != -1 else len(chunk)
            self.logger.info(f"外部库 wton 标记完成，共更新 {updated_total} 条")
            return updated_total
        except Exception as e:
            self.logger.error(f"标记外部记录 wton=1 失败: {e}")
            raise

    # ====== 双数据库连接管理方法 ======
    def connect_both_databases(self):
        """同时连接内部和外部数据库"""
        try:
            # 内部数据库连接通过连接池自动管理
            # 外部数据库连接也通过连接池自动管理
            self.logger.info("双数据库连接建立成功（使用连接池）")
            return True
        except Exception as e:
            self.logger.error(f"双数据库连接失败: {str(e)}")
            raise

    def close_both_databases(self):
        """关闭所有数据库连接"""
        try:
            # 连接池会自动管理连接的关闭
            self.logger.info("所有数据库连接已关闭（连接池管理）")
        except Exception as e:
            self.logger.error(f"关闭数据库连接失败: {str(e)}")

    def commit_both(self):
        """提交两个数据库的事务"""
        try:
            # 连接池的上下文管理器会自动处理事务提交
            self.logger.debug("双数据库事务提交成功（连接池自动管理）")
        except Exception as e:
            self.logger.error(f"双数据库事务提交失败: {str(e)}")
            raise

    def rollback_both(self):
        """回滚两个数据库的事务"""
        try:
            # 连接池的上下文管理器会自动处理事务回滚
            self.logger.info("双数据库事务回滚成功（连接池自动管理）")
        except Exception as e:
            self.logger.error(f"双数据库事务回滚失败: {str(e)}")

    def sync_external_to_internal(self):
        """外部到内部数据同步（插入新记录）"""
        insert_count = 0
        inserted_ids = []

        try:
            self.logger.info("开始外部到内部数据同步")

            # 查找外部数据库中存在但内部数据库中不存在的记录
            self.logger.info("查找需要同步的新记录...")

            # 先获取内部数据库的所有ID
            try:
                internal_ids_rows = self.execute_query_safe("SELECT id FROM 调查点台账合并 WHERE id IS NOT NULL")
                internal_ids = [row[0] for row in internal_ids_rows]
                self.logger.info(f"内部数据库现有记录数: {len(internal_ids)}")
            except Exception as e:
                self.logger.error(f"获取内部数据库ID列表失败: {str(e)}")
                raise

            # 获取外部数据库的新记录（仅获取未同步的记录）
            try:
                external_result = self.execute_external_query_safe("""
                    SELECT * FROM 调查点台账合并
                    WHERE (wton <> '1' OR wton IS NULL)
                """)
                self.logger.info(f"从外部数据库获取到 {len(external_result)} 条未同步记录")

                # 过滤掉内部数据库中已存在的记录
                if internal_ids and external_result:
                    filtered_records = []
                    for record in external_result:
                        # 假设id在第12列（索引11）
                        try:
                            record_id = record[11] if len(record) > 11 else None
                            if record_id and record_id not in internal_ids:
                                filtered_records.append(record)
                        except (IndexError, TypeError):
                            self.logger.warning("记录格式异常，跳过此记录")
                            continue
                    new_records = filtered_records
                    self.logger.info(f"过滤后需要同步的新记录数: {len(new_records)}")
                else:
                    new_records = external_result
                    self.logger.info(f"找到 {len(new_records)} 条需要同步的新记录")

            except Exception as e:
                self.logger.error(f"获取外部数据库新记录失败: {str(e)}")
                raise

            if not new_records:
                self.logger.info("没有需要同步的新记录")
                return 0

            # 获取外部数据的列名（假设标准列结构）
            external_columns = [
                'hudm','code','amount','money','note','person','year','month',
                'z_guid','date','type','id','type_name','unit_name','ybm','ybz','wton','ntow'
            ]

            # 获取内部数据库"调查点台账合并"表的列结构
            internal_columns_result = self.execute_query_safe("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '调查点台账合并'
                ORDER BY ORDINAL_POSITION
            """)
            internal_columns = [row[0] for row in internal_columns_result]
            self.logger.info(f"内部数据库表列: {internal_columns}")

            # 构建列映射
            column_mapping = []
            mapped_columns = []
            for internal_col in internal_columns:
                if internal_col in external_columns:
                    column_mapping.append(external_columns.index(internal_col))
                    mapped_columns.append(internal_col)

            # 使用连接池批量插入新记录到内部数据库
            batch_size = 50

            with self.pool.get_cursor() as cursor:
                for i in range(0, len(new_records), batch_size):
                    batch_records = new_records[i:i+batch_size]
                    self.logger.info(f"处理批次 {i//batch_size + 1}/{(len(new_records)-1)//batch_size + 1}")

                    # 构建插入SQL - 只使用映射的列
                    column_names = ', '.join([f'[{col}]' for col in mapped_columns])
                    placeholders = ', '.join(['?' for _ in mapped_columns])
                    insert_sql = f"""
                        INSERT INTO 调查点台账合并 ({column_names})
                        VALUES ({placeholders})
                    """

                    # 批量插入
                    for record in batch_records:
                        try:
                            # 根据列映射构建数据
                            mapped_data = []
                            for mapping_index in column_mapping:
                                mapped_data.append(record[mapping_index])

                            # 设置wton字段为1（如果存在）
                            if 'wton' in mapped_columns:
                                wton_index = mapped_columns.index('wton')
                                mapped_data[wton_index] = '1'

                            cursor.execute(insert_sql, mapped_data)
                            insert_count += 1
                            # 假设id在第12列（索引11）
                            record_id = record[11] if len(record) > 11 else None
                            if record_id:
                                inserted_ids.append(record_id)

                        except Exception as e:
                            record_id = record[11] if len(record) > 11 else 'Unknown'
                            self.logger.warning(f"插入记录失败 (ID: {record_id}): {str(e)}")
                            continue

            # 更新外部数据库中对应记录的wton字段
            if inserted_ids:
                try:
                    self.mark_external_records_synced(inserted_ids)
                except Exception as e:
                    self.logger.error(f"更新外部数据库wton标记失败: {str(e)}")
                    # 不抛出异常，因为数据已经插入成功

            self.logger.info(f"外部到内部同步完成，成功插入 {insert_count} 条记录")
            return insert_count

        except Exception as e:
            self.logger.error(f"外部到内部数据同步失败: {str(e)}")
            # 如果有部分记录插入成功，记录详情
            if insert_count > 0:
                self.logger.info(f"部分同步成功：已插入 {insert_count} 条记录")
            raise

    def get_sync_statistics(self):
        """获取同步统计信息"""
        try:
            # 内部数据库统计
            internal_stats = {}
            result = self.execute_query_safe("SELECT COUNT(*) FROM 调查点台账合并")
            internal_stats['total_records'] = result[0][0] if result else 0

            # 外部数据库统计
            external_stats = {}

            try:
                # 总记录数
                total_result = self.execute_external_query_safe("SELECT COUNT(*) FROM 调查点台账合并")
                external_stats['total_records'] = total_result[0][0] if total_result else 0

                # 待同步记录数（wton不等于'1'或为NULL的记录）
                pending_result = self.execute_external_query_safe("""
                    SELECT COUNT(*) FROM 调查点台账合并
                    WHERE (wton <> '1' OR wton IS NULL)
                """)
                external_stats['pending_to_internal'] = pending_result[0][0] if pending_result else 0

            except Exception as e:
                self.logger.warning(f"获取外部数据库统计失败: {str(e)}")
                external_stats['total_records'] = 0
                external_stats['pending_to_internal'] = 0

            return {
                'internal': internal_stats,
                'external': external_stats,
                'last_check_time': pd.Timestamp.now().strftime('%Y年%m月%d日 %H:%M:%S')
            }

        except Exception as e:
            self.logger.error(f"获取同步统计信息失败: {str(e)}")
            raise