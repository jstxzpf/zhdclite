import pandas as pd

class DataProcessor:
    def __init__(self, db):
        self.db = db

    def update_note(self, year, month):
        sql = "UPDATE 调查点台账合并 SET note=note+type_name COLLATE database_default,ybz='1' where year=? and month=? and ybz<>'1'"
        with self.db.pool.get_cursor() as cursor:
            cursor.execute(sql, (year, month))
            # 事务会在上下文管理器退出时自动提交

    def get_uncoded_data(self, year, month):
        sql = """
        SELECT 调查点户名单.户代码, 调查点户名单.户主姓名, 调查点台账合并.type_name, 调查点台账合并.amount AS 数量, 调查点台账合并.[date] as 日期,
               调查点台账合并.money AS 金额, 调查点台账合并.note AS 备注, 调查点台账合并.type AS 收支, 调查点台账合并.id, 调查点台账合并.code
        FROM 调查点台账合并 INNER JOIN 调查点户名单 ON 调查点台账合并.hudm = 调查点户名单.户代码
        WHERE 调查点台账合并.year = ? AND 调查点台账合并.month = ? AND 调查点台账合并.code IS NULL
            AND 调查点台账合并.type_name IS NOT NULL
            AND LTRIM(RTRIM(调查点台账合并.type_name)) COLLATE Chinese_PRC_CI_AS <> ''
        ORDER BY 调查点台账合并.type_name
        """
        # 使用连接池执行查询，避免连接冲突
        try:
            result = self.db.execute_query_safe(sql, (year, month))
            # 将结果转换为DataFrame
            columns = ['户代码', '户主姓名', 'type_name', '数量', '日期', '金额', '备注', '收支', 'id', 'code']
            return pd.DataFrame(result, columns=columns)
        except Exception as e:
            # 记录错误并重新抛出
            print(f"查询执行失败: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def get_all_uncoded_data(self):
        """获取所有未编码数据"""
        sql = """
        SELECT 调查点户名单.户代码, 调查点户名单.户主姓名, 调查点台账合并.type_name, 调查点台账合并.amount AS 数量, 调查点台账合并.[date] as 日期,
               调查点台账合并.money AS 金额, 调查点台账合并.note AS 备注, 调查点台账合并.type AS 收支, 调查点台账合并.id, 调查点台账合并.code,
               调查点台账合并.year AS 年度, 调查点台账合并.month AS 月份
        FROM 调查点台账合并 INNER JOIN 调查点户名单 ON 调查点台账合并.hudm = 调查点户名单.户代码
        WHERE 调查点台账合并.code IS NULL
            AND 调查点台账合并.type_name IS NOT NULL
            AND LTRIM(RTRIM(调查点台账合并.type_name)) COLLATE Chinese_PRC_CI_AS <> ''
        ORDER BY 调查点台账合并.year, 调查点台账合并.month, 调查点台账合并.type_name
        """
        # 使用安全查询方法，避免连接冲突
        try:
            result = self.db.execute_query_safe(sql)

            # 调试：检查结果结构
            if result:
                print(f"查询结果行数: {len(result)}")
                print(f"第一行数据: {result[0] if result else 'None'}")
                print(f"第一行列数: {len(result[0]) if result else 0}")

            # 将结果转换为DataFrame
            columns = ['户代码', '户主姓名', 'type_name', '数量', '日期', '金额', '备注', '收支', 'id', 'code', '年度', '月份']

            # 检查列数是否匹配
            if result and len(result[0]) != len(columns):
                print(f"警告：查询结果列数({len(result[0])})与期望列数({len(columns)})不匹配")
                # 动态调整列名
                actual_columns = [f'col_{i}' for i in range(len(result[0]))]
                return pd.DataFrame(result, columns=actual_columns)

            # 使用list comprehension确保数据格式正确
            data_list = [list(row) for row in result] if result else []
            return pd.DataFrame(data_list, columns=columns)

        except Exception as e:
            print(f"查询执行失败: {str(e)}")
            import traceback
            traceback.print_exc()
            # 如果查询失败，返回空DataFrame
            columns = ['户代码', '户主姓名', 'type_name', '数量', '日期', '金额', '备注', '收支', 'id', 'code', '年度', '月份']
            return pd.DataFrame(columns=columns)

    def update_all_note(self):
        """更新所有记录的note字段"""
        sql = """
        UPDATE 调查点台账合并
        SET note = CASE
            WHEN type_name IS NOT NULL AND LTRIM(RTRIM(type_name)) COLLATE Chinese_PRC_CI_AS <> ''
            THEN type_name COLLATE Chinese_PRC_CI_AS
            ELSE note
        END
        WHERE type_name IS NOT NULL AND LTRIM(RTRIM(type_name)) COLLATE Chinese_PRC_CI_AS <> ''
        """
        with self.db.pool.get_cursor() as cursor:
            cursor.execute(sql)
            # 事务会在上下文管理器退出时自动提交



