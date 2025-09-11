"""
数据库查询服务类
提供通用的查询方法，减少重复代码
"""

import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class QueryService:
    """数据库查询服务类，提供统一的查询接口"""
    
    def __init__(self, database):
        self.db = database
        self.logger = logging.getLogger(__name__)
    
    def execute_with_result_mapping(
        self, 
        sql: str, 
        params: Optional[List] = None, 
        columns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        执行查询并将结果映射为字典列表
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            columns: 列名列表
            
        Returns:
            字典列表形式的查询结果
        """
        result = self.db.execute_query_safe(sql, params or [])
        if not result:
            return []
        
        if columns:
            return [dict(zip(columns, row)) for row in result]
        else:
            return [list(row) for row in result]
    
    def execute_single_value(
        self, 
        sql: str, 
        params: Optional[List] = None
    ) -> Any:
        """
        执行查询并返回单个值
        
        Args:
            sql: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果的第一行第一列的值，如果没有结果则返回None
        """
        result = self.db.execute_query_safe(sql, params or [])
        if result and result[0]:
            return result[0][0]
        return None
    
    def execute_statistics_query(
        self, 
        sql: str, 
        params: Optional[List] = None
    ) -> Dict[str, Any]:
        """
        执行统计查询，返回标准的统计结果格式
        
        Args:
            sql: SQL查询语句，应该包含统计字段
            params: 查询参数
            
        Returns:
            包含统计信息的字典
        """
        result = self.db.execute_query_safe(sql, params or [])
        if result and result[0]:
            data = result[0]
            return {
                'total_records': data[0] or 0,
                'total_households': data[1] or 0,
                'income_records': data[2] or 0,
                'expenditure_records': data[3] or 0,
                'total_income': float(data[4] or 0),
                'total_expenditure': float(data[5] or 0),
                'uncoded_records': data[6] or 0,
                'coded_records': data[7] or 0
            }
        return {
            'total_records': 0, 'total_households': 0, 'income_records': 0,
            'expenditure_records': 0, 'total_income': 0.0, 'total_expenditure': 0.0,
            'uncoded_records': 0, 'coded_records': 0
        }
    
    def get_household_statistics(
        self, 
        where_clause: str = "", 
        params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """
        获取分户统计数据
        
        Args:
            where_clause: WHERE子句
            params: 查询参数
            
        Returns:
            分户统计结果列表
        """
        sql = f"""
        SELECT
            t.hudm, h.`户主姓名`, t.year, t.month, COUNT(*),
            COUNT(CASE WHEN t.type = 1 THEN 1 END), COUNT(CASE WHEN t.type = 2 THEN 1 END),
            SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END),
            SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END),
            COUNT(CASE WHEN t.code IS NULL THEN 1 END), COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END)
        FROM `调查点台账合并` t LEFT JOIN `调查点户名单` h ON t.hudm = h.`户代码`
        {where_clause}
        GROUP BY t.hudm, h.`户主姓名`, t.year, t.month
        ORDER BY t.year, t.month, t.hudm
        """
        
        columns = ['户代码', '户主姓名', '年份', '月份', '记账笔数', '收入笔数', '支出笔数', '收入总额', '支出总额', '未编码笔数', '已编码笔数']
        data = self.execute_with_result_mapping(sql, params, columns)
        
        # 处理数值类型转换
        for row in data:
            row['收入总额'] = float(row.get('收入总额', 0) or 0)
            row['支出总额'] = float(row.get('支出总额', 0) or 0)
        
        return data
    
    def get_town_statistics_for_town_name(
        self,
        town_name: str,
        where_clause: str = "",
        params: Optional[List] = None
    ) -> Optional[Dict[str, Any]]:
        """
        使用优化后的查询获取乡镇统计数据
        优先使用缓存表，如果有筛选条件则使用实时查询
        """
        # 如果没有筛选条件，直接使用缓存表
        if not where_clause or where_clause.strip() == "":
            cache_sql = """
            SELECT 记账笔数, 户数, 收入笔数, 支出笔数, 收入总额, 支出总额, 未编码笔数, 已编码笔数
            FROM town_statistics_cache
            WHERE 乡镇名称 = ?
            """
            result = self.db.execute_query_safe(cache_sql, [town_name])
            if result and result[0]:
                row = result[0]
                return {
                    '记账笔数': row[0] or 0,
                    '户数': row[1] or 0,
                    '收入笔数': row[2] or 0,
                    '支出笔数': row[3] or 0,
                    '收入总额': float(row[4] or 0),
                    '支出总额': float(row[5] or 0),
                    '未编码笔数': row[6] or 0,
                    '已编码笔数': row[7] or 0
                }

        # 有筛选条件时使用优化后的实时查询
        # 使用JOIN代替子查询，提高性能
        sql = f"""
        SELECT
            COUNT(*), COUNT(DISTINCT t.hudm),
            COUNT(CASE WHEN t.type = 1 THEN 1 END), COUNT(CASE WHEN t.type = 2 THEN 1 END),
            SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END),
            SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END),
            COUNT(CASE WHEN t.code IS NULL THEN 1 END), COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END)
        FROM `调查点台账合并` t
        INNER JOIN `v_town_village_list` v ON SUBSTR(t.hudm, 1, 12) = v.村代码
        WHERE v.所在乡镇街道 = ? {' AND ' + where_clause if where_clause else ''}
        """

        query_params = [town_name] + (params or [])
        result = self.db.execute_query_safe(sql, query_params)

        if result and result[0][0] is not None:
            row = result[0]
            return {
                '记账笔数': row[0] or 0,
                '户数': row[1] or 0,
                '收入笔数': row[2] or 0,
                '支出笔数': row[3] or 0,
                '收入总额': float(row[4] or 0),
                '支出总额': float(row[5] or 0),
                '未编码笔数': row[6] or 0,
                '已编码笔数': row[7] or 0
            }
        return None

    def get_all_town_statistics(
        self,
        where_clause: str = "",
        params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """
        一次性获取所有乡镇统计数据，避免N+1查询问题
        """
        # 如果没有筛选条件，优先使用缓存表；若缓存表不存在或查询失败，则回退到实时查询
        if not where_clause or where_clause.strip() == "":
            try:
                cache_sql = """
                SELECT 乡镇名称, 记账笔数, 户数, 收入笔数, 支出笔数, 收入总额, 支出总额, 未编码笔数, 已编码笔数
                FROM town_statistics_cache
                ORDER BY 乡镇名称
                """
                result = self.db.execute_query_safe(cache_sql)
                if result:
                    return [
                        {
                            '乡镇名称': row[0],
                            '记账笔数': row[1] or 0,
                            '户数': row[2] or 0,
                            '收入笔数': row[3] or 0,
                            '支出笔数': row[4] or 0,
                            '收入总额': float(row[5] or 0),
                            '支出总额': float(row[6] or 0),
                            '未编码笔数': row[7] or 0,
                            '已编码笔数': row[8] or 0
                        }
                        for row in result
                    ]
            except Exception as e:
                # 缓存表缺失或查询失败时，记录日志并回退到实时查询
                self.logger.warning(f"读取town_statistics_cache失败，将使用实时查询: {e}")

        # 有筛选条件时使用优化后的实时查询
        sql = f"""
        SELECT
            v.所在乡镇街道 as 乡镇名称,
            COUNT(*) as 记账笔数,
            COUNT(DISTINCT t.hudm) as 户数,
            SUM(CASE WHEN t.id IS NOT NULL AND t.type = 1 THEN 1 ELSE 0 END) as 收入笔数,
            SUM(CASE WHEN t.id IS NOT NULL AND t.type = 2 THEN 1 ELSE 0 END) as 支出笔数,
            SUM(CASE WHEN t.id IS NOT NULL AND t.type = 1 THEN t.money ELSE 0 END) as 收入总额,
            SUM(CASE WHEN t.id IS NOT NULL AND t.type = 2 THEN t.money ELSE 0 END) as 支出总额,
            SUM(CASE WHEN t.id IS NOT NULL AND t.code IS NULL THEN 1 ELSE 0 END) as 未编码笔数,
            SUM(CASE WHEN t.id IS NOT NULL AND t.code IS NOT NULL THEN 1 ELSE 0 END) as 已编码笔数
        FROM `v_town_village_list` v
        LEFT JOIN `调查点台账合并` t ON SUBSTR(t.hudm, 1, 12) = v.村代码
        {where_clause}
        GROUP BY v.所在乡镇街道
        ORDER BY v.所在乡镇街道
        """

        result = self.db.execute_query_safe(sql, params or [])
        if result:
            return [
                {
                    '乡镇名称': row[0],
                    '记账笔数': row[1] or 0,
                    '户数': row[2] or 0,
                    '收入笔数': row[3] or 0,
                    '支出笔数': row[4] or 0,
                    '收入总额': float(row[5] or 0),
                    '支出总额': float(row[6] or 0),
                    '未编码笔数': row[7] or 0,
                    '已编码笔数': row[8] or 0
                }
                for row in result
            ]

        return []

    def refresh_statistics_cache(self):
        """刷新统计缓存表（若缓存表不存在则自动创建）"""
        try:
            self.logger.info("开始刷新统计缓存...")

            # 1) 确保缓存表存在
            create_town_cache_sql = """
            CREATE TABLE IF NOT EXISTS town_statistics_cache (
                乡镇名称 TEXT,
                记账笔数 INTEGER,
                户数 INTEGER,
                收入笔数 INTEGER,
                支出笔数 INTEGER,
                收入总额 REAL,
                支出总额 REAL,
                未编码笔数 INTEGER,
                已编码笔数 INTEGER,
                更新时间 TEXT
            )
            """
            self.db.execute_query_safe(create_town_cache_sql)

            create_month_cache_sql = """
            CREATE TABLE IF NOT EXISTS month_statistics_cache (
                年份 INTEGER,
                月份 INTEGER,
                记账笔数 INTEGER,
                户数 INTEGER,
                收入笔数 INTEGER,
                支出笔数 INTEGER,
                收入总额 REAL,
                支出总额 REAL,
                未编码笔数 INTEGER,
                已编码笔数 INTEGER,
                更新时间 TEXT
            )
            """
            self.db.execute_query_safe(create_month_cache_sql)

            # 2) 清空旧缓存
            self.db.execute_query_safe("DELETE FROM town_statistics_cache")

            town_refresh_sql = """
            INSERT INTO town_statistics_cache
            SELECT
                v.所在乡镇街道 as 乡镇名称,
                COUNT(t.id) as 记账笔数,
                COUNT(DISTINCT t.hudm) as 户数,
                SUM(CASE WHEN t.id IS NOT NULL AND t.type = 1 THEN 1 ELSE 0 END) as 收入笔数,
                SUM(CASE WHEN t.id IS NOT NULL AND t.type = 2 THEN 1 ELSE 0 END) as 支出笔数,
                SUM(CASE WHEN t.id IS NOT NULL AND t.type = 1 THEN t.money ELSE 0 END) as 收入总额,
                SUM(CASE WHEN t.id IS NOT NULL AND t.type = 2 THEN t.money ELSE 0 END) as 支出总额,
                SUM(CASE WHEN t.id IS NOT NULL AND t.code IS NULL THEN 1 ELSE 0 END) as 未编码笔数,
                SUM(CASE WHEN t.id IS NOT NULL AND t.code IS NOT NULL THEN 1 ELSE 0 END) as 已编码笔数,
                datetime('now') as 更新时间
            FROM v_town_village_list v
            LEFT JOIN 调查点台账合并 t ON SUBSTR(t.hudm, 1, 12) = v.村代码
            GROUP BY v.所在乡镇街道
            """
            self.db.execute_query_safe(town_refresh_sql)

            self.db.execute_query_safe("DELETE FROM month_statistics_cache")

            month_refresh_sql = """
            INSERT INTO month_statistics_cache
            SELECT
                t.year as 年份,
                t.month as 月份,
                COUNT(*) as 记账笔数,
                COUNT(DISTINCT t.hudm) as 户数,
                COUNT(CASE WHEN t.type = 1 THEN 1 END) as 收入笔数,
                COUNT(CASE WHEN t.type = 2 THEN 1 END) as 支出笔数,
                SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) as 收入总额,
                SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) as 支出总额,
                COUNT(CASE WHEN t.code IS NULL THEN 1 END) as 未编码笔数,
                COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) as 已编码笔数,
                datetime('now') as 更新时间
            FROM 调查点台账合并 t
            GROUP BY t.year, t.month
            """
            self.db.execute_query_safe(month_refresh_sql)

            self.logger.info("统计缓存刷新完成")
            return True

        except Exception as e:
            self.logger.error(f"刷新统计缓存失败: {e}")
            return False

    def get_consumption_structure(
        self, 
        where_clause: str = "", 
        params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """
        获取消费结构统计数据（仅统计支出 code 前两位在 31..38）。
        - 分组维度：code 前两位
        - 指标：笔数、总金额、平均金额、涉及户数
        - 类别名称：映射为中文
        """
        consumption_where = where_clause or ""
        extra = "(SUBSTR(t.code, 1, 2) IN ('31','32','33','34','35','36','37','38') AND t.type = 2 AND t.code IS NOT NULL)"
        if consumption_where.strip():
            consumption_where += f" AND {extra}"
        else:
            consumption_where = f"WHERE {extra}"

        sql = f"""
        SELECT
            SUBSTR(t.code, 1, 2) AS `编码`,
            CASE SUBSTR(t.code, 1, 2)
                WHEN '31' THEN '食品烟酒'
                WHEN '32' THEN '衣着'
                WHEN '33' THEN '居住'
                WHEN '34' THEN '生活用品及服务'
                WHEN '35' THEN '交通通信'
                WHEN '36' THEN '教育文化娱乐'
                WHEN '37' THEN '医疗保健'
                WHEN '38' THEN '其他用品及服务'
                ELSE '其他'
            END AS `消费类别`,
            COUNT(*) AS `记账笔数`,
            SUM(t.money) AS `总金额`,
            AVG(t.money) AS `平均金额`,
            COUNT(DISTINCT t.hudm) AS `涉及户数`
        FROM `调查点台账合并` t
        {consumption_where}
        GROUP BY SUBSTR(t.code, 1, 2)
        ORDER BY SUM(t.money) DESC
        """

        columns = ['编码', '消费类别', '记账笔数', '总金额', '平均金额', '涉及户数']
        data = self.execute_with_result_mapping(sql, params, columns)

        for row in data:
            row['总金额'] = float(row.get('总金额', 0) or 0)
            row['平均金额'] = float(row.get('平均金额', 0) or 0)

        return data
    
    def get_missing_days_statistics(
        self,
        year: str,
        month: str,
        days_in_month: int,
        where_clause: str = "",
        params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """
        获取漏记账天数统计

        Args:
            year: 年份
            month: 月份
            days_in_month: 该月天数
            where_clause: WHERE子句（针对调查点户名单表h的筛选条件）
            params: 查询参数

        Returns:
            漏记账统计结果列表
        """
        # 处理WHERE子句：where_clause是针对h表的，我们需要添加针对t表的年月筛选
        if where_clause and where_clause.strip().upper().startswith('WHERE'):
            # 移除WHERE关键字，只保留条件部分
            household_conditions = where_clause[5:].strip()
            full_where_clause = f"WHERE {household_conditions}"
        elif where_clause:
            full_where_clause = f"WHERE {where_clause}"
        else:
            full_where_clause = ""

        # 使用真实数据计算漏记账统计
        try:
            # 处理WHERE子句
            if where_clause and where_clause.strip().upper().startswith('WHERE'):
                household_conditions = where_clause[5:].strip()
                full_where_clause = f"WHERE {household_conditions}"
            elif where_clause:
                full_where_clause = f"WHERE {where_clause}"
            else:
                full_where_clause = ""

            # 先获取户名单
            households_sql = f"""
            SELECT h.`户代码`, h.`户主姓名`
            FROM `调查点户名单` h
            {full_where_clause}
            ORDER BY h.`户代码`
            LIMIT 50
            """

            households_result = self.db.execute_query_safe(households_sql, params or [])

            if not households_result:
                return []

            # 为每个户计算真实的漏记账统计
            data = []
            for household_row in households_result:
                household_code = household_row[0]
                household_name = household_row[1]

                # 查询该户在指定月份的记账统计
                # 修复：使用CONVERT(VARCHAR(10), t.date, 120)只计算日期部分，避免时间信息干扰
                stats_sql = """
                SELECT
                    COUNT(*) AS `总记账笔数`,
                    COUNT(DISTINCT DATE(t.date)) AS `实际记账天数`,
                    MIN(t.date) AS `首次记账日期`,
                    MAX(t.date) AS `最后记账日期`
                FROM `调查点台账合并` t
                WHERE t.hudm = ? AND t.year = ? AND t.month = ?
                """

                stats_params = [household_code, year, month]
                stats_result = self.db.execute_query_safe(stats_sql, stats_params)

                if stats_result and stats_result[0]:
                    stats_row = stats_result[0]
                    total_records = stats_row[0] or 0
                    actual_recording_days = stats_row[1] or 0
                    first_date = stats_row[2]
                    last_date = stats_row[3]
                else:
                    total_records = 0
                    actual_recording_days = 0
                    first_date = None
                    last_date = None

                # 对于单月份分析，漏记账天数 = 该月总天数 - 实际记账天数
                recording_days = actual_recording_days
                missing_days = days_in_month - actual_recording_days

                data.append({
                    '户代码': household_code,
                    '户主姓名': household_name,
                    '记账天数': recording_days,
                    '漏记账天数': missing_days,
                    '总记账笔数': total_records,
                    '首次记账日期': first_date,
                    '最后记账日期': last_date
                })

            self.logger.info(f"单月份漏记账分析完成，{year}年{month}月，共{len(data)}户")
            return data

        except Exception as e:
            self.logger.error(f"漏记账统计查询失败: {e}")
            return []

    def get_missing_days_statistics_range(
        self,
        start_year: str,
        start_month: str,
        end_year: str,
        end_month: str,
        where_clause: str = "",
        params: Optional[List] = None
    ) -> List[Dict[str, Any]]:
        """
        获取时间区间内的漏记账天数统计（简化版本，避免数据库转换错误）

        Args:
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            where_clause: WHERE子句（针对调查点户名单表h的筛选条件）
            params: 查询参数

        Returns:
            漏记账统计结果列表
        """
        try:
            # 计算时间区间内的总天数
            import calendar
            from datetime import datetime

            start_date = datetime(int(start_year), int(start_month), 1)
            end_month_days = calendar.monthrange(int(end_year), int(end_month))[1]
            end_date = datetime(int(end_year), int(end_month), end_month_days)
            total_days = (end_date - start_date).days + 1

            # 处理WHERE子句
            if where_clause and where_clause.strip().upper().startswith('WHERE'):
                household_conditions = where_clause[5:].strip()
                full_where_clause = f"WHERE {household_conditions}"
            elif where_clause:
                full_where_clause = f"WHERE {where_clause}"
            else:
                full_where_clause = ""

            # 获取符合条件的户名单
            households_sql = f"""
            SELECT TOP 50
                h.户代码,
                h.户主姓名
            FROM 调查点户名单 h
            {full_where_clause}
            ORDER BY h.户代码
            """

            households_result = self.db.execute_query_safe(households_sql, params or [])

            if not households_result:
                return []

            # 为每个户计算真实的漏记账统计
            data = []
            for household_row in households_result:
                household_code = household_row[0]
                household_name = household_row[1]

                # 查询该户在时间区间内的记账统计
                # 修复：使用CONVERT(VARCHAR(10), t.date, 120)只计算日期部分，避免时间信息干扰
                stats_sql = """
                SELECT
                    COUNT(*) AS 总记账笔数,
                    COUNT(DISTINCT CONVERT(VARCHAR(10), t.date, 120)) AS 实际记账天数,
                    MIN(t.date) AS 首次记账日期,
                    MAX(t.date) AS 最后记账日期
                FROM 调查点台账合并 t
                WHERE t.hudm = ?
                    AND ((t.year > ? OR (t.year = ? AND t.month >= ?))
                    AND (t.year < ? OR (t.year = ? AND t.month <= ?)))
                """

                stats_params = [
                    household_code,
                    start_year, start_year, start_month,
                    end_year, end_year, end_month
                ]

                stats_result = self.db.execute_query_safe(stats_sql, stats_params)

                if stats_result and stats_result[0]:
                    stats_row = stats_result[0]
                    total_records = stats_row[0] or 0
                    actual_recording_days = stats_row[1] or 0
                    first_date = stats_row[2]
                    last_date = stats_row[3]
                else:
                    total_records = 0
                    actual_recording_days = 0
                    first_date = None
                    last_date = None

                # 计算漏记账天数
                # 漏记账天数 = 时间范围总天数 - 实际记账天数
                recording_days = actual_recording_days
                missing_days = total_days - actual_recording_days

                # 确保漏记账天数不为负数（如果记账天数超过时间范围，说明有重复记账）
                if missing_days < 0:
                    missing_days = 0

                data.append({
                    '户代码': household_code,
                    '户主姓名': household_name,
                    '记账天数': recording_days,
                    '漏记账天数': missing_days,
                    '总记账笔数': total_records,
                    '首次记账日期': first_date,
                    '最后记账日期': last_date
                })

            self.logger.info(f"漏记账分析完成，时间区间：{start_year}-{start_month}至{end_year}-{end_month}，共{len(data)}户")
            return data

        except Exception as e:
            self.logger.error(f"漏记账统计查询失败: {e}")
            return []

    def get_all_towns(self) -> List[str]:
        """从v_town_village_list视图中获取所有唯一的乡镇列表"""
        sql = "SELECT DISTINCT 所在乡镇街道 FROM v_town_village_list WHERE 所在乡镇街道 IS NOT NULL AND 所在乡镇街道 != '' ORDER BY 所在乡镇街道"
        result = self.db.execute_query_safe(sql)
        return [row[0] for row in result] if result else []

    def get_villages_by_town(self, town_name: str) -> List[Dict[str, str]]:
        """根据乡镇名称从v_town_village_list视图中获取村庄列表，返回包含名称和代码的字典列表"""
        sql = """
        SELECT 村居名称, 村代码
        FROM v_town_village_list
        WHERE 所在乡镇街道 = ?
        AND 村居名称 IS NOT NULL
        AND 村居名称 != ''
        ORDER BY 村居名称
        """
        result = self.db.execute_query_safe(sql, [town_name])
        return [{'name': row[0], 'code': row[1]} for row in result if row[0] and row[1]]

    def get_towns_with_data(self, year: str, month: str) -> List[str]:
        """
        获取指定年月有台账记录的所有乡镇列表
        严格按照数据源规范：使用v_town_village_list视图，通过户代码前12位与村代码关联

        Args:
            year: 年份 (如 '2024')
            month: 月份 (如 '06')

        Returns:
            有记录的乡镇名称列表
        """
        try:
            # 使用v_town_village_list视图获取村代码到乡镇的映射
            mapping_sql = """
            SELECT DISTINCT 村代码, 所在乡镇街道
            FROM v_town_village_list
            WHERE 村代码 IS NOT NULL AND 所在乡镇街道 IS NOT NULL
            """
            mapping_result = self.db.execute_query_safe(mapping_sql)

            if not mapping_result:
                self.logger.warning("未找到乡镇村庄映射数据")
                return []

            # 构建村代码到乡镇名称的映射
            village_to_town = {}
            for row in mapping_result:
                village_code = row[0]
                town_name = row[1]
                if village_code and town_name:
                    village_to_town[village_code] = town_name

            # 查询有记录的村代码（使用户代码前12位）
            data_sql = """
            SELECT DISTINCT SUBSTR(t.hudm, 1, 12) as village_code
            FROM 调查点台账合并 t
            WHERE t.year = ? AND t.month = ?
                AND t.hudm IS NOT NULL
                AND LENGTH(t.hudm) >= 12
            """

            result = self.db.execute_query_safe(data_sql, [year, month])

            if not result:
                return []

            # 将村代码转换为乡镇名称
            towns_with_data = set()  # 使用set避免重复
            for row in result:
                village_code = row[0]
                if village_code in village_to_town:
                    town_name = village_to_town[village_code]
                    towns_with_data.add(town_name)

            towns_list = sorted(list(towns_with_data))
            self.logger.info(f"找到 {len(towns_list)} 个有记录的乡镇: {', '.join(towns_list)}")
            return towns_list

        except Exception as e:
            self.logger.error(f"获取有数据的乡镇列表失败: {e}")
            return []