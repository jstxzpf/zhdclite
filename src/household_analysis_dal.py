#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 分户审核分析数据访问层(DAL)
严格按照数据源规范：使用v_town_village_list视图作为权威数据源
"""

import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd


class HouseholdAnalysisDAL:
    """分户审核分析数据访问层"""
    
    def __init__(self, db):
        """
        初始化数据访问层
        
        Args:
            db: 数据库连接对象
        """
        self.db = db
        self.logger = logging.getLogger(__name__)
    
    def get_household_basic_info(self, household_code: str) -> Optional[Dict]:
        """
        获取户基础信息
        
        Args:
            household_code: 户代码
            
        Returns:
            户基础信息字典，包含户主姓名、人数、村庄信息等
        """
        try:
            # 首先尝试从调查点户名单获取基础信息
            household_sql = """
            SELECT 户代码, 户主姓名, 人数
            FROM 调查点户名单
            WHERE 户代码 = ?
            """
            household_result = self.db.execute_query_safe(household_sql, [household_code])

            if household_result:
                # 找到了户名单信息
                household_info = household_result[0]
                household_head = household_info[1]
                family_size = household_info[2] or 0
            else:
                # 户名单中没有找到，尝试从台账合并表中获取信息
                self.logger.info(f"户名单中未找到户代码 {household_code}，尝试从台账表获取信息")

                # 检查台账表中是否有该户代码的数据
                ledger_check_sql = """
                SELECT COUNT(*) FROM 调查点台账合并 WHERE hudm = ?
                """
                ledger_check_result = self.db.execute_query_safe(ledger_check_sql, [household_code])

                if not ledger_check_result or ledger_check_result[0][0] == 0:
                    self.logger.warning(f"台账表中也未找到户代码 {household_code} 的数据")
                    return None

                # 从台账表构造基本信息
                household_head = f"户主_{household_code[-3:]}"  # 使用户代码后3位作为标识
                family_size = 1  # 默认家庭人口为1
            
            # 通过户代码前12位获取村庄信息（严格使用v_town_village_list视图）
            village_code = household_code[:12] if len(household_code) >= 12 else None
            village_info = None
            
            if village_code:
                village_sql = """
                SELECT 村代码, 村居名称, 所在乡镇街道
                FROM v_town_village_list
                WHERE 村代码 = ?
                """
                village_result = self.db.execute_query_safe(village_sql, [village_code])
                if village_result:
                    village_info = village_result[0]
            
            return {
                '户代码': household_code,
                '户主姓名': household_head,
                '人数': family_size,
                '家庭人口': family_size,  # 添加家庭人口字段作为人数的别名
                '村代码': village_info[0] if village_info else village_code,
                '村居名称': village_info[1] if village_info else None,
                '所在乡镇街道': village_info[2] if village_info else None
            }
            
        except Exception as e:
            self.logger.error(f"获取户基础信息失败: {household_code}, 错误: {e}")
            return None
    
    def get_household_income_expense_data(self, household_code: str, 
                                        start_year: str = None, start_month: str = None,
                                        end_year: str = None, end_month: str = None) -> List[Dict]:
        """
        获取户收支明细数据
        
        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            收支明细数据列表
        """
        try:
            # 构建时间筛选条件
            time_conditions = []
            params = [household_code]
            
            if start_year and start_month:
                time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
                params.extend([start_year, start_year, start_month])
            
            if end_year and end_month:
                time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
                params.extend([end_year, end_year, end_month])
            
            time_clause = " AND " + " AND ".join(time_conditions) if time_conditions else ""
            
            sql = f"""
            SELECT 
                t.id,
                t.hudm AS 户代码,
                t.year AS 年份,
                t.month AS 月份,
                t.date AS 日期,
                t.type AS 收支类型,
                t.code AS 编码,
                t.type_name AS 项目名称,
                t.amount AS 数量,
                t.money AS 金额,
                t.note AS 备注,
                t.unit_name AS 单位名称,
                c.帐目指标名称,
                c.收支类别,
                c.单位名称 AS 标准单位名称
            FROM 调查点台账合并 t
            LEFT JOIN 调查品种编码 c ON t.code = c.帐目编码
            WHERE t.hudm = ? {time_clause}
            ORDER BY t.year, t.month, t.date, t.id
            """
            
            result = self.db.execute_query_safe(sql, params)
            
            if not result:
                return []
            
            # 转换为字典列表
            columns = ['id', '户代码', '年份', '月份', '日期', '收支类型', '编码', '项目名称', 
                      '数量', '金额', '备注', '单位名称', '帐目指标名称', '收支类别', '标准单位名称']
            
            data_list = []
            for row in result:
                record = dict(zip(columns, row))
                # 数据类型转换
                record['金额'] = float(record['金额']) if record['金额'] is not None else 0.0
                record['数量'] = float(record['数量']) if record['数量'] is not None else 0.0
                record['收支类型'] = int(record['收支类型']) if record['收支类型'] is not None else 0
                data_list.append(record)
            
            return data_list
            
        except Exception as e:
            self.logger.error(f"获取户收支数据失败: {household_code}, 错误: {e}")
            return []
    
    def get_category_mapping(self) -> Dict[str, Dict]:
        """
        获取收支分类映射信息
        
        Returns:
            编码到分类信息的映射字典
        """
        try:
            sql = """
            SELECT 帐目编码, 帐目指标名称, 收支类别, 单位名称
            FROM 调查品种编码
            WHERE 帐目编码 IS NOT NULL
            ORDER BY 帐目编码
            """
            
            result = self.db.execute_query_safe(sql)
            
            mapping = {}
            for row in result:
                code = row[0]
                mapping[code] = {
                    '帐目指标名称': row[1],
                    '收支类别': row[2],
                    '单位名称': row[3],
                    '编码前两位': code[:2] if code and len(code) >= 2 else '',
                    '主要分类': self._get_main_category(code)
                }
            
            return mapping
            
        except Exception as e:
            self.logger.error(f"获取分类映射失败: {e}")
            return {}
    
    def _get_main_category(self, code: str) -> str:
        """
        根据编码前两位获取主要分类
        
        Args:
            code: 编码
            
        Returns:
            主要分类名称
        """
        if not code or len(code) < 2:
            return '未知'
        
        prefix = code[:2]
        
        # 收入类型分类
        income_categories = {
            '21': '工资性收入',
            '22': '经营净收入',
            '23': '财产净收入',
            '24': '转移净收入',
            '12': '出售农产品及提供农业服务',
            '25': '非收入所得',
            '26': '借贷性所得',
            '42': '从政府得到的实物和服务'
        }
        
        # 支出类型分类
        expense_categories = {
            '31': '食品烟酒',
            '32': '衣着',
            '33': '居住',
            '34': '生活用品及服务',
            '35': '交通通信',
            '36': '教育文化娱乐',
            '37': '医疗保健',
            '38': '其他用品及服务',
            '41': '从单位或雇主得到的实物和服务',
            '43': '从社会得到的实物和服务',
            '51': '非农业生产经营费用',
            '52': '财产性支出',
            '53': '转移性支出',
            '13': '购买农业生产资料和农业服务成本支持',
            '14': '购建农业生产性固定资产'
        }
        
        return income_categories.get(prefix) or expense_categories.get(prefix, '其他')
    
    def get_households_by_area(self, town_name: str = None, village_name: str = None) -> List[str]:
        """
        根据地区获取户代码列表
        
        Args:
            town_name: 乡镇名称
            village_name: 村庄名称
            
        Returns:
            户代码列表
        """
        try:
            # 首先从v_town_village_list视图获取符合条件的村代码
            village_conditions = []
            params = []
            
            if town_name:
                village_conditions.append("所在乡镇街道 = ?")
                params.append(town_name)
            
            if village_name:
                village_conditions.append("村居名称 = ?")
                params.append(village_name)
            
            if village_conditions:
                village_where = " WHERE " + " AND ".join(village_conditions)
            else:
                village_where = ""
            
            village_sql = f"""
            SELECT DISTINCT 村代码
            FROM v_town_village_list
            {village_where}
            """
            
            village_result = self.db.execute_query_safe(village_sql, params)
            
            if not village_result:
                return []
            
            village_codes = [row[0] for row in village_result]
            
            # 根据村代码获取户代码（户代码前12位匹配村代码）
            if village_codes:
                placeholders = ','.join(['?' for _ in village_codes])
                household_sql = f"""
                SELECT DISTINCT 户代码
                FROM 调查点户名单
                WHERE SUBSTR(户代码, 1, 12) IN ({placeholders})
                ORDER BY 户代码
                """

                household_result = self.db.execute_query_safe(household_sql, village_codes)
                return [row[0] for row in household_result] if household_result else []
            
            return []
            
        except Exception as e:
            self.logger.error(f"根据地区获取户代码失败: {e}")
            return []

    def get_household_monthly_summary(self, household_code: str,
                                    start_year: str = None, start_month: str = None,
                                    end_year: str = None, end_month: str = None) -> List[Dict]:
        """
        获取户月度收支汇总数据

        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            月度汇总数据列表
        """
        try:
            # 构建时间筛选条件
            time_conditions = []
            params = [household_code]

            if start_year and start_month:
                time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
                params.extend([start_year, start_year, start_month])

            if end_year and end_month:
                time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
                params.extend([end_year, end_year, end_month])

            time_clause = " AND " + " AND ".join(time_conditions) if time_conditions else ""

            sql = f"""
            SELECT
                t.year AS 年份,
                t.month AS 月份,
                COUNT(*) AS 总记账笔数,
                COUNT(CASE WHEN t.type = 1 THEN 1 END) AS 收入笔数,
                COUNT(CASE WHEN t.type = 2 THEN 1 END) AS 支出笔数,
                SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) AS 收入总额,
                SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) AS 支出总额,
                COUNT(CASE WHEN t.code IS NULL THEN 1 END) AS 未编码笔数,
                COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) AS 已编码笔数,
                COUNT(DISTINCT t.date) AS 记账天数,
                MIN(t.date) AS 首次记账日期,
                MAX(t.date) AS 最后记账日期
            FROM 调查点台账合并 t
            WHERE t.hudm = ? {time_clause}
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
            """

            result = self.db.execute_query_safe(sql, params)

            if not result:
                return []

            # 转换为字典列表
            columns = ['年份', '月份', '总记账笔数', '收入笔数', '支出笔数', '收入总额', '支出总额',
                      '未编码笔数', '已编码笔数', '记账天数', '首次记账日期', '最后记账日期']

            data_list = []
            for row in result:
                record = dict(zip(columns, row))
                # 数据类型转换
                record['收入总额'] = float(record['收入总额']) if record['收入总额'] is not None else 0.0
                record['支出总额'] = float(record['支出总额']) if record['支出总额'] is not None else 0.0
                record['收支差额'] = record['收入总额'] - record['支出总额']
                data_list.append(record)

            return data_list

        except Exception as e:
            self.logger.error(f"获取户月度汇总数据失败: {household_code}, 错误: {e}")
            return []

    def get_household_category_summary(self, household_code: str,
                                     start_year: str = None, start_month: str = None,
                                     end_year: str = None, end_month: str = None) -> List[Dict]:
        """
        获取户分类汇总数据

        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            分类汇总数据列表
        """
        try:
            # 构建时间筛选条件
            time_conditions = []
            params = [household_code]

            if start_year and start_month:
                time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
                params.extend([start_year, start_year, start_month])

            if end_year and end_month:
                time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
                params.extend([end_year, end_year, end_month])

            time_clause = " AND " + " AND ".join(time_conditions) if time_conditions else ""

            sql = f"""
            SELECT
                SUBSTR(t.code, 1, 2) AS 编码前缀,
                t.type AS 收支类型,
                COUNT(*) AS 记账笔数,
                SUM(t.money) AS 总金额,
                AVG(t.money) AS 平均金额,
                MIN(t.money) AS 最小金额,
                MAX(t.money) AS 最大金额,
                COUNT(DISTINCT (t.year || '-' || t.month)) AS 涉及月份数
            FROM 调查点台账合并 t
            WHERE t.hudm = ? AND t.code IS NOT NULL {time_clause}
            GROUP BY SUBSTR(t.code, 1, 2), t.type
            ORDER BY t.type, SUM(t.money) DESC
            """

            result = self.db.execute_query_safe(sql, params)

            if not result:
                return []

            # 转换为字典列表
            columns = ['编码前缀', '收支类型', '记账笔数', '总金额', '平均金额', '最小金额', '最大金额', '涉及月份数']

            data_list = []
            for row in result:
                record = dict(zip(columns, row))
                # 数据类型转换
                record['总金额'] = float(record['总金额']) if record['总金额'] is not None else 0.0
                record['平均金额'] = float(record['平均金额']) if record['平均金额'] is not None else 0.0
                record['最小金额'] = float(record['最小金额']) if record['最小金额'] is not None else 0.0
                record['最大金额'] = float(record['最大金额']) if record['最大金额'] is not None else 0.0
                record['主要分类'] = self._get_main_category(record['编码前缀'] + '0000')
                data_list.append(record)

            return data_list

        except Exception as e:
            self.logger.error(f"获取户分类汇总数据失败: {household_code}, 错误: {e}")
            return []

    def get_statistical_benchmarks(self, area_type: str = 'all', area_value: str = None) -> Dict:
        """
        获取统计基准数据（用于异常检测）

        Args:
            area_type: 区域类型 ('all', 'town', 'village')
            area_value: 区域值（乡镇名或村庄名）

        Returns:
            统计基准数据字典
        """
        try:
            # 构建区域筛选条件
            area_conditions = []
            params = []

            if area_type == 'town' and area_value:
                # 通过v_town_village_list视图获取乡镇下的村代码
                village_sql = """
                SELECT DISTINCT 村代码
                FROM v_town_village_list
                WHERE 所在乡镇街道 = ?
                """
                village_result = self.db.execute_query_safe(village_sql, [area_value])
                if village_result:
                    village_codes = [row[0] for row in village_result]
                    placeholders = ','.join(['?' for _ in village_codes])
                    area_conditions.append(f"SUBSTR(t.hudm, 1, 12) IN ({placeholders})")
                    params.extend(village_codes)
            elif area_type == 'village' and area_value:
                # 通过v_town_village_list视图获取村代码
                village_sql = """
                SELECT DISTINCT 村代码
                FROM v_town_village_list
                WHERE 村居名称 = ?
                """
                village_result = self.db.execute_query_safe(village_sql, [area_value])
                if village_result:
                    village_code = village_result[0][0]
                    area_conditions.append("SUBSTR(t.hudm, 1, 12) = ?")
                    params.append(village_code)

            area_clause = " AND " + " AND ".join(area_conditions) if area_conditions else ""

            # 获取金额统计基准（简化版本，移除百分位数计算）
            amount_sql = f"""
            SELECT
                SUBSTR(t.code, 1, 2) AS 编码前缀,
                t.type AS 收支类型,
                COUNT(*) AS 记录数,
                AVG(t.money) AS 平均金额,
                -- SQLite 无 STDEV，临时以0替代或后续在应用层计算
                0 AS 标准差,
                MIN(t.money) AS 最小金额,
                MAX(t.money) AS 最大金额
            FROM 调查点台账合并 t
            WHERE t.code IS NOT NULL AND t.money > 0 {area_clause}
            GROUP BY SUBSTR(t.code, 1, 2), t.type
            HAVING COUNT(*) >= 10
            """

            amount_result = self.db.execute_query_safe(amount_sql, params)

            benchmarks = {}
            if amount_result:
                for row in amount_result:
                    key = f"{row[0]}_{row[1]}"  # 编码前缀_收支类型
                    benchmarks[key] = {
                        '编码前缀': row[0],
                        '收支类型': row[1],
                        '记录数': row[2],
                        '平均金额': float(row[3]) if row[3] is not None else 0.0,
                        '标准差': float(row[4]) if row[4] is not None else 0.0,
                        '最小金额': float(row[5]) if row[5] is not None else 0.0,
                        '最大金额': float(row[6]) if row[6] is not None else 0.0
                    }

            return benchmarks

        except Exception as e:
            self.logger.error(f"获取统计基准数据失败: {e}")
            return {}

    def get_household_recording_patterns(self, household_code: str,
                                       start_year: str = None, start_month: str = None,
                                       end_year: str = None, end_month: str = None) -> Dict:
        """
        获取户记账模式数据（用于记账质量评估）

        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            记账模式数据字典
        """
        try:
            # 构建时间筛选条件
            time_conditions = []
            params = [household_code]

            if start_year and start_month:
                time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
                params.extend([start_year, start_year, start_month])

            if end_year and end_month:
                time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
                params.extend([end_year, end_year, end_month])

            time_clause = " AND " + " AND ".join(time_conditions) if time_conditions else ""

            # 获取记账模式统计
            pattern_sql = f"""
            SELECT
                COUNT(*) AS 总记录数,
                COUNT(DISTINCT (t.year || '-' || t.month || '-' || t.date)) AS 记账天数,
                COUNT(CASE WHEN t.money = CAST(t.money AS INTEGER) THEN 1 END) AS 整数金额数,
                COUNT(CASE WHEN t.note IS NOT NULL AND LENGTH(TRIM(t.note)) > 0 THEN 1 END) AS 有备注数,
                COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) AS 已编码数,
                AVG(CAST(STRFTIME('%d', t.date) AS FLOAT)) AS 平均记账日期,
                COUNT(CASE WHEN CAST(STRFTIME('%d', t.date) AS INTEGER) >= 25 THEN 1 END) AS 月末记账数,
                COUNT(DISTINCT t.type_name) AS 项目名称种类数,
                MIN(t.date) AS 最早记账日期,
                MAX(t.date) AS 最晚记账日期
            FROM 调查点台账合并 t
            WHERE t.hudm = ? {time_clause}
            """

            pattern_result = self.db.execute_query_safe(pattern_sql, params)

            if not pattern_result or not pattern_result[0]:
                return {}

            row = pattern_result[0]

            # 获取月度记账分布
            monthly_sql = f"""
            SELECT
                t.year,
                t.month,
                COUNT(*) AS 月记录数,
                COUNT(DISTINCT t.date) AS 月记账天数,
                MIN(t.date) AS 月首次记账,
                MAX(t.date) AS 月最后记账
            FROM 调查点台账合并 t
            WHERE t.hudm = ? {time_clause}
            GROUP BY t.year, t.month
            ORDER BY t.year, t.month
            """

            monthly_result = self.db.execute_query_safe(monthly_sql, params)

            monthly_data = []
            if monthly_result:
                for month_row in monthly_result:
                    monthly_data.append({
                        '年份': month_row[0],
                        '月份': month_row[1],
                        '月记录数': month_row[2],
                        '月记账天数': month_row[3],
                        '月首次记账': month_row[4],
                        '月最后记账': month_row[5]
                    })

            return {
                '总记录数': row[0] or 0,
                '记账天数': row[1] or 0,
                '整数金额数': row[2] or 0,
                '有备注数': row[3] or 0,
                '已编码数': row[4] or 0,
                '平均记账日期': float(row[5]) if row[5] is not None else 0.0,
                '月末记账数': row[6] or 0,
                '项目名称种类数': row[7] or 0,
                '最早记账日期': row[8],
                '最晚记账日期': row[9],
                '月度分布': monthly_data,
                # 计算衍生指标
                '整数金额比例': (row[2] or 0) / max(row[0] or 1, 1),
                '备注使用率': (row[3] or 0) / max(row[0] or 1, 1),
                '编码完整率': (row[4] or 0) / max(row[0] or 1, 1),
                '月末集中记账比例': (row[6] or 0) / max(row[0] or 1, 1)
            }

        except Exception as e:
            self.logger.error(f"获取户记账模式数据失败: {household_code}, 错误: {e}")
            return {}

    def get_all_households_with_data(self, start_year: str = None, start_month: str = None,
                                   end_year: str = None, end_month: str = None) -> List[str]:
        """
        获取有数据的所有户代码列表

        Args:
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            户代码列表
        """
        try:
            # 构建时间筛选条件
            time_conditions = []
            params = []

            if start_year and start_month:
                time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
                params.extend([start_year, start_year, start_month])

            if end_year and end_month:
                time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
                params.extend([end_year, end_year, end_month])

            time_clause = " WHERE " + " AND ".join(time_conditions) if time_conditions else ""

            sql = f"""
            SELECT DISTINCT t.hudm
            FROM 调查点台账合并 t
            {time_clause}
            ORDER BY t.hudm
            """

            result = self.db.execute_query_safe(sql, params)
            return [row[0] for row in result] if result else []

        except Exception as e:
            self.logger.error(f"获取有数据的户代码列表失败: {e}")
            return []
