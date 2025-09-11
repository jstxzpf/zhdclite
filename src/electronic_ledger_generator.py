#!/usr/bin/env python3
"""
重新设计的电子台账生成器
使用简单直接的SQL查询，避免复杂的多表关联和性能问题
"""

import pandas as pd
import logging


class ElectronicLedgerGenerator:
    """电子台账生成器"""
    
    def __init__(self, db):
        self.db = db
        self.logger = logging.getLogger(__name__)

    def _get_town_villages(self, town_name):
        """
        从v_town_village_list视图获取指定乡镇的所有村代码
        严格按照数据源规范：使用v_town_village_list视图作为权威数据源
        """
        sql = """
        SELECT DISTINCT 村代码
        FROM v_town_village_list
        WHERE 所在乡镇街道 = ? AND 村代码 IS NOT NULL AND 村代码 != ''
        """
        result = self.db.execute_query_safe(sql, [town_name])
        if result:
            return [row[0] for row in result]
        self.logger.warning(f"未能从v_town_village_list视图中找到乡镇 '{town_name}' 的村代码")
        return []

    def _get_village_code(self, village_name):
        """
        从v_town_village_list视图获取指定村庄的代码
        严格按照数据源规范：使用v_town_village_list视图作为权威数据源
        """
        sql = """
        SELECT DISTINCT 村代码
        FROM v_town_village_list
        WHERE 村居名称 = ? AND 村代码 IS NOT NULL AND 村代码 != ''
        """
        result = self.db.execute_query_safe(sql, [village_name])
        if result and result[0]:
            return result[0][0]
        self.logger.warning(f"未能从v_town_village_list视图中找到村庄 '{village_name}' 的代码")
        return None

    def generate(self, year, month, town, village=None):
        """
        生成电子台账的三个工作表
        
        Args:
            year: 年份 (如 '2024')
            month: 月份 (如 '06')
            town: 乡镇名称 (如 '分界镇')
            village: 村庄名称 (可选, 如 '三营居委会')
            
        Returns:
            tuple: (汇总表DataFrame, 分户详细账DataFrame, 分户消费结构DataFrame)
        """
        # 获取地区代码
        if village:
            # 如果指定了村庄，使用村庄代码
            area_code = self._get_village_code(village)
            if not area_code:
                raise ValueError(f"未找到村庄 '{village}' 对应的代码")
            is_village_level = True
            self.logger.info(f"开始生成电子台账 - 村庄: {village} (代码: {area_code}), 年度: {year}, 月份: {month}")
        else:
            # 如果只指定了乡镇，获取该乡镇的所有村代码
            village_codes = self._get_town_villages(town)
            if not village_codes:
                raise ValueError(f"未找到乡镇 '{town}' 对应的村代码")
            area_code = village_codes  # 乡镇级别使用村代码列表
            is_village_level = False
            self.logger.info(f"开始生成电子台账 - 乡镇: {town} (包含{len(village_codes)}个村), 年度: {year}, 月份: {month}")

        try:
            # 生成三个工作表的数据
            summary_df = self._generate_summary_table(year, month, area_code, is_village_level)
            detail_df = self._generate_detail_table(year, month, area_code, is_village_level)
            consumption_df = self._generate_consumption_table(year, month, area_code, is_village_level)

            # 数据验证和清理
            summary_df = self._clean_summary_data(summary_df)
            detail_df = self._clean_detail_data(detail_df)
            consumption_df = self._clean_consumption_data(consumption_df)

            self.logger.info(f"电子台账生成完成 - 汇总表: {len(summary_df)}行, 分户详细账: {len(detail_df)}行, 分户消费结构: {len(consumption_df)}行")
            return summary_df, detail_df, consumption_df

        except Exception as e:
            self.logger.error(f"生成电子台账失败: {str(e)}")
            import traceback
            traceback.print_exc()
            # 返回空DataFrame
            return self._get_empty_dataframes()

    def _generate_summary_table(self, year, month, area_code, is_village=False):
        """
        生成汇总表数据
        严格按照数据源规范：使用户代码前12位与村代码关联，户主信息从调查点户名单表获取
        """
        self.logger.info("开始生成汇总表...")

        # 根据是否是村庄级别决定筛选条件
        if is_village:
            # 村庄级别：精确匹配户代码前12位与村代码（SQLite 使用 SUBSTR）
            where_condition = "SUBSTR(t.hudm, 1, 12) = ?"
            code_param = area_code
        else:
            # 乡镇级别：匹配户代码前12位与该乡镇的所有村代码
            if isinstance(area_code, list) and area_code:
                placeholders = ','.join(['?' for _ in area_code])
                where_condition = f"SUBSTR(t.hudm, 1, 12) IN ({placeholders})"
                code_param = area_code
            else:
                self.logger.error("乡镇级别查询缺少村代码列表")
                return pd.DataFrame()
        
        # 简化的汇总表查询 - 一次性获取所有需要的数据
        sql = f"""
        SELECT 
            t.hudm AS 户代码,
            h.户主姓名,
            SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) AS 收入,
            SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) AS 支出,
            COUNT(*) AS 记账笔数,
            0 AS 漏记账天数
        FROM 调查点台账合并 t
        INNER JOIN 调查点户名单 h ON t.hudm = h.户代码
        WHERE t.year = ? AND t.month = ? AND {where_condition}
        GROUP BY t.hudm, h.户主姓名
        ORDER BY t.hudm
        """
        
        try:
            # 构建查询参数
            if is_village:
                query_params = [year, month, code_param]
            else:
                query_params = [year, month] + code_param

            result = self.db.execute_query_safe(sql, query_params)
            if result:
                columns = ['户代码', '户主姓名', '收入', '支出', '记账笔数', '漏记账天数']
                # 将pyodbc.Row对象转换为列表
                data = [list(row) for row in result]
                df = pd.DataFrame(data, columns=columns)
                self.logger.info(f"汇总表生成成功，共 {len(df)} 户")
                return df
            else:
                self.logger.warning("汇总表查询结果为空")
                return pd.DataFrame(columns=['户代码', '户主姓名', '收入', '支出', '记账笔数', '漏记账天数'])
        except Exception as e:
            self.logger.error(f"汇总表生成失败: {str(e)}")
            return pd.DataFrame(columns=['户代码', '户主姓名', '收入', '支出', '记账笔数', '漏记账天数'])

    def _generate_detail_table(self, year, month, area_code, is_village=False):
        """
        生成分户详细账数据
        严格按照数据源规范：使用户代码前12位与村代码关联，户主信息从调查点户名单表获取
        """
        self.logger.info("开始生成分户详细账...")

        # 根据是否是村庄级别决定筛选条件
        if is_village:
            # 村庄级别：精确匹配户代码前12位与村代码（SQLite 使用 SUBSTR）
            where_condition = "SUBSTR(t.hudm, 1, 12) = ?"
            code_param = area_code
        else:
            # 乡镇级别：匹配户代码前12位与该乡镇的所有村代码
            if isinstance(area_code, list) and area_code:
                placeholders = ','.join(['?' for _ in area_code])
                where_condition = f"SUBSTR(t.hudm, 1, 12) IN ({placeholders})"
                code_param = area_code
            else:
                self.logger.error("乡镇级别查询缺少村代码列表")
                return pd.DataFrame()
        
        # 简化的分户详细账查询
        sql = f"""
        SELECT 
            t.hudm AS 户代码,
            h.户主姓名,
            t.code AS 编码,
            t.amount AS 数量,
            t.money AS 金额,
            t.date AS 日期,
            t.type AS 收支类型,
            t.id AS ID,
            t.type_name AS 类型名称,
            t.unit_name AS 单位名称
        FROM 调查点台账合并 t
        INNER JOIN 调查点户名单 h ON t.hudm = h.户代码
        WHERE t.year = ? AND t.month = ? AND {where_condition} AND t.code IS NOT NULL
        ORDER BY t.hudm, t.type, t.date
        """
        
        try:
            # 构建查询参数
            if is_village:
                query_params = [year, month, code_param]
            else:
                query_params = [year, month] + code_param

            result = self.db.execute_query_safe(sql, query_params)
            if result:
                columns = ['户代码', '户主姓名', '编码', '数量', '金额', '日期', '收支类型', 'ID', '类型名称', '单位名称']
                # 将pyodbc.Row对象转换为列表
                data = [list(row) for row in result]
                df = pd.DataFrame(data, columns=columns)
                self.logger.info(f"分户详细账生成成功，共 {len(df)} 条记录")
                return df
            else:
                self.logger.warning("分户详细账查询结果为空")
                return pd.DataFrame(columns=['户代码', '户主姓名', '编码', '数量', '金额', '日期', '收支类型', 'ID', '类型名称', '单位名称'])
        except Exception as e:
            self.logger.error(f"分户详细账生成失败: {str(e)}")
            return pd.DataFrame(columns=['户代码', '户主姓名', '编码', '数量', '金额', '日期', '收支类型', 'ID', '类型名称', '单位名称'])

    def _generate_consumption_table(self, year, month, area_code, is_village=False):
        """
        生成分户消费结构数据
        严格按照数据源规范：使用户代码前12位与村代码关联，户主信息从调查点户名单表获取
        """
        self.logger.info("开始生成分户消费结构...")

        # 根据是否是村庄级别决定筛选条件
        if is_village:
            # 村庄级别：精确匹配户代码前12位与村代码（SQLite 使用 SUBSTR）
            where_condition = "SUBSTR(t.hudm, 1, 12) = ?"
            code_param = area_code
        else:
            # 乡镇级别：匹配户代码前12位与该乡镇的所有村代码
            if isinstance(area_code, list) and area_code:
                placeholders = ','.join(['?' for _ in area_code])
                where_condition = f"SUBSTR(t.hudm, 1, 12) IN ({placeholders})"
                code_param = area_code
            else:
                self.logger.error("乡镇级别查询缺少村代码列表")
                return pd.DataFrame()
        
        # 简化的分户消费结构查询
        sql = f"""
        SELECT
            t.hudm AS 户代码,
            h.户主姓名,
            t.code AS 编码,
            COALESCE(c.帐目指标名称, '未知') AS 帐目指标名称,
            SUM(t.money) AS 总金额,
            COUNT(*) AS 记账笔数
        FROM 调查点台账合并 t
        INNER JOIN 调查点户名单 h ON t.hudm = h.户代码
        LEFT JOIN 调查品种编码 c ON t.code = c.帐目编码
        WHERE t.year = ? AND t.month = ? AND {where_condition} AND t.code IS NOT NULL
        GROUP BY t.hudm, h.户主姓名, t.code, c.帐目指标名称
        ORDER BY t.hudm, t.code
        """
        
        try:
            # 构建查询参数
            if is_village:
                query_params = [year, month, code_param]
            else:
                query_params = [year, month] + code_param

            result = self.db.execute_query_safe(sql, query_params)
            if result:
                columns = ['户代码', '户主姓名', '编码', '帐目指标名称', '总金额', '记账笔数']
                # 将pyodbc.Row对象转换为列表
                data = [list(row) for row in result]
                df = pd.DataFrame(data, columns=columns)
                self.logger.info(f"分户消费结构生成成功，共 {len(df)} 条记录")
                return df
            else:
                self.logger.warning("分户消费结构查询结果为空")
                return pd.DataFrame(columns=['户代码', '户主姓名', '编码', '帐目指标名称', '总金额', '记账笔数'])
        except Exception as e:
            self.logger.error(f"分户消费结构生成失败: {str(e)}")
            return pd.DataFrame(columns=['户代码', '户主姓名', '编码', '帐目指标名称', '总金额', '记账笔数'])

    def _clean_summary_data(self, df):
        """清理汇总表数据"""
        if df.empty:
            return df
            
        # 处理空值
        df = df.fillna(0)
        
        # 确保数值列为数值类型
        numeric_cols = ['收入', '支出', '记账笔数', '漏记账天数']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df

    def _clean_detail_data(self, df):
        """清理分户详细账数据"""
        if df.empty:
            return df
            
        # 处理空值
        df = df.fillna('')
        
        # 确保数值列为数值类型
        numeric_cols = ['数量', '金额', 'ID']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 处理日期格式
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        
        return df

    def _clean_consumption_data(self, df):
        """清理分户消费结构数据"""
        if df.empty:
            return df
            
        # 处理空值
        df = df.fillna('')
        
        # 确保数值列为数值类型
        numeric_cols = ['总金额', '记账笔数']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df

    def _get_empty_dataframes(self):
        """返回空的DataFrame"""
        summary_df = pd.DataFrame(columns=['户代码', '户主姓名', '收入', '支出', '记账笔数', '漏记账天数'])
        detail_df = pd.DataFrame(columns=['户代码', '户主姓名', '编码', '数量', '金额', '日期', '收支类型', 'ID', '类型名称', '单位名称'])
        consumption_df = pd.DataFrame(columns=['户代码', '户主姓名', '编码', '帐目指标名称', '总金额', '记账笔数'])
        return summary_df, detail_df, consumption_df
