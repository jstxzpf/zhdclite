#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 消费习惯画像分析引擎
为每个记账户生成多维度消费习惯标签
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime


class ConsumptionProfileEngine:
    """消费习惯画像分析引擎"""
    
    def __init__(self, dal):
        """
        初始化画像分析引擎
        
        Args:
            dal: 数据访问层对象
        """
        self.dal = dal
        self.logger = logging.getLogger(__name__)
    
    def generate_household_profile(self, household_code: str,
                                 start_year: str = None, start_month: str = None,
                                 end_year: str = None, end_month: str = None) -> Dict:
        """
        生成户消费习惯画像
        
        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            消费习惯画像字典
        """
        try:
            # 获取基础数据
            basic_info = self.dal.get_household_basic_info(household_code)
            if not basic_info:
                self.logger.warning(f"无法获取户 {household_code} 的基础信息")
                return {}
            
            # 获取收支数据
            income_expense_data = self.dal.get_household_income_expense_data(
                household_code, start_year, start_month, end_year, end_month
            )
            
            if not income_expense_data:
                self.logger.warning(f"户 {household_code} 无收支数据")
                return {}
            
            # 获取分类汇总数据
            category_summary = self.dal.get_household_category_summary(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 获取月度汇总数据
            monthly_summary = self.dal.get_household_monthly_summary(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 生成各类标签
            profile = {
                '户代码': household_code,
                '户主姓名': basic_info.get('户主姓名'),
                '分析时间范围': {
                    '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                    '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                },
                '消费结构型标签': self._analyze_consumption_structure(category_summary, basic_info),
                '消费水平型标签': self._analyze_consumption_level(monthly_summary, basic_info),
                '财务健康型标签': self._analyze_financial_health(monthly_summary, basic_info),
                '收入结构型标签': self._analyze_income_structure(category_summary),
                '生活方式型标签': self._analyze_lifestyle(category_summary, income_expense_data),
                '消费偏好型标签': self._analyze_consumption_preferences(category_summary, income_expense_data),
                # 添加详细分析
                '消费习惯': self._analyze_consumption_habits(category_summary, monthly_summary, income_expense_data),
                '消费结构': self._analyze_detailed_consumption_structure(category_summary)
            }
            
            return profile
            
        except Exception as e:
            self.logger.error(f"生成户消费画像失败: {household_code}, 错误: {e}")
            return {}
    
    def _analyze_consumption_structure(self, category_summary: List[Dict], basic_info: Dict) -> List[str]:
        """
        分析消费结构型标签
        
        Args:
            category_summary: 分类汇总数据
            basic_info: 基础信息
            
        Returns:
            消费结构型标签列表
        """
        tags = []
        
        # 计算各类消费支出占比
        expense_data = [item for item in category_summary if item['收支类型'] == 2]
        total_expense = sum(item['总金额'] for item in expense_data)
        
        if total_expense <= 0:
            return ['数据不足']
        
        # 计算各类支出占比
        ratios = {}
        for item in expense_data:
            prefix = item['编码前缀']
            ratios[prefix] = item['总金额'] / total_expense
        
        # 基本生活型判断（食品烟酒占比高）
        food_ratio = ratios.get('31', 0)
        if food_ratio > 0.4:
            tags.append('基本生活型')
        elif food_ratio < 0.2:
            tags.append('发展享受型')
        
        # 交通依赖型判断
        transport_ratio = ratios.get('35', 0)
        if transport_ratio > 0.2:
            tags.append('交通依赖型')
        
        # 住房高压型判断
        housing_ratio = ratios.get('33', 0)
        if housing_ratio > 0.3:
            tags.append('住房高压型')
        
        # 教育投资型判断
        education_ratio = ratios.get('36', 0)
        if education_ratio > 0.15:
            tags.append('教育投资型')
        
        # 健康关注型判断
        health_ratio = ratios.get('37', 0)
        if health_ratio > 0.15:
            tags.append('健康关注型')
        
        return tags if tags else ['均衡消费型']
    
    def _analyze_consumption_level(self, monthly_summary: List[Dict], basic_info: Dict) -> List[str]:
        """
        分析消费水平型标签
        
        Args:
            monthly_summary: 月度汇总数据
            basic_info: 基础信息
            
        Returns:
            消费水平型标签列表
        """
        tags = []
        
        if not monthly_summary:
            return ['数据不足']
        
        # 计算平均月支出和人均支出
        total_expense = sum(item['支出总额'] for item in monthly_summary)
        months_count = len(monthly_summary)
        household_size = basic_info.get('人数', 1) or 1
        
        avg_monthly_expense = total_expense / months_count if months_count > 0 else 0
        avg_per_capita_expense = avg_monthly_expense / household_size
        
        # 根据人均月支出水平分类（这里使用相对标准，实际应用中可根据当地经济水平调整）
        if avg_per_capita_expense > 3000:
            tags.append('高消费户')
        elif avg_per_capita_expense > 1500:
            tags.append('理性消费户')
        elif avg_per_capita_expense > 800:
            tags.append('节俭储蓄型')
        else:
            tags.append('低消费户')
        
        # 消费稳定性分析
        if months_count >= 3:
            expenses = [item['支出总额'] for item in monthly_summary]
            cv = np.std(expenses) / np.mean(expenses) if np.mean(expenses) > 0 else 0
            
            if cv < 0.2:
                tags.append('消费稳定型')
            elif cv > 0.5:
                tags.append('消费波动型')
        
        return tags
    
    def _analyze_financial_health(self, monthly_summary: List[Dict], basic_info: Dict) -> List[str]:
        """
        分析财务健康型标签
        
        Args:
            monthly_summary: 月度汇总数据
            basic_info: 基础信息
            
        Returns:
            财务健康型标签列表
        """
        tags = []
        
        if not monthly_summary:
            return ['数据不足']
        
        # 计算储蓄率
        total_income = sum(item['收入总额'] for item in monthly_summary)
        total_expense = sum(item['支出总额'] for item in monthly_summary)
        
        if total_income > 0:
            savings_rate = (total_income - total_expense) / total_income
            
            if savings_rate > 0.3:
                tags.append('高储蓄率家庭')
            elif savings_rate > 0.1:
                tags.append('稳健储蓄家庭')
            elif savings_rate > -0.1:
                tags.append('月光家庭')
            else:
                tags.append('债务驱动型')
        
        # 收支平衡分析
        deficit_months = sum(1 for item in monthly_summary if item['收支差额'] < 0)
        total_months = len(monthly_summary)
        
        if deficit_months / total_months > 0.5:
            tags.append('收支失衡型')
        elif deficit_months == 0:
            tags.append('收支平衡型')
        
        return tags
    
    def _analyze_income_structure(self, category_summary: List[Dict]) -> List[str]:
        """
        分析收入结构型标签
        
        Args:
            category_summary: 分类汇总数据
            
        Returns:
            收入结构型标签列表
        """
        tags = []
        
        # 获取收入数据
        income_data = [item for item in category_summary if item['收支类型'] == 1]
        total_income = sum(item['总金额'] for item in income_data)
        
        if total_income <= 0:
            return ['数据不足']
        
        # 计算各类收入占比
        income_ratios = {}
        for item in income_data:
            prefix = item['编码前缀']
            income_ratios[prefix] = item['总金额'] / total_income
        
        # 工资主导型（21开头）
        wage_ratio = income_ratios.get('21', 0)
        if wage_ratio > 0.6:
            tags.append('工资主导型')
        
        # 经营主导型（22开头）
        business_ratio = income_ratios.get('22', 0)
        if business_ratio > 0.5:
            tags.append('经营主导型')
        
        # 财产投资驱动型（23开头）
        property_ratio = income_ratios.get('23', 0)
        if property_ratio > 0.3:
            tags.append('财产投资驱动型')
        
        # 多元收入型（没有单一收入占比超过60%）
        max_ratio = max(income_ratios.values()) if income_ratios else 0
        if max_ratio < 0.6 and len(income_ratios) >= 2:
            tags.append('多元收入型')
        
        return tags if tags else ['单一收入型']

    def _analyze_lifestyle(self, category_summary: List[Dict], income_expense_data: List[Dict]) -> List[str]:
        """
        分析生活方式型标签

        Args:
            category_summary: 分类汇总数据
            income_expense_data: 收支明细数据

        Returns:
            生活方式型标签列表
        """
        tags = []

        # 获取支出数据
        expense_data = [item for item in category_summary if item['收支类型'] == 2]
        total_expense = sum(item['总金额'] for item in expense_data)

        if total_expense <= 0:
            return ['数据不足']

        # 计算各类支出占比
        expense_ratios = {}
        for item in expense_data:
            prefix = item['编码前缀']
            expense_ratios[prefix] = item['总金额'] / total_expense

        # 家庭成长型（教育文化娱乐支出高）
        education_ratio = expense_ratios.get('36', 0)
        if education_ratio > 0.15:
            tags.append('家庭成长型')

        # 人情社交型（其他用品及服务支出高，通常包含礼金等）
        other_ratio = expense_ratios.get('38', 0)
        if other_ratio > 0.1:
            tags.append('人情社交型')

        # 数字生活家（通过交通通信支出判断，包含通信费、网络费等）
        transport_comm_ratio = expense_ratios.get('35', 0)
        if transport_comm_ratio > 0.15:
            # 进一步分析明细数据中是否有通信相关支出
            comm_related = [item for item in income_expense_data
                          if item['收支类型'] == 2 and item['编码'] and item['编码'].startswith('35')
                          and any(keyword in (item['项目名称'] or '') for keyword in ['通信', '网络', '流量', '话费', '宽带'])]
            if comm_related:
                tags.append('数字生活家')

        # 便捷生活追求者（生活用品及服务支出较高）
        daily_goods_ratio = expense_ratios.get('34', 0)
        if daily_goods_ratio > 0.08:
            tags.append('便捷生活追求者')

        # 品质生活型（衣着支出占比较高）
        clothing_ratio = expense_ratios.get('32', 0)
        if clothing_ratio > 0.08:
            tags.append('品质生活型')

        return tags if tags else ['朴素生活型']

    def _analyze_consumption_preferences(self, category_summary: List[Dict],
                                       income_expense_data: List[Dict]) -> List[str]:
        """
        分析消费偏好型标签

        Args:
            category_summary: 分类汇总数据
            income_expense_data: 收支明细数据

        Returns:
            消费偏好型标签列表
        """
        tags = []

        # 获取支出数据
        expense_data = [item for item in category_summary if item['收支类型'] == 2]
        total_expense = sum(item['总金额'] for item in expense_data)

        if total_expense <= 0:
            return ['数据不足']

        # 计算各类支出占比
        expense_ratios = {}
        for item in expense_data:
            prefix = item['编码前缀']
            expense_ratios[prefix] = item['总金额'] / total_expense

        # 美食爱好者（食品烟酒支出高且种类丰富）
        food_ratio = expense_ratios.get('31', 0)
        if food_ratio > 0.35:
            # 检查食品支出的多样性
            food_items = [item for item in income_expense_data
                         if item['收支类型'] == 2 and item['编码'] and item['编码'].startswith('31')]
            unique_food_types = len(set(item['项目名称'] for item in food_items if item['项目名称']))
            if unique_food_types > 10:
                tags.append('美食爱好者')

        # 健康养生派（医疗保健支出高）
        health_ratio = expense_ratios.get('37', 0)
        if health_ratio > 0.12:
            tags.append('健康养生派')

        # 爱宠家庭（通过项目名称关键词识别）
        pet_related = [item for item in income_expense_data
                      if item['收支类型'] == 2 and item['项目名称'] and
                      any(keyword in item['项目名称'] for keyword in ['宠物', '猫', '狗', '鸟', '鱼', '宠'])]
        if pet_related and sum(item['金额'] for item in pet_related) > 500:
            tags.append('爱宠家庭')

        # 汽车生活族（交通支出高且有汽车相关支出）
        transport_ratio = expense_ratios.get('35', 0)
        if transport_ratio > 0.15:
            car_related = [item for item in income_expense_data
                          if item['收支类型'] == 2 and item['编码'] and item['编码'].startswith('35')
                          and any(keyword in (item['项目名称'] or '') for keyword in ['汽车', '车', '油费', '停车', '保险', '维修'])]
            if car_related:
                tags.append('汽车生活族')

        # 居家装修族（居住支出高）
        housing_ratio = expense_ratios.get('33', 0)
        if housing_ratio > 0.2:
            decoration_related = [item for item in income_expense_data
                                if item['收支类型'] == 2 and item['编码'] and item['编码'].startswith('33')
                                and any(keyword in (item['项目名称'] or '') for keyword in ['装修', '家具', '电器', '建材'])]
            if decoration_related:
                tags.append('居家装修族')

        # 文化娱乐型（教育文化娱乐支出中娱乐部分较多）
        culture_ratio = expense_ratios.get('36', 0)
        if culture_ratio > 0.1:
            entertainment_related = [item for item in income_expense_data
                                   if item['收支类型'] == 2 and item['编码'] and item['编码'].startswith('36')
                                   and any(keyword in (item['项目名称'] or '') for keyword in ['娱乐', '旅游', '电影', '游戏', '运动'])]
            if entertainment_related:
                tags.append('文化娱乐型')

        return tags if tags else ['实用主义型']

    def generate_batch_profiles(self, household_codes: List[str],
                              start_year: str = None, start_month: str = None,
                              end_year: str = None, end_month: str = None) -> Dict[str, Dict]:
        """
        批量生成户消费习惯画像

        Args:
            household_codes: 户代码列表
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            户代码到画像的映射字典
        """
        profiles = {}

        for household_code in household_codes:
            try:
                profile = self.generate_household_profile(
                    household_code, start_year, start_month, end_year, end_month
                )
                if profile:
                    profiles[household_code] = profile

            except Exception as e:
                self.logger.error(f"生成户 {household_code} 画像失败: {e}")
                continue

        return profiles

    def _analyze_consumption_habits(self, category_summary: List[Dict],
                                  monthly_summary: List[Dict],
                                  income_expense_data: List[Dict]) -> Dict:
        """
        分析消费习惯详情

        Args:
            category_summary: 分类汇总数据
            monthly_summary: 月度汇总数据
            income_expense_data: 收支明细数据

        Returns:
            消费习惯详情字典
        """
        try:
            habits = {}

            # 分析各类消费的频次和平均金额
            expense_data = [item for item in category_summary if item['收支类型'] == 2]

            for item in expense_data:
                category = item.get('项目名称', '未知类别')
                total_amount = item.get('总金额', 0)
                count = item.get('记录数', 0)

                if count > 0:
                    avg_amount = total_amount / count
                    habits[category] = {
                        '总金额': total_amount,
                        '频次': count,
                        '平均金额': avg_amount,
                        '占比': 0  # 稍后计算
                    }

            # 计算占比
            total_expense = sum(h['总金额'] for h in habits.values())
            if total_expense > 0:
                for habit in habits.values():
                    habit['占比'] = habit['总金额'] / total_expense

            return habits

        except Exception as e:
            self.logger.error(f"分析消费习惯失败: {e}")
            return {}

    def _analyze_detailed_consumption_structure(self, category_summary: List[Dict]) -> Dict:
        """
        分析详细消费结构，按照标准消费支出编码（31-38）分类

        Args:
            category_summary: 分类汇总数据

        Returns:
            详细消费结构字典
        """
        try:
            # 标准消费支出类型映射
            consumption_categories = {
                '31': '食品烟酒',
                '32': '衣着',
                '33': '居住',
                '34': '生活用品及服务',
                '35': '交通通信',
                '36': '教育文化娱乐',
                '37': '医疗保健',
                '38': '其他用品及服务'
            }

            # 初始化结构
            structure = {category: 0.0 for category in consumption_categories.values()}
            structure['其他消费'] = 0.0  # 用于不在标准分类中的支出

            # 按支出类别统计
            expense_data = [item for item in category_summary if item['收支类型'] == 2]

            for item in expense_data:
                # 从数据访问层获取的是编码前缀（已经是前两位）
                code_prefix = str(item.get('编码前缀', ''))
                amount = float(item.get('总金额', 0))

                if amount <= 0:
                    continue

                # 根据编码前缀分类
                if code_prefix in consumption_categories:
                    category_name = consumption_categories[code_prefix]
                    structure[category_name] += amount
                else:
                    # 不在标准分类中的支出归入"其他消费"
                    structure['其他消费'] += amount

            # 移除金额为0的类别
            structure = {k: v for k, v in structure.items() if v > 0}

            # 计算总金额和占比
            total_amount = sum(structure.values())

            # 构建详细结果
            detailed_structure = {
                'categories': [],
                'total_expenditure': total_amount,
                'category_count': len(structure)
            }

            # 按金额排序并添加占比信息
            sorted_categories = sorted(structure.items(), key=lambda x: x[1], reverse=True)

            for category, amount in sorted_categories:
                percentage = (amount / total_amount * 100) if total_amount > 0 else 0
                detailed_structure['categories'].append({
                    'name': category,
                    'amount': amount,
                    'percentage': percentage
                })

            return detailed_structure

        except Exception as e:
            self.logger.error(f"分析详细消费结构失败: {e}")
            return {
                'categories': [],
                'total_expenditure': 0,
                'category_count': 0
            }
