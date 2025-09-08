#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 记账行为质量评估引擎
评估用户记账的认真程度和数据质量
"""

import logging
from typing import Dict, List, Optional, Tuple
import numpy as np
from datetime import datetime, timedelta
import calendar


class RecordingQualityEngine:
    """记账行为质量评估引擎"""
    
    def __init__(self, dal):
        """
        初始化质量评估引擎
        
        Args:
            dal: 数据访问层对象
        """
        self.dal = dal
        self.logger = logging.getLogger(__name__)
    
    def evaluate_household_quality(self, household_code: str,
                                 start_year: str = None, start_month: str = None,
                                 end_year: str = None, end_month: str = None) -> Dict:
        """
        评估户记账质量
        
        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            记账质量评估结果字典
        """
        try:
            # 获取记账模式数据
            pattern_data = self.dal.get_household_recording_patterns(
                household_code, start_year, start_month, end_year, end_month
            )
            
            if not pattern_data or pattern_data['总记录数'] == 0:
                return {
                    '户代码': household_code,
                    '质量评估': '数据不足',
                    '总评分': 0,
                    '各项评分': {}
                }
            
            # 获取收支明细数据
            income_expense_data = self.dal.get_household_income_expense_data(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 获取月度汇总数据
            monthly_summary = self.dal.get_household_monthly_summary(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 各项质量评估
            frequency_score = self._evaluate_recording_frequency(pattern_data, monthly_summary)
            continuity_score = self._evaluate_recording_continuity(monthly_summary)
            time_distribution_score = self._evaluate_time_distribution(pattern_data, income_expense_data)
            completeness_score = self._evaluate_data_completeness(pattern_data)
            consistency_score = self._evaluate_recording_consistency(income_expense_data)
            
            # 计算总评分（加权平均）
            weights = {
                '记账频率': 0.25,
                '记账连续性': 0.20,
                '时间分布': 0.20,
                '数据完整性': 0.20,
                '记录一致性': 0.15
            }
            
            scores = {
                '记账频率': frequency_score,
                '记账连续性': continuity_score,
                '时间分布': time_distribution_score,
                '数据完整性': completeness_score,
                '记录一致性': consistency_score
            }
            
            total_score = sum(scores[key] * weights[key] for key in weights)
            
            # 质量等级评定
            quality_level = self._get_quality_level(total_score)
            
            return {
                '户代码': household_code,
                '分析时间范围': {
                    '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                    '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                },
                '质量评估': quality_level,
                '总评分': round(total_score, 2),
                '各项评分': scores,
                '评分权重': weights,
                '详细分析': {
                    '基础统计': pattern_data,
                    '质量建议': self._generate_quality_suggestions(scores, pattern_data)
                }
            }
            
        except Exception as e:
            self.logger.error(f"评估户记账质量失败: {household_code}, 错误: {e}")
            return {
                '户代码': household_code,
                '质量评估': '评估失败',
                '总评分': 0,
                '各项评分': {}
            }
    
    def _evaluate_recording_frequency(self, pattern_data: Dict, monthly_summary: List[Dict]) -> float:
        """
        评估记账频率
        
        Args:
            pattern_data: 记账模式数据
            monthly_summary: 月度汇总数据
            
        Returns:
            记账频率评分 (0-100)
        """
        if not monthly_summary:
            return 0
        
        total_records = pattern_data['总记录数']
        months_count = len(monthly_summary)
        
        # 计算平均每月记账笔数
        avg_records_per_month = total_records / months_count
        
        # 评分标准：
        # 优秀(90-100): 每月30笔以上
        # 良好(80-89): 每月20-29笔
        # 一般(70-79): 每月15-19笔
        # 较差(60-69): 每月10-14笔
        # 很差(0-59): 每月10笔以下
        
        if avg_records_per_month >= 30:
            score = 95
        elif avg_records_per_month >= 20:
            score = 85
        elif avg_records_per_month >= 15:
            score = 75
        elif avg_records_per_month >= 10:
            score = 65
        else:
            score = max(0, avg_records_per_month * 6)  # 线性递减
        
        return min(100, score)
    
    def _evaluate_recording_continuity(self, monthly_summary: List[Dict]) -> float:
        """
        评估记账连续性
        
        Args:
            monthly_summary: 月度汇总数据
            
        Returns:
            记账连续性评分 (0-100)
        """
        if not monthly_summary:
            return 0
        
        # 检查是否有连续的月份记录
        months_with_records = len(monthly_summary)
        
        if months_with_records == 1:
            return 60  # 只有一个月的数据
        
        # 计算月份间隔
        sorted_months = sorted(monthly_summary, key=lambda x: (x['年份'], x['月份']))
        gaps = []
        
        for i in range(1, len(sorted_months)):
            prev_year = int(sorted_months[i-1]['年份'])
            prev_month = int(sorted_months[i-1]['月份'])
            curr_year = int(sorted_months[i]['年份'])
            curr_month = int(sorted_months[i]['月份'])
            
            # 计算月份差
            month_diff = (curr_year - prev_year) * 12 + (curr_month - prev_month)
            if month_diff > 1:
                gaps.append(month_diff - 1)
        
        # 连续性评分
        if not gaps:
            score = 100  # 完全连续
        else:
            # 根据间隔扣分
            total_gap_months = sum(gaps)
            gap_penalty = min(40, total_gap_months * 5)  # 每个间隔月扣5分，最多扣40分
            score = max(60, 100 - gap_penalty)
        
        return score
    
    def _evaluate_time_distribution(self, pattern_data: Dict, income_expense_data: List[Dict]) -> float:
        """
        评估时间分布（检测月底集中补记）
        
        Args:
            pattern_data: 记账模式数据
            income_expense_data: 收支明细数据
            
        Returns:
            时间分布评分 (0-100)
        """
        if not income_expense_data:
            return 0
        
        # 统计各日期的记账分布
        date_distribution = {}
        for record in income_expense_data:
            date_str = str(record['日期'])
            if date_str and len(date_str) >= 8:  # 确保日期格式正确
                try:
                    day = int(date_str[-2:])  # 取日期的最后两位作为日
                    date_distribution[day] = date_distribution.get(day, 0) + 1
                except:
                    continue
        
        if not date_distribution:
            return 0
        
        total_records = sum(date_distribution.values())
        
        # 计算月底集中记账比例（25-31日）
        month_end_records = sum(date_distribution.get(day, 0) for day in range(25, 32))
        month_end_ratio = month_end_records / total_records
        
        # 计算分布均匀性（使用变异系数）
        daily_counts = list(date_distribution.values())
        if len(daily_counts) > 1:
            cv = np.std(daily_counts) / np.mean(daily_counts)
        else:
            cv = 0
        
        # 评分标准
        score = 100
        
        # 月底集中记账扣分
        if month_end_ratio > 0.5:
            score -= 30  # 月底记账超过50%，扣30分
        elif month_end_ratio > 0.3:
            score -= 15  # 月底记账超过30%，扣15分
        
        # 分布不均匀扣分
        if cv > 2:
            score -= 20  # 分布极不均匀，扣20分
        elif cv > 1:
            score -= 10  # 分布不均匀，扣10分
        
        return max(0, score)
    
    def _evaluate_data_completeness(self, pattern_data: Dict) -> float:
        """
        评估数据完整性
        
        Args:
            pattern_data: 记账模式数据
            
        Returns:
            数据完整性评分 (0-100)
        """
        # 备注使用率评分 (40分)
        note_score = min(40, pattern_data['备注使用率'] * 100)
        
        # 编码完整率评分 (40分)
        coding_score = min(40, pattern_data['编码完整率'] * 100)
        
        # 项目名称多样性评分 (20分)
        variety_score = min(20, pattern_data['项目名称种类数'] * 2)
        
        total_score = note_score + coding_score + variety_score
        
        return min(100, total_score)
    
    def _evaluate_recording_consistency(self, income_expense_data: List[Dict]) -> float:
        """
        评估记录一致性
        
        Args:
            income_expense_data: 收支明细数据
            
        Returns:
            记录一致性评分 (0-100)
        """
        if not income_expense_data:
            return 0
        
        total_records = len(income_expense_data)
        score = 100
        
        # 检测整数金额过多
        integer_amounts = sum(1 for record in income_expense_data 
                            if record['金额'] == int(record['金额']))
        integer_ratio = integer_amounts / total_records
        
        if integer_ratio > 0.8:
            score -= 25  # 整数金额过多，扣25分
        elif integer_ratio > 0.6:
            score -= 15  # 整数金额较多，扣15分
        
        # 检测重复记录
        seen_records = set()
        duplicate_count = 0
        for record in income_expense_data:
            key = (record['日期'], record['金额'], record['项目名称'])
            if key in seen_records:
                duplicate_count += 1
            else:
                seen_records.add(key)
        
        if duplicate_count > 0:
            duplicate_ratio = duplicate_count / total_records
            score -= min(20, duplicate_ratio * 100)  # 重复记录扣分，最多扣20分
        
        # 检测异常金额模式
        amounts = [record['金额'] for record in income_expense_data if record['金额'] > 0]
        if amounts:
            # 检测是否存在明显的金额模式（如大量100、200、500等整数）
            common_amounts = [100, 200, 500, 1000, 2000, 5000]
            pattern_count = sum(1 for amount in amounts if amount in common_amounts)
            pattern_ratio = pattern_count / len(amounts)
            
            if pattern_ratio > 0.3:
                score -= 15  # 金额模式过于规律，扣15分
        
        return max(0, score)

    def _get_quality_level(self, total_score: float) -> str:
        """
        根据总评分获取质量等级

        Args:
            total_score: 总评分

        Returns:
            质量等级字符串
        """
        if total_score >= 90:
            return '优秀'
        elif total_score >= 80:
            return '良好'
        elif total_score >= 70:
            return '一般'
        elif total_score >= 60:
            return '较差'
        else:
            return '很差'

    def _generate_quality_suggestions(self, scores: Dict, pattern_data: Dict) -> List[str]:
        """
        生成质量改进建议

        Args:
            scores: 各项评分
            pattern_data: 记账模式数据

        Returns:
            改进建议列表
        """
        suggestions = []

        # 记账频率建议
        if scores['记账频率'] < 70:
            suggestions.append("建议增加记账频率，尽量做到每日记账，记录所有收支项目")

        # 记账连续性建议
        if scores['记账连续性'] < 80:
            suggestions.append("建议保持记账的连续性，避免长时间中断记账")

        # 时间分布建议
        if scores['时间分布'] < 80:
            if pattern_data['月末集中记账比例'] > 0.3:
                suggestions.append("避免月底集中补记，建议及时记录每日收支")

        # 数据完整性建议
        if scores['数据完整性'] < 80:
            if pattern_data['备注使用率'] < 0.3:
                suggestions.append("建议增加备注信息，详细说明收支项目的具体内容")
            if pattern_data['编码完整率'] < 0.8:
                suggestions.append("建议完善收支项目的编码，确保分类准确")

        # 记录一致性建议
        if scores['记录一致性'] < 80:
            if pattern_data['整数金额比例'] > 0.7:
                suggestions.append("建议记录精确金额，避免过多使用整数金额")

        if not suggestions:
            suggestions.append("记账质量良好，请继续保持")

        return suggestions

    def evaluate_batch_quality(self, household_codes: List[str],
                             start_year: str = None, start_month: str = None,
                             end_year: str = None, end_month: str = None) -> Dict[str, Dict]:
        """
        批量评估户记账质量

        Args:
            household_codes: 户代码列表
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            户代码到质量评估结果的映射字典
        """
        results = {}

        for household_code in household_codes:
            try:
                result = self.evaluate_household_quality(
                    household_code, start_year, start_month, end_year, end_month
                )
                if result:
                    results[household_code] = result

            except Exception as e:
                self.logger.error(f"评估户 {household_code} 记账质量失败: {e}")
                continue

        return results

    def generate_quality_report(self, household_codes: List[str],
                              start_year: str = None, start_month: str = None,
                              end_year: str = None, end_month: str = None) -> Dict:
        """
        生成质量评估报告

        Args:
            household_codes: 户代码列表
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            质量评估报告字典
        """
        try:
            # 批量评估
            quality_results = self.evaluate_batch_quality(
                household_codes, start_year, start_month, end_year, end_month
            )

            if not quality_results:
                return {'error': '无有效的质量评估数据'}

            # 统计分析
            total_households = len(quality_results)
            scores = [result['总评分'] for result in quality_results.values() if result['总评分'] > 0]

            if not scores:
                return {'error': '无有效的评分数据'}

            # 质量等级分布
            level_distribution = {}
            for result in quality_results.values():
                level = result['质量评估']
                level_distribution[level] = level_distribution.get(level, 0) + 1

            # 各项指标平均分
            indicator_averages = {}
            indicators = ['记账频率', '记账连续性', '时间分布', '数据完整性', '记录一致性']

            for indicator in indicators:
                indicator_scores = [result['各项评分'].get(indicator, 0)
                                 for result in quality_results.values()
                                 if result['各项评分']]
                if indicator_scores:
                    indicator_averages[indicator] = round(np.mean(indicator_scores), 2)

            return {
                '报告生成时间': datetime.now().strftime('%Y年%m月%d日 %H:%M:%S'),
                '分析时间范围': {
                    '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                    '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                },
                '总体统计': {
                    '评估户数': total_households,
                    '平均评分': round(np.mean(scores), 2),
                    '最高评分': max(scores),
                    '最低评分': min(scores),
                    '标准差': round(np.std(scores), 2)
                },
                '质量等级分布': level_distribution,
                '各项指标平均分': indicator_averages,
                '详细结果': quality_results
            }

        except Exception as e:
            self.logger.error(f"生成质量评估报告失败: {e}")
            return {'error': f'生成报告失败: {str(e)}'}
