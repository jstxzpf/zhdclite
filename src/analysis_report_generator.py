#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 分析结果输出层
实现JSON格式的分析报告生成，包含消费画像、异常记录清单、记账质量评分等
"""

import logging
import json
from typing import Dict, List
from datetime import datetime
from src.household_analysis_dal import HouseholdAnalysisDAL
from src.consumption_profile_engine import ConsumptionProfileEngine
from src.anomaly_detection_engine import AnomalyDetectionEngine
from src.recording_quality_engine import RecordingQualityEngine


class AnalysisReportGenerator:
    """分析结果输出层"""
    
    def __init__(self, db):
        """
        初始化分析报告生成器
        
        Args:
            db: 数据库连接对象
        """
        self.db = db
        self.logger = logging.getLogger(__name__)
        
        # 初始化各个分析引擎
        self.dal = HouseholdAnalysisDAL(db)
        self.profile_engine = ConsumptionProfileEngine(self.dal)
        self.anomaly_engine = AnomalyDetectionEngine(self.dal)
        self.quality_engine = RecordingQualityEngine(self.dal)
    
    def generate_household_analysis_report(self, household_code: str,
                                         start_year: str = None, start_month: str = None,
                                         end_year: str = None, end_month: str = None) -> Dict:
        """
        生成单户完整分析报告
        
        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            完整分析报告字典
        """
        try:
            self.logger.info(f"开始生成户 {household_code} 的分析报告")
            
            # 获取基础信息
            basic_info = self.dal.get_household_basic_info(household_code)
            if not basic_info:
                return {
                    'success': False,
                    'error': f'未找到户代码 {household_code} 的基础信息'
                }

            # 扩展基本信息，添加统计数据
            basic_info = self._enrich_basic_info(basic_info, household_code, start_year, start_month, end_year, end_month)
            
            # 生成消费习惯画像
            consumption_profile = self.profile_engine.generate_household_profile(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 检测异常记录
            anomaly_detection = self.anomaly_engine.detect_household_anomalies(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 评估记账质量
            quality_assessment = self.quality_engine.evaluate_household_quality(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 生成综合评估
            comprehensive_assessment = self._generate_comprehensive_assessment(
                consumption_profile, anomaly_detection, quality_assessment
            )
            
            # 构建完整报告
            report = {
                'success': True,
                'report_metadata': {
                    '报告生成时间': datetime.now().strftime('%Y年%m月%d日 %H:%M:%S'),
                    '户代码': household_code,
                    '户主姓名': basic_info.get('户主姓名'),
                    '分析时间范围': {
                        '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                        '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                    }
                },
                'household_basic_info': basic_info,
                'consumption_profile': consumption_profile,
                'anomaly_detection': anomaly_detection,
                'quality_assessment': quality_assessment,
                'comprehensive_assessment': comprehensive_assessment
            }
            
            self.logger.info(f"户 {household_code} 分析报告生成完成")
            return report

        except Exception as e:
            self.logger.error(f"生成户 {household_code} 分析报告失败: {e}")
            return {
                'success': False,
                'error': f'生成分析报告失败: {str(e)}'
            }

    def _enrich_basic_info(self, basic_info: Dict, household_code: str,
                          start_year: str = None, start_month: str = None,
                          end_year: str = None, end_month: str = None) -> Dict:
        """
        扩展基本信息，添加统计数据

        Args:
            basic_info: 基础信息字典
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            扩展后的基本信息字典
        """
        try:
            # 获取收支数据进行统计
            income_expense_data = self.dal.get_household_income_expense_data(
                household_code, start_year, start_month, end_year, end_month
            )

            # 获取月度汇总数据
            monthly_summary = self.dal.get_household_monthly_summary(
                household_code, start_year, start_month, end_year, end_month
            )

            # 计算统计指标
            total_income = sum(record['金额'] for record in income_expense_data if record['收支类型'] == 1)
            total_expense = sum(record['金额'] for record in income_expense_data if record['收支类型'] == 2)
            net_income = total_income - total_expense

            # 计算记账天数
            unique_dates = set()
            for record in income_expense_data:
                if record.get('日期'):
                    unique_dates.add(record['日期'])
            record_days = len(unique_dates)

            # 记录总数
            total_records = len(income_expense_data)

            # 扩展基本信息
            basic_info.update({
                '总收入': total_income,
                '总支出': total_expense,
                '净收入': net_income,
                '记账天数': record_days,
                '记录总数': total_records,
                '月度数据': len(monthly_summary)
            })

            return basic_info

        except Exception as e:
            self.logger.error(f"扩展基本信息失败: {household_code}, 错误: {e}")
            return basic_info
    
    def generate_batch_analysis_report(self, household_codes: List[str],
                                     start_year: str = None, start_month: str = None,
                                     end_year: str = None, end_month: str = None) -> Dict:
        """
        生成批量分析报告
        
        Args:
            household_codes: 户代码列表
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            批量分析报告字典
        """
        try:
            self.logger.info(f"开始生成 {len(household_codes)} 户的批量分析报告")
            
            # 批量生成各类分析
            consumption_profiles = self.profile_engine.generate_batch_profiles(
                household_codes, start_year, start_month, end_year, end_month
            )
            
            anomaly_detections = self.anomaly_engine.detect_batch_anomalies(
                household_codes, start_year, start_month, end_year, end_month
            )
            
            quality_assessments = self.quality_engine.evaluate_batch_quality(
                household_codes, start_year, start_month, end_year, end_month
            )
            
            # 生成统计汇总
            batch_statistics = self._generate_batch_statistics(
                consumption_profiles, anomaly_detections, quality_assessments
            )
            
            # 构建批量报告
            report = {
                'success': True,
                'report_metadata': {
                    '报告生成时间': datetime.now().strftime('%Y年%m月%d日 %H:%M:%S'),
                    '分析户数': len(household_codes),
                    '分析时间范围': {
                        '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                        '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                    }
                },
                'batch_statistics': batch_statistics,
                'detailed_results': {
                    'consumption_profiles': consumption_profiles,
                    'anomaly_detections': anomaly_detections,
                    'quality_assessments': quality_assessments
                }
            }
            
            self.logger.info(f"批量分析报告生成完成，共分析 {len(household_codes)} 户")
            return report
            
        except Exception as e:
            self.logger.error(f"生成批量分析报告失败: {e}")
            return {
                'success': False,
                'error': f'生成批量分析报告失败: {str(e)}'
            }
    
    def generate_area_analysis_report(self, town_name: str = None, village_name: str = None,
                                    start_year: str = None, start_month: str = None,
                                    end_year: str = None, end_month: str = None) -> Dict:
        """
        生成区域分析报告
        
        Args:
            town_name: 乡镇名称
            village_name: 村庄名称
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            区域分析报告字典
        """
        try:
            # 获取区域内的户代码列表
            household_codes = self.dal.get_households_by_area(town_name, village_name)
            
            if not household_codes:
                return {
                    'success': False,
                    'error': f'未找到指定区域的户数据'
                }
            
            # 生成批量分析报告
            batch_report = self.generate_batch_analysis_report(
                household_codes, start_year, start_month, end_year, end_month
            )
            
            if not batch_report['success']:
                return batch_report
            
            # 添加区域特定信息
            batch_report['report_metadata']['区域信息'] = {
                '乡镇': town_name,
                '村庄': village_name,
                '户数': len(household_codes)
            }
            
            return batch_report
            
        except Exception as e:
            self.logger.error(f"生成区域分析报告失败: {e}")
            return {
                'success': False,
                'error': f'生成区域分析报告失败: {str(e)}'
            }
    
    def _generate_comprehensive_assessment(self, consumption_profile: Dict,
                                         anomaly_detection: Dict,
                                         quality_assessment: Dict) -> Dict:
        """
        生成综合评估
        
        Args:
            consumption_profile: 消费画像
            anomaly_detection: 异常检测结果
            quality_assessment: 质量评估结果
            
        Returns:
            综合评估字典
        """
        try:
            # 提取关键指标
            quality_score = quality_assessment.get('总评分', 0)
            anomaly_score = anomaly_detection.get('异常统计', {}).get('异常评分', 0)
            
            # 计算综合评分（质量评分权重70%，异常评分权重30%，异常评分越高越差）
            comprehensive_score = quality_score * 0.7 + (100 - anomaly_score) * 0.3
            
            # 确定综合等级
            if comprehensive_score >= 90:
                level = '优秀'
                description = '记账质量优秀，数据可靠性高，异常情况少'
            elif comprehensive_score >= 80:
                level = '良好'
                description = '记账质量良好，数据基本可靠，存在少量异常'
            elif comprehensive_score >= 70:
                level = '一般'
                description = '记账质量一般，数据可用性中等，需要关注异常情况'
            elif comprehensive_score >= 60:
                level = '较差'
                description = '记账质量较差，数据可靠性不高，存在较多异常'
            else:
                level = '很差'
                description = '记账质量很差，数据可靠性低，异常情况严重'
            
            # 生成改进建议
            improvement_suggestions = []
            
            # 基于质量评估的建议
            if quality_score < 80:
                quality_suggestions = quality_assessment.get('详细分析', {}).get('质量建议', [])
                improvement_suggestions.extend(quality_suggestions)
            
            # 基于异常检测的建议
            if anomaly_score > 20:
                improvement_suggestions.append('建议检查和修正异常记录，提高数据准确性')
            
            # 基于消费画像的建议
            profile_tags = []
            for tag_type, tags in consumption_profile.items():
                if isinstance(tags, list) and tag_type.endswith('标签'):
                    profile_tags.extend(tags)
            
            if '数据不足' in profile_tags:
                improvement_suggestions.append('建议增加记账数据，以便进行更准确的消费习惯分析')
            
            return {
                '综合评分': round(comprehensive_score, 2),
                '综合等级': level,
                '评估描述': description,
                '改进建议': improvement_suggestions,
                '评分构成': {
                    '记账质量评分': quality_score,
                    '异常情况评分': 100 - anomaly_score,
                    '权重说明': '记账质量70% + 异常情况30%'
                }
            }
            
        except Exception as e:
            self.logger.error(f"生成综合评估失败: {e}")
            return {
                '综合评分': 0,
                '综合等级': '评估失败',
                '评估描述': f'综合评估失败: {str(e)}',
                '改进建议': [],
                '评分构成': {}
            }

    def _generate_batch_statistics(self, consumption_profiles: Dict,
                                 anomaly_detections: Dict,
                                 quality_assessments: Dict) -> Dict:
        """
        生成批量统计信息

        Args:
            consumption_profiles: 消费画像结果
            anomaly_detections: 异常检测结果
            quality_assessments: 质量评估结果

        Returns:
            批量统计字典
        """
        try:
            # 质量评估统计
            quality_scores = [result.get('总评分', 0) for result in quality_assessments.values()]
            quality_levels = [result.get('质量评估', '') for result in quality_assessments.values()]

            # 异常检测统计
            anomaly_scores = [result.get('异常统计', {}).get('异常评分', 0)
                            for result in anomaly_detections.values()]
            total_anomalies = sum(result.get('异常统计', {}).get('异常记录数', 0)
                                for result in anomaly_detections.values())

            # 消费画像标签统计
            tag_statistics = {}
            for profile in consumption_profiles.values():
                for tag_type, tags in profile.items():
                    if isinstance(tags, list) and tag_type.endswith('标签'):
                        if tag_type not in tag_statistics:
                            tag_statistics[tag_type] = {}
                        for tag in tags:
                            tag_statistics[tag_type][tag] = tag_statistics[tag_type].get(tag, 0) + 1

            # 质量等级分布
            level_distribution = {}
            for level in quality_levels:
                if level:
                    level_distribution[level] = level_distribution.get(level, 0) + 1

            return {
                '质量评估统计': {
                    '平均质量评分': round(sum(quality_scores) / len(quality_scores), 2) if quality_scores else 0,
                    '最高质量评分': max(quality_scores) if quality_scores else 0,
                    '最低质量评分': min(quality_scores) if quality_scores else 0,
                    '质量等级分布': level_distribution
                },
                '异常检测统计': {
                    '平均异常评分': round(sum(anomaly_scores) / len(anomaly_scores), 2) if anomaly_scores else 0,
                    '总异常记录数': total_anomalies,
                    '平均每户异常数': round(total_anomalies / len(anomaly_detections), 2) if anomaly_detections else 0
                },
                '消费画像统计': tag_statistics,
                '总体概况': {
                    '分析户数': len(quality_assessments),
                    '有效数据户数': len([s for s in quality_scores if s > 0]),
                    '数据完整率': len([s for s in quality_scores if s > 0]) / len(quality_assessments) if quality_assessments else 0
                }
            }

        except Exception as e:
            self.logger.error(f"生成批量统计失败: {e}")
            return {'error': f'生成批量统计失败: {str(e)}'}

    def export_report_to_json(self, report: Dict, file_path: str = None) -> str:
        """
        导出报告为JSON文件

        Args:
            report: 报告字典
            file_path: 文件路径（可选）

        Returns:
            JSON字符串或文件路径
        """
        try:
            # 生成JSON字符串
            json_str = json.dumps(report, ensure_ascii=False, indent=2)

            if file_path:
                # 保存到文件
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(json_str)
                self.logger.info(f"报告已导出到文件: {file_path}")
                return file_path
            else:
                # 返回JSON字符串
                return json_str

        except Exception as e:
            self.logger.error(f"导出报告失败: {e}")
            return f"导出失败: {str(e)}"
