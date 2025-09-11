#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 异常收支记录分析引擎
识别可能存在错误或需要关注的收支记录
"""

import logging
from typing import Dict, List


class AnomalyDetectionEngine:
    """异常收支记录分析引擎"""
    
    def __init__(self, dal):
        """
        初始化异常检测引擎
        
        Args:
            dal: 数据访问层对象
        """
        self.dal = dal
        self.logger = logging.getLogger(__name__)
    
    def detect_household_anomalies(self, household_code: str,
                                 start_year: str = None, start_month: str = None,
                                 end_year: str = None, end_month: str = None) -> Dict:
        """
        检测户异常记录
        
        Args:
            household_code: 户代码
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份
            
        Returns:
            异常检测结果字典
        """
        try:
            # 获取户收支数据
            income_expense_data = self.dal.get_household_income_expense_data(
                household_code, start_year, start_month, end_year, end_month
            )
            
            if not income_expense_data:
                return {'户代码': household_code, '异常记录': [], '异常统计': {}}
            
            # 获取月度汇总数据
            monthly_summary = self.dal.get_household_monthly_summary(
                household_code, start_year, start_month, end_year, end_month
            )
            
            # 获取统计基准数据
            benchmarks = self.dal.get_statistical_benchmarks('all')
            
            anomalies = []
            
            # 1. 单笔金额异常检测
            amount_anomalies = self._detect_amount_anomalies(income_expense_data, benchmarks)
            anomalies.extend(amount_anomalies)
            
            # 2. 收支类别异常检测
            category_anomalies = self._detect_category_anomalies(income_expense_data, benchmarks)
            anomalies.extend(category_anomalies)
            
            # 3. 收支平衡异常检测
            balance_anomalies = self._detect_balance_anomalies(monthly_summary)
            anomalies.extend(balance_anomalies)
            
            # 4. 记账模式异常检测
            pattern_anomalies = self._detect_pattern_anomalies(income_expense_data)
            anomalies.extend(pattern_anomalies)
            
            # 统计异常情况
            anomaly_stats = self._calculate_anomaly_statistics(anomalies, income_expense_data)
            
            # 为Word报告生成异常详情
            anomaly_details = self._generate_anomaly_details(anomalies)

            return {
                '户代码': household_code,
                '分析时间范围': {
                    '开始': f"{start_year}-{start_month}" if start_year and start_month else None,
                    '结束': f"{end_year}-{end_month}" if end_year and end_month else None
                },
                '异常记录': anomalies,
                '异常统计': anomaly_stats,
                '异常详情': anomaly_details  # 为Word报告提供的格式化数据
            }
            
        except Exception as e:
            self.logger.error(f"检测户异常记录失败: {household_code}, 错误: {e}")
            return {'户代码': household_code, '异常记录': [], '异常统计': {}}
    
    def _detect_amount_anomalies(self, data: List[Dict], benchmarks: Dict) -> List[Dict]:
        """
        检测单笔金额异常
        
        Args:
            data: 收支数据
            benchmarks: 统计基准数据
            
        Returns:
            金额异常记录列表
        """
        anomalies = []
        
        for record in data:
            amount = record['金额']
            code = record['编码']
            income_type = record['收支类型']
            
            if not code or amount <= 0:
                continue
            
            # 获取对应的基准数据
            benchmark_key = f"{code[:2]}_{income_type}"
            benchmark = benchmarks.get(benchmark_key)
            
            if not benchmark:
                continue
            
            # 使用标准差方法检测异常（替代IQR方法）
            mean = benchmark['平均金额']
            std = benchmark['标准差']

            # 2倍标准差作为异常边界
            lower_bound = mean - 2 * std
            upper_bound = mean + 2 * std

            # 极端异常检测（3倍标准差）
            extreme_lower = mean - 3 * std
            extreme_upper = mean + 3 * std
            
            anomaly_type = None
            severity = None
            
            if amount < extreme_lower or amount > extreme_upper:
                anomaly_type = '极端金额异常'
                severity = '高'
            elif amount < lower_bound or amount > upper_bound:
                anomaly_type = '金额异常'
                severity = '中'
            
            if anomaly_type:
                anomalies.append({
                    '记录ID': record['id'],
                    '异常类型': anomaly_type,
                    '严重程度': severity,
                    '异常描述': f"金额{amount}元超出正常范围[{lower_bound:.2f}, {upper_bound:.2f}]",
                    '记录详情': {
                        '日期': record['日期'],
                        '项目名称': record['项目名称'],
                        '金额': amount,
                        '编码': code,
                        '收支类型': '收入' if income_type == 1 else '支出'
                    },
                    '基准信息': {
                        '平均值': mean,
                        '标准差': std,
                        '最小值': benchmark['最小金额'],
                        '最大值': benchmark['最大金额']
                    }
                })
        
        return anomalies
    
    def _detect_category_anomalies(self, data: List[Dict], benchmarks: Dict) -> List[Dict]:
        """
        检测收支类别异常
        
        Args:
            data: 收支数据
            benchmarks: 统计基准数据
            
        Returns:
            类别异常记录列表
        """
        anomalies = []
        
        # 统计各类别的记录数
        category_counts = {}
        for record in data:
            code = record['编码']
            if code:
                prefix = code[:2]
                category_counts[prefix] = category_counts.get(prefix, 0) + 1
        
        # 检测罕见类别
        for record in data:
            code = record['编码']
            if not code:
                continue
            
            prefix = code[:2]
            
            # 检查是否为罕见消费类别
            benchmark_key = f"{prefix}_{record['收支类型']}"
            benchmark = benchmarks.get(benchmark_key)
            
            if benchmark and benchmark['记录数'] < 50:  # 基准数据中记录数少于50的视为罕见类别
                anomalies.append({
                    '记录ID': record['id'],
                    '异常类型': '罕见类别',
                    '严重程度': '低',
                    '异常描述': f"类别{prefix}在统计中较为罕见（仅{benchmark['记录数']}条记录）",
                    '记录详情': {
                        '日期': record['日期'],
                        '项目名称': record['项目名称'],
                        '金额': record['金额'],
                        '编码': code,
                        '收支类型': '收入' if record['收支类型'] == 1 else '支出'
                    }
                })
        
        return anomalies
    
    def _detect_balance_anomalies(self, monthly_summary: List[Dict]) -> List[Dict]:
        """
        检测收支平衡异常
        
        Args:
            monthly_summary: 月度汇总数据
            
        Returns:
            收支平衡异常记录列表
        """
        anomalies = []
        
        for month_data in monthly_summary:
            income = month_data['收入总额']
            expense = month_data['支出总额']
            balance = month_data['收支差额']
            
            # 检测收支倒挂（支出远大于收入）
            if income > 0 and expense / income > 2:
                anomalies.append({
                    '记录ID': f"{month_data['年份']}-{month_data['月份']}",
                    '异常类型': '收支严重倒挂',
                    '严重程度': '高',
                    '异常描述': f"支出({expense:.2f})是收入({income:.2f})的{expense/income:.1f}倍",
                    '记录详情': {
                        '年月': f"{month_data['年份']}-{month_data['月份']}",
                        '收入': income,
                        '支出': expense,
                        '差额': balance
                    }
                })
            elif income > 0 and expense / income > 1.5:
                anomalies.append({
                    '记录ID': f"{month_data['年份']}-{month_data['月份']}",
                    '异常类型': '收支倒挂',
                    '严重程度': '中',
                    '异常描述': f"支出({expense:.2f})超过收入({income:.2f})",
                    '记录详情': {
                        '年月': f"{month_data['年份']}-{month_data['月份']}",
                        '收入': income,
                        '支出': expense,
                        '差额': balance
                    }
                })
            
            # 检测零收入但有支出
            if income == 0 and expense > 0:
                anomalies.append({
                    '记录ID': f"{month_data['年份']}-{month_data['月份']}",
                    '异常类型': '零收入有支出',
                    '严重程度': '中',
                    '异常描述': f"无收入记录但有支出{expense:.2f}元",
                    '记录详情': {
                        '年月': f"{month_data['年份']}-{month_data['月份']}",
                        '收入': income,
                        '支出': expense,
                        '差额': balance
                    }
                })
        
        return anomalies

    def _detect_pattern_anomalies(self, data: List[Dict]) -> List[Dict]:
        """
        检测记账模式异常

        Args:
            data: 收支数据

        Returns:
            记账模式异常记录列表
        """
        anomalies = []

        if not data:
            return anomalies

        # 统计整数金额比例
        total_records = len(data)
        integer_amounts = sum(1 for record in data if record['金额'] == int(record['金额']))
        integer_ratio = integer_amounts / total_records

        # 整数金额过多异常
        if integer_ratio > 0.8:
            anomalies.append({
                '记录ID': 'PATTERN_001',
                '异常类型': '整数金额过多',
                '严重程度': '中',
                '异常描述': f"整数金额占比{integer_ratio:.1%}，可能存在记账不精确问题",
                '记录详情': {
                    '总记录数': total_records,
                    '整数金额数': integer_amounts,
                    '整数金额比例': integer_ratio
                }
            })

        # 零钱记录缺失检测
        small_amounts = sum(1 for record in data if 0 < record['金额'] < 10)
        small_ratio = small_amounts / total_records

        if small_ratio < 0.05 and total_records > 50:
            anomalies.append({
                '记录ID': 'PATTERN_002',
                '异常类型': '零钱记录缺失',
                '严重程度': '低',
                '异常描述': f"小额支出(10元以下)占比仅{small_ratio:.1%}，可能遗漏日常小额消费",
                '记录详情': {
                    '总记录数': total_records,
                    '小额记录数': small_amounts,
                    '小额记录比例': small_ratio
                }
            })

        # 检测重复记录
        duplicate_groups = {}
        for record in data:
            key = (record['日期'], record['金额'], record['项目名称'])
            if key not in duplicate_groups:
                duplicate_groups[key] = []
            duplicate_groups[key].append(record)

        for key, group in duplicate_groups.items():
            if len(group) > 1:
                anomalies.append({
                    '记录ID': f"DUP_{group[0]['id']}",
                    '异常类型': '疑似重复记录',
                    '严重程度': '中',
                    '异常描述': f"发现{len(group)}条相同的记录：{key[0]} {key[2]} {key[1]}元",
                    '记录详情': {
                        '重复记录数': len(group),
                        '记录IDs': [r['id'] for r in group],
                        '日期': key[0],
                        '金额': key[1],
                        '项目名称': key[2]
                    }
                })

        # 检测异常高频记账
        date_counts = {}
        for record in data:
            date = record['日期']
            date_counts[date] = date_counts.get(date, 0) + 1

        high_freq_dates = [(date, count) for date, count in date_counts.items() if count > 20]
        for date, count in high_freq_dates:
            anomalies.append({
                '记录ID': f"FREQ_{date}",
                '异常类型': '单日记账过多',
                '严重程度': '低',
                '异常描述': f"{date}当日记账{count}笔，可能存在集中补记情况",
                '记录详情': {
                    '日期': date,
                    '记账笔数': count
                }
            })

        return anomalies

    def _calculate_anomaly_statistics(self, anomalies: List[Dict], data: List[Dict]) -> Dict:
        """
        计算异常统计信息

        Args:
            anomalies: 异常记录列表
            data: 原始数据

        Returns:
            异常统计字典
        """
        total_records = len(data)
        total_anomalies = len(anomalies)

        # 按类型统计异常
        type_stats = {}
        severity_stats = {'高': 0, '中': 0, '低': 0}

        for anomaly in anomalies:
            anomaly_type = anomaly['异常类型']
            severity = anomaly['严重程度']

            type_stats[anomaly_type] = type_stats.get(anomaly_type, 0) + 1
            severity_stats[severity] += 1

        return {
            '总记录数': total_records,
            '异常记录数': total_anomalies,
            '异常比例': total_anomalies / max(total_records, 1),
            '按类型统计': type_stats,
            '按严重程度统计': severity_stats,
            '异常评分': self._calculate_anomaly_score(severity_stats, total_records)
        }

    def _calculate_anomaly_score(self, severity_stats: Dict, total_records: int) -> float:
        """
        计算异常评分（0-100，分数越高表示异常越多）

        Args:
            severity_stats: 严重程度统计
            total_records: 总记录数

        Returns:
            异常评分
        """
        if total_records == 0:
            return 0

        # 权重：高严重程度权重更大
        weights = {'高': 10, '中': 5, '低': 2}
        weighted_score = sum(severity_stats[level] * weights[level] for level in weights)

        # 归一化到0-100
        max_possible_score = total_records * weights['高']
        score = min(100, (weighted_score / max_possible_score) * 100) if max_possible_score > 0 else 0

        return round(score, 2)

    def detect_batch_anomalies(self, household_codes: List[str],
                             start_year: str = None, start_month: str = None,
                             end_year: str = None, end_month: str = None) -> Dict[str, Dict]:
        """
        批量检测户异常记录

        Args:
            household_codes: 户代码列表
            start_year: 开始年份
            start_month: 开始月份
            end_year: 结束年份
            end_month: 结束月份

        Returns:
            户代码到异常检测结果的映射字典
        """
        results = {}

        for household_code in household_codes:
            try:
                result = self.detect_household_anomalies(
                    household_code, start_year, start_month, end_year, end_month
                )
                if result:
                    results[household_code] = result

            except Exception as e:
                self.logger.error(f"检测户 {household_code} 异常失败: {e}")
                continue

        return results

    def _generate_anomaly_details(self, anomalies: List[Dict]) -> List[Dict]:
        """
        生成异常详情列表，用于Word报告

        Args:
            anomalies: 异常记录列表

        Returns:
            格式化的异常详情列表
        """
        details = []

        for anomaly in anomalies:
            record_detail = anomaly.get('记录详情', {})
            detail = {
                '日期': record_detail.get('日期', 'N/A'),
                '类型': record_detail.get('收支类型', 'N/A'),
                '项目名称': record_detail.get('项目名称', 'N/A'),
                '金额': record_detail.get('金额', 0),
                '异常类型': anomaly.get('异常类型', 'N/A'),
                '严重程度': anomaly.get('严重程度', 'N/A'),
                '异常原因': anomaly.get('异常描述', 'N/A'),
                '编码': record_detail.get('编码', 'N/A')
            }
            details.append(detail)

        # 按严重程度和金额排序
        severity_order = {'高': 3, '中': 2, '低': 1}
        details.sort(key=lambda x: (
            severity_order.get(x['严重程度'], 0),
            x['金额']
        ), reverse=True)

        return details
