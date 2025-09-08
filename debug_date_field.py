#!/usr/bin/env python3
"""
调试date字段的实际内容，找出记账天数计算错误的原因
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import Database
import calendar
from datetime import datetime
import json

def debug_date_field(household_code="321283001002012", start_year="2024", start_month="12", end_year="2025", end_month="8"):
    """调试date字段的实际内容"""
    
    db = Database()
    
    print(f"=== 调试户代码 {household_code} 的date字段 ===")
    print(f"时间范围: {start_year}年{start_month}月 至 {end_year}年{end_month}月")
    print()
    
    # 1. 查看date字段的实际内容和类型
    print("1. 查看date字段的实际内容...")
    
    date_debug_sql = """
    SELECT TOP 20
        t.year,
        t.month,
        t.date,
        CAST(t.date AS VARCHAR(50)) AS date_string,
        CONVERT(VARCHAR(10), t.date, 120) AS date_only,
        CONVERT(VARCHAR(19), t.date, 120) AS datetime_full,
        t.money,
        t.type_name
    FROM 调查点台账合并 t
    WHERE t.hudm = ?
        AND ((t.year > ? OR (t.year = ? AND t.month >= ?))
        AND (t.year < ? OR (t.year = ? AND t.month <= ?)))
    ORDER BY t.date
    """
    
    params = [
        household_code,
        start_year, start_year, start_month,
        end_year, end_year, end_month
    ]
    
    date_result = db.execute_query_safe(date_debug_sql, params)
    
    print("前20条记录的date字段内容:")
    print("年份 | 月份 | date原始值 | date字符串 | 仅日期 | 完整日期时间 | 金额 | 项目")
    print("-" * 100)
    
    for row in date_result:
        print(f"{row[0]} | {row[1]:>2} | {str(row[2])[:19]:>19} | {row[3][:19]:>19} | {row[4]:>10} | {row[5]:>19} | {row[6]:>8} | {row[7][:10]}")
    
    print()
    
    # 2. 统计不同的日期值
    print("2. 统计不同的日期值...")
    
    distinct_dates_sql = """
    SELECT
        CONVERT(VARCHAR(10), t.date, 120) AS date_only,
        COUNT(*) AS record_count,
        MIN(CONVERT(VARCHAR(19), t.date, 120)) AS earliest_time,
        MAX(CONVERT(VARCHAR(19), t.date, 120)) AS latest_time
    FROM 调查点台账合并 t
    WHERE t.hudm = ?
        AND ((t.year > ? OR (t.year = ? AND t.month >= ?))
        AND (t.year < ? OR (t.year = ? AND t.month <= ?)))
    GROUP BY CONVERT(VARCHAR(10), t.date, 120)
    ORDER BY date_only
    """
    
    distinct_result = db.execute_query_safe(distinct_dates_sql, params)
    
    print(f"不同日期的统计（共{len(distinct_result)}个不同日期）:")
    print("日期       | 记录数 | 最早时间            | 最晚时间")
    print("-" * 60)
    
    for i, row in enumerate(distinct_result):
        if i < 10:  # 只显示前10个日期
            print(f"{row[0]} | {row[1]:>6} | {row[2]} | {row[3]}")
        elif i == 10:
            print("...")
            break
    
    if len(distinct_result) > 10:
        # 显示最后几个日期
        for row in distinct_result[-3:]:
            print(f"{row[0]} | {row[1]:>6} | {row[2]} | {row[3]}")
    
    print()
    
    # 3. 对比不同的COUNT方式
    print("3. 对比不同的COUNT方式...")
    
    count_comparison_sql = """
    SELECT
        COUNT(*) AS 总记录数,
        COUNT(DISTINCT t.date) AS 按完整datetime计数,
        COUNT(DISTINCT CONVERT(VARCHAR(10), t.date, 120)) AS 按日期部分计数,
        COUNT(DISTINCT CONVERT(VARCHAR(13), t.date, 120)) AS 按年月日时计数,
        MIN(t.date) AS 最早记录,
        MAX(t.date) AS 最晚记录
    FROM 调查点台账合并 t
    WHERE t.hudm = ?
        AND ((t.year > ? OR (t.year = ? AND t.month >= ?))
        AND (t.year < ? OR (t.year = ? AND t.month <= ?)))
    """
    
    count_result = db.execute_query_safe(count_comparison_sql, params)
    
    if count_result:
        row = count_result[0]
        print("不同计数方式的结果:")
        print(f"总记录数: {row[0]}")
        print(f"按完整datetime计数 (COUNT(DISTINCT t.date)): {row[1]}")
        print(f"按日期部分计数 (COUNT(DISTINCT CONVERT(..., 120))): {row[2]}")
        print(f"按年月日时计数: {row[3]}")
        print(f"最早记录: {row[4]}")
        print(f"最晚记录: {row[5]}")
        print()
        
        print("=== 问题分析 ===")
        if row[1] > row[2]:
            print(f"❌ 发现问题：按完整datetime计数({row[1]})大于按日期部分计数({row[2]})")
            print("   这说明同一天内有多个不同的时间点被计算为不同的记账天数")
            print("   解决方案：应该使用 COUNT(DISTINCT CONVERT(VARCHAR(10), t.date, 120)) 来计算记账天数")
        else:
            print("✅ 计数方式正常")
    
    # 4. 计算时间范围总天数
    print("4. 计算时间范围总天数...")
    
    start_date = datetime(int(start_year), int(start_month), 1)
    end_year_int, end_month_int = int(end_year), int(end_month)
    last_day = calendar.monthrange(end_year_int, end_month_int)[1]
    end_date = datetime(end_year_int, end_month_int, last_day)
    total_days = (end_date - start_date).days + 1
    
    print(f"开始日期: {start_date.strftime('%Y年%m月%d日')}")
    print(f"结束日期: {end_date.strftime('%Y年%m月%d日')}")
    print(f"总天数: {total_days}")
    
    if count_result:
        correct_recording_days = count_result[0][2]  # 按日期部分计数
        missing_days = max(0, total_days - correct_recording_days)
        
        print()
        print("=== 修正后的计算结果 ===")
        print(f"正确的记账天数: {correct_recording_days}")
        print(f"漏记账天数: {missing_days}")
        print(f"记账覆盖率: {(correct_recording_days / total_days * 100):.1f}%")

if __name__ == "__main__":
    debug_date_field()
