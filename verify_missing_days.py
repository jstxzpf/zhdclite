#!/usr/bin/env python3
"""
验证漏记账功能的汇总指标准确性
使用户代码321283001002012为例进行详细分析
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.database import Database
import calendar
from datetime import datetime, timedelta
import json

def verify_household_data(household_code="321283001002012", start_year="2024", start_month="12", end_year="2025", end_month="8"):
    """验证指定户代码的漏记账数据准确性"""
    
    db = Database()
    
    print(f"=== 验证户代码 {household_code} 的漏记账数据 ===")
    print(f"时间范围: {start_year}-{start_month} 至 {end_year}-{end_month}")
    print()
    
    # 1. 首先检查户代码是否存在
    print("1. 检查户代码是否存在...")
    household_sql = """
    SELECT 户代码, 户主姓名 
    FROM 调查点户名单 
    WHERE 户代码 = ?
    """
    household_result = db.execute_query_safe(household_sql, [household_code])
    
    if not household_result:
        print(f"❌ 户代码 {household_code} 不存在于调查点户名单表中")
        return
    
    household_name = household_result[0][1]
    print(f"✅ 户代码存在: {household_code} - {household_name}")
    print()
    
    # 2. 查询该户在指定时间范围内的所有记账数据
    print("2. 查询记账数据...")
    
    # 构建时间筛选条件
    time_conditions = []
    params = [household_code]
    
    # 开始时间条件
    time_conditions.append("(t.year > ? OR (t.year = ? AND t.month >= ?))")
    params.extend([start_year, start_year, start_month])
    
    # 结束时间条件  
    time_conditions.append("(t.year < ? OR (t.year = ? AND t.month <= ?))")
    params.extend([end_year, end_year, end_month])
    
    time_clause = " AND " + " AND ".join(time_conditions)
    
    records_sql = f"""
    SELECT 
        t.year AS 年份,
        t.month AS 月份,
        t.date AS 日期,
        t.type AS 收支类型,
        t.money AS 金额,
        t.type_name AS 项目名称,
        t.code AS 编码
    FROM 调查点台账合并 t
    WHERE t.hudm = ? {time_clause}
    ORDER BY t.year, t.month, t.date
    """
    
    records_result = db.execute_query_safe(records_sql, params)
    
    print(f"总记账笔数: {len(records_result)}")
    
    if records_result:
        print("前5条记录:")
        for i, record in enumerate(records_result[:5]):
            print(f"  {i+1}. {record[0]}-{record[1]:02d}-{record[2]} | 类型:{record[3]} | 金额:{record[4]} | 项目:{record[5]} | 编码:{record[6]}")
        print()
    
    # 3. 按月统计记账情况
    print("3. 按月统计记账情况...")
    
    monthly_sql = f"""
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
    
    monthly_result = db.execute_query_safe(monthly_sql, params)
    
    total_recording_days = 0
    total_missing_days = 0
    total_records = 0
    first_date = None
    last_date = None
    
    print("月度统计:")
    print("年月     | 记账笔数 | 记账天数 | 月天数 | 漏记账天数 | 首次记账 | 最后记账")
    print("-" * 80)
    
    for row in monthly_result:
        year, month = int(row[0]), int(row[1])
        records_count = row[2]
        recording_days = row[9]
        first_record_date = row[10]
        last_record_date = row[11]
        
        # 计算该月总天数
        days_in_month = calendar.monthrange(year, month)[1]
        
        # 计算漏记账天数
        missing_days = days_in_month - recording_days
        
        total_recording_days += recording_days
        total_missing_days += missing_days
        total_records += records_count
        
        if first_date is None or (first_record_date and first_record_date < first_date):
            first_date = first_record_date
        if last_date is None or (last_record_date and last_record_date > last_date):
            last_date = last_record_date
        
        print(f"{year}-{month:02d} | {records_count:8d} | {recording_days:8d} | {days_in_month:6d} | {missing_days:10d} | {first_record_date or 'N/A':10s} | {last_record_date or 'N/A'}")
    
    print("-" * 80)
    print(f"总计     | {total_records:8d} | {total_recording_days:8d} | {'':6s} | {total_missing_days:10d} | {first_date or 'N/A':10s} | {last_date or 'N/A'}")
    print()
    
    # 4. 计算时间范围内的总天数
    print("4. 计算时间范围总天数...")
    
    start_date = datetime(int(start_year), int(start_month), 1)
    
    # 计算结束日期（该月最后一天）
    end_year_int, end_month_int = int(end_year), int(end_month)
    last_day = calendar.monthrange(end_year_int, end_month_int)[1]
    end_date = datetime(end_year_int, end_month_int, last_day)
    
    total_days = (end_date - start_date).days + 1
    
    print(f"开始日期: {start_date.strftime('%Y年%m月%d日')}")
    print(f"结束日期: {end_date.strftime('%Y年%m月%d日')}")
    print(f"总天数: {total_days}")
    print()
    
    # 5. 汇总结果对比
    print("5. 汇总结果对比...")
    print("指标                | 计算值")
    print("-" * 40)
    print(f"户代码              | {household_code}")
    print(f"户主姓名            | {household_name}")
    print(f"总记账笔数          | {total_records}")
    print(f"记账天数            | {total_recording_days}")
    print(f"时间范围总天数      | {total_days}")
    print(f"漏记账天数          | {total_missing_days}")
    print(f"首次记账日期        | {first_date or 'N/A'}")
    print(f"最后记账日期        | {last_date or 'N/A'}")
    print()
    
    # 6. 验证当前API返回的数据
    print("6. 当前API返回的数据问题...")
    print("❌ 当前get_missing_days_statistics_range()函数存在以下问题:")
    print("   - 记账天数固定返回0（应该是实际记账天数）")
    print("   - 漏记账天数固定返回总天数（应该是总天数-记账天数）")
    print("   - 总记账笔数固定返回0（应该是实际记账笔数）")
    print("   - 首次/最后记账日期固定返回None（应该是实际日期）")
    print()
    
    return {
        '户代码': household_code,
        '户主姓名': household_name,
        '记账天数': total_recording_days,
        '漏记账天数': total_missing_days,
        '总记账笔数': total_records,
        '首次记账日期': first_date,
        '最后记账日期': last_date,
        '时间范围总天数': total_days
    }

if __name__ == "__main__":
    result = verify_household_data()
    
    print("=== 验证完成 ===")
    print("建议修复方案:")
    print("1. 修改get_missing_days_statistics_range()函数，使用真实的SQL查询")
    print("2. 正确计算记账天数、漏记账天数、记账笔数等指标")
    print("3. 返回实际的首次和最后记账日期")
