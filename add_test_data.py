#!/usr/bin/env python3
"""
为SQLite数据库添加更多测试数据
"""

import sqlite3
import random
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_more_test_data():
    """添加更多测试数据以验证系统功能"""
    
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    try:
        # 添加更多台账记录
        logger.info("添加更多台账测试数据...")
        
        # 获取现有的户代码和编码
        cursor.execute("SELECT `户代码` FROM `调查点户名单`")
        households = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT `帐目编码` FROM `调查品种编码`")
        codes = [row[0] for row in cursor.fetchall()]
        
        # 生成更多台账记录
        test_records = []
        for _ in range(100):  # 添加100条记录
            household = random.choice(households)
            code = random.choice(codes)
            amount = random.randint(1, 10)
            money = round(random.uniform(10, 1000), 2)
            year = random.choice(['2023', '2024'])
            month = f"{random.randint(1, 12):02d}"
            
            # 随机生成日期
            try:
                day = random.randint(1, 28)  # 使用28避免月份天数问题
                date_str = f"{year}-{month}-{day:02d}"
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except:
                date_obj = datetime.now()
            
            # 获取编码对应的类型信息
            cursor.execute("SELECT `收支类别`, `帐目指标名称`, `单位名称` FROM `调查品种编码` WHERE `帐目编码` = ?", [code])
            code_info = cursor.fetchone()
            
            if code_info:
                type_val, type_name, unit_name = code_info
            else:
                type_val, type_name, unit_name = 1, '未知', '元'
            
            test_records.append((
                household,  # hudm
                code,       # code
                amount,     # amount
                money,      # money
                f"测试记录-{random.randint(1000, 9999)}",  # note
                '001',      # person
                year,       # year
                month,      # month
                date_obj,   # date
                type_val,   # type
                type_name,  # type_name
                unit_name,  # unit_name
                '1',        # ybm
                '1',        # ybz
                '1',        # wton
                '0'         # ntow
            ))
        
        # 批量插入
        cursor.executemany("""
            INSERT INTO `调查点台账合并` 
            (hudm, code, amount, money, note, person, year, month, date, type, type_name, unit_name, ybm, ybz, wton, ntow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, test_records)
        
        logger.info(f"成功添加 {len(test_records)} 条台账记录")
        
        # 添加一些未编码的记录（code为NULL）
        uncoded_records = []
        for _ in range(20):  # 添加20条未编码记录
            household = random.choice(households)
            amount = random.randint(1, 5)
            money = round(random.uniform(50, 500), 2)
            year = '2024'
            month = f"{random.randint(1, 12):02d}"
            
            try:
                day = random.randint(1, 28)
                date_str = f"{year}-{month}-{day:02d}"
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            except:
                date_obj = datetime.now()
            
            uncoded_records.append((
                household,  # hudm
                None,       # code (未编码)
                amount,     # amount
                money,      # money
                f"未编码记录-{random.randint(1000, 9999)}",  # note
                '001',      # person
                year,       # year
                month,      # month
                date_obj,   # date
                0,          # type (未知)
                f"待编码项目-{random.randint(1, 100)}",  # type_name
                '元',       # unit_name
                '0',        # ybm (未编码)
                '0',        # ybz (未编码)
                '1',        # wton
                '1'         # ntow
            ))
        
        cursor.executemany("""
            INSERT INTO `调查点台账合并` 
            (hudm, code, amount, money, note, person, year, month, date, type, type_name, unit_name, ybm, ybz, wton, ntow)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, uncoded_records)
        
        logger.info(f"成功添加 {len(uncoded_records)} 条未编码记录")
        
        # 提交事务
        conn.commit()
        
        # 显示统计信息
        cursor.execute("SELECT COUNT(*) FROM `调查点台账合并`")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM `调查点台账合并` WHERE code IS NOT NULL")
        coded_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM `调查点台账合并` WHERE code IS NULL")
        uncoded_records_count = cursor.fetchone()[0]
        
        logger.info(f"数据库统计:")
        logger.info(f"  总记录数: {total_records}")
        logger.info(f"  已编码记录: {coded_records}")
        logger.info(f"  未编码记录: {uncoded_records_count}")
        
        # 按年月统计
        cursor.execute("""
            SELECT year, month, COUNT(*) 
            FROM `调查点台账合并` 
            GROUP BY year, month 
            ORDER BY year, month
        """)
        
        logger.info("按年月分布:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0]}-{row[1]}: {row[2]} 条记录")
        
    except Exception as e:
        logger.error(f"添加测试数据失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    add_more_test_data()
    print("测试数据添加完成！")
