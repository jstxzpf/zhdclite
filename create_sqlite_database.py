#!/usr/bin/env python3
"""
创建SQLite数据库和表结构
基于现有SQL Server数据库结构分析结果
"""

import sqlite3
import os
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database():
    """创建SQLite数据库文件和所有必需的表"""

    # 数据库文件路径
    db_path = 'database.db'

    # 如果数据库文件已存在，先备份
    if os.path.exists(db_path):
        backup_path = f'database_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
        os.rename(db_path, backup_path)
        logger.info(f"已备份现有数据库到: {backup_path}")

    # 创建新数据库连接
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # 启用外键约束
        cursor.execute("PRAGMA foreign_keys = ON")

        # 1. 创建调查点户名单表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 调查点户名单 (
            户代码 TEXT PRIMARY KEY,
            户主姓名 TEXT NOT NULL,
            人数 INTEGER DEFAULT 1,
            所在乡镇街道 TEXT,
            村居名称 TEXT,
            密码 TEXT,
            调查小区名称 TEXT,
            城乡属性 TEXT,
            住宅地址 TEXT,
            家庭人口 REAL,
            是否退出 REAL,
            创建时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            更新时间 DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        logger.info("创建表: 调查点户名单")

        # 2. 创建调查品种编码表
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 调查品种编码 (
            帐目编码 TEXT PRIMARY KEY,
            帐目指标名称 TEXT NOT NULL,
            单位名称 TEXT,
            收支类别 INTEGER,
            录入控制码 TEXT,
            下限 REAL,
            上限 REAL,
            计量单位代码 TEXT,
            折算系数 REAL,
            创建时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            更新时间 DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        logger.info("创建表: 调查品种编码")

        # 3. 创建调查点台账合并表（主要数据表）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 调查点台账合并 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hudm TEXT NOT NULL,
            code TEXT,
            amount REAL DEFAULT 0,
            money REAL DEFAULT 0,
            note TEXT,
            person TEXT,
            year TEXT NOT NULL,
            month TEXT NOT NULL,
            z_guid TEXT,
            date DATETIME,
            type INTEGER DEFAULT 0,
            type_name TEXT,
            unit_name TEXT,
            ybm TEXT DEFAULT '',
            ybz TEXT DEFAULT '',
            wton TEXT DEFAULT '',
            ntow TEXT DEFAULT '',
            创建时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            更新时间 DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (hudm) REFERENCES 调查点户名单(户代码),
            FOREIGN KEY (code) REFERENCES 调查品种编码(帐目编码)
        )
        """)
        logger.info("创建表: 调查点台账合并")

        # 4. 创建调查点村名单表与视图（直接基于该表创建视图）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 调查点村名单 (
            户代码前12位 TEXT,
            数量 REAL,
            调查点类型 TEXT,
            所在乡镇街道 TEXT,
            村居名称 TEXT,
            调查员姓名 TEXT,
            调查员电话 TEXT,
            城乡属性 TEXT
        )
        """)
        logger.info("创建表: 调查点村名单")

        cursor.execute("""
        CREATE VIEW IF NOT EXISTS v_town_village_list AS
        SELECT DISTINCT 户代码前12位 AS 村代码, 所在乡镇街道, 村居名称
        FROM 调查点村名单
        WHERE 所在乡镇街道 IS NOT NULL AND 村居名称 IS NOT NULL
        """)
        logger.info("创建视图: v_town_village_list")

        # 5. 创建临时表：已经编码完成
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 已经编码完成 (
            户代码 TEXT,
            户主姓名 TEXT,
            type_name TEXT,
            数量 TEXT,
            日期 TEXT,
            金额 TEXT,
            备注 TEXT,
            收支 TEXT,
            id INTEGER NOT NULL,
            code TEXT,
            年度 TEXT,
            月份 TEXT,
            创建时间 DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        logger.info("创建表: 已经编码完成")

        # 6. 创建临时表：国家点待导入
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS 国家点待导入 (
            SID TEXT,
            县码 TEXT,
            样本编码 TEXT,
            年 TEXT,
            月 TEXT,
            页码 TEXT,
            行码 TEXT,
            编码 TEXT,
            数量 REAL,
            金额 REAL,
            品名 TEXT,
            人码 TEXT,
            人代码 TEXT,
            记账说明 TEXT,
            创建时间 TEXT,
            数量2 REAL,
            是否网购 TEXT,
            记账方式 TEXT,
            问题类型 TEXT,
            记账审核说明 TEXT,
            记账日期 TEXT,
            更新时间 TEXT,
            账页生成设备标识 TEXT,
            导入时间 DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        logger.info("创建表: 国家点待导入")

        # 创建索引以提高查询性能
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_台账_户代码 ON 调查点台账合并(hudm)",
            "CREATE INDEX IF NOT EXISTS idx_台账_年月 ON 调查点台账合并(year, month)",
            "CREATE INDEX IF NOT EXISTS idx_台账_编码 ON 调查点台账合并(code)",
            "CREATE INDEX IF NOT EXISTS idx_台账_类型 ON 调查点台账合并(type)",
            "CREATE INDEX IF NOT EXISTS idx_台账_日期 ON 调查点台账合并(date)",
            "CREATE INDEX IF NOT EXISTS idx_台账_id ON 调查点台账合并(id)",
            "CREATE INDEX IF NOT EXISTS idx_户名单_乡镇 ON 调查点户名单(所在乡镇街道)",

        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        logger.info("创建索引完成")

        # 提交事务
        conn.commit()
        logger.info(f"SQLite数据库创建成功: {db_path}")

        # 插入一些示例数据
        insert_sample_data(cursor)
        conn.commit()

    except Exception as e:
        logger.error(f"创建数据库失败: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def insert_sample_data(cursor):
    """插入示例测试数据"""
    logger.info("开始插入示例数据...")

    # 插入示例乡镇村庄数据
    sample_villages = [
        ('城关镇', '城关社区', '110101001001'),
        ('城关镇', '东街村', '110101001002'),
        ('城关镇', '西街村', '110101001003'),
        ('新华镇', '新华村', '110101002001'),
        ('新华镇', '光明村', '110101002002'),
        ('新华镇', '团结村', '110101002003'),
        ('永安镇', '永安村', '110101003001'),
        ('永安镇', '和谐村', '110101003002'),
    ]

    cursor.executemany("""
        INSERT OR IGNORE INTO v_town_village_list_base (所在乡镇街道, 村居名称, 村代码)
        VALUES (?, ?, ?)
    """, sample_villages)

    # 插入示例户名单数据
    sample_households = [
        ('110101001001001', '张三', 4, '城关镇', '城关社区'),
        ('110101001001002', '李四', 3, '城关镇', '城关社区'),
        ('110101001002001', '王五', 5, '城关镇', '东街村'),
        ('110101001002002', '赵六', 2, '城关镇', '东街村'),
        ('110101002001001', '钱七', 4, '新华镇', '新华村'),
        ('110101002001002', '孙八', 3, '新华镇', '新华村'),
        ('110101003001001', '周九', 6, '永安镇', '永安村'),
        ('110101003001002', '吴十', 2, '永安镇', '永安村'),
    ]

    cursor.executemany("""
        INSERT OR IGNORE INTO 调查点户名单 (户代码, 户主姓名, 人数, 所在乡镇街道, 村居名称)
        VALUES (?, ?, ?, ?, ?)
    """, sample_households)

    # 插入示例编码数据
    sample_codes = [
        ('110101', '工资性收入', '元', 1),
        ('110201', '经营净收入', '元', 1),
        ('110301', '财产净收入', '元', 1),
        ('110401', '转移净收入', '元', 1),
        ('310101', '粮食', '元', 2),
        ('310201', '蔬菜', '元', 2),
        ('310301', '肉类', '元', 2),
        ('320101', '服装', '元', 2),
        ('330101', '住房', '元', 2),
        ('340101', '交通', '元', 2),
    ]

    cursor.executemany("""
        INSERT OR IGNORE INTO 调查品种编码 (帐目编码, 帐目指标名称, 单位名称, 收支类别)
        VALUES (?, ?, ?, ?)
    """, sample_codes)

    # 插入示例台账数据
    sample_records = [
        ('110101001001001', '110101', 1, 5000.0, '工资收入', '001', '2024', '01', datetime.now(), 1, '工资性收入', '元'),
        ('110101001001001', '310101', 1, 200.0, '购买大米', '001', '2024', '01', datetime.now(), 2, '粮食', '元'),
        ('110101001001002', '110101', 1, 4500.0, '工资收入', '001', '2024', '01', datetime.now(), 1, '工资性收入', '元'),
        ('110101001001002', '310201', 1, 150.0, '购买蔬菜', '001', '2024', '01', datetime.now(), 2, '蔬菜', '元'),
        ('110101001002001', '110201', 1, 3000.0, '经营收入', '001', '2024', '01', datetime.now(), 1, '经营净收入', '元'),
        ('110101001002001', '310301', 1, 300.0, '购买肉类', '001', '2024', '01', datetime.now(), 2, '肉类', '元'),
    ]

    cursor.executemany("""
        INSERT OR IGNORE INTO 调查点台账合并
        (hudm, code, amount, money, note, person, year, month, date, type, type_name, unit_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, sample_records)

    logger.info("示例数据插入完成")

if __name__ == "__main__":
    create_database()
    print("SQLite数据库创建完成！")
