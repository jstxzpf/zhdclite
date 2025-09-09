#!/usr/bin/env python3
"""
为现有 SQLite 数据库补充下列表的 MSSQL 缺失列（仅增列，保留现有约束）：
- 调查品种编码：录入控制码、下限、上限、计量单位代码、折算系数
- 国家点待导入：数量2、是否网购、记账方式、问题类型、记账审核说明、记账日期、更新时间、账页生成设备标识
"""
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'database.db'

TASKS = [
    {
        'table': '调查品种编码',
        'columns': [
            ('录入控制码', 'TEXT'),
            ('下限', 'REAL'),
            ('上限', 'REAL'),
            ('计量单位代码', 'TEXT'),
            ('折算系数', 'REAL'),
        ]
    },
    {
        'table': '国家点待导入',
        'columns': [
            ('数量2', 'REAL'),
            ('是否网购', 'TEXT'),
            ('记账方式', 'TEXT'),
            ('问题类型', 'TEXT'),
            ('记账审核说明', 'TEXT'),
            ('记账日期', 'TEXT'),
            ('更新时间', 'TEXT'),
            ('账页生成设备标识', 'TEXT'),
        ]
    }
]

def column_exists(cur, table, col):
    info = cur.execute(f"PRAGMA table_info('{table}')").fetchall()
    cols = {r[1] for r in info}
    return col in cols


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        for task in TASKS:
            table = task['table']
            logger.info('检查表: %s', table)
            before = cur.execute(f"PRAGMA table_info('{table}')").fetchall()
            logger.info('增列前: %s', before)
            for col, ctype in task['columns']:
                if not column_exists(cur, table, col):
                    sql = f"ALTER TABLE {table} ADD COLUMN {col} {ctype}"
                    logger.info('执行: %s', sql)
                    cur.execute(sql)
                else:
                    logger.info('已存在列，跳过: %s', col)
            after = cur.execute(f"PRAGMA table_info('{table}')").fetchall()
            logger.info('增列后: %s', after)
        conn.commit()
        print('增列完成')
    except Exception as e:
        conn.rollback()
        logger.error('增列失败: %s', e)
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()

