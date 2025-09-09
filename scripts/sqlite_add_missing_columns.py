#!/usr/bin/env python3
"""
为现有 SQLite 数据库的 调查点户名单 表补充 MSSQL 缺失列（仅增列，保留现有约束）
"""
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'database.db'
TABLE = '调查点户名单'
MISSING_COLUMNS = [
    ('密码', 'TEXT'),
    ('调查小区名称', 'TEXT'),
    ('城乡属性', 'TEXT'),
    ('住宅地址', 'TEXT'),
    ('家庭人口', 'REAL'),
    ('是否退出', 'REAL'),
]

def column_exists(cur, table, col):
    info = cur.execute(f"PRAGMA table_info('{table}')").fetchall()
    cols = {r[1] for r in info}
    return col in cols


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        info_before = cur.execute(f"PRAGMA table_info('{TABLE}')").fetchall()
        logger.info('增列前结构: %s', info_before)

        for col, ctype in MISSING_COLUMNS:
            if not column_exists(cur, TABLE, col):
                sql = f"ALTER TABLE {TABLE} ADD COLUMN {col} {ctype}"
                logger.info('执行: %s', sql)
                cur.execute(sql)
            else:
                logger.info('已存在列，跳过: %s', col)

        conn.commit()
        info_after = cur.execute(f"PRAGMA table_info('{TABLE}')").fetchall()
        logger.info('增列后结构: %s', info_after)
        print('增列完成')
    except Exception as e:
        conn.rollback()
        logger.error('增列失败: %s', e)
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()

