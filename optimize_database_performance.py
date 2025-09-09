#!/usr/bin/env python3
"""
数据库性能优化脚本
解决概览数据加载慢的问题
"""

import sqlite3
import logging
import time
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseOptimizer:
    """数据库性能优化器"""
    
    def __init__(self, db_path='database.db'):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """连接数据库"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("PRAGMA cache_size = 20000")  # 增加缓存
        self.conn.execute("PRAGMA temp_store = MEMORY")
        self.conn.execute("PRAGMA mmap_size = 268435456")  # 256MB内存映射
        logger.info("数据库连接成功，已优化基础参数")
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
    
    def create_performance_indexes(self):
        """创建性能优化索引"""
        logger.info("开始创建性能优化索引...")
        
        indexes = [
            # 复合索引 - 针对常用查询组合
            ("idx_台账_复合_年月类型", "调查点台账合并", "(year, month, type)"),
            ("idx_台账_复合_户代码年月", "调查点台账合并", "(hudm, year, month)"),
            ("idx_台账_复合_类型编码", "调查点台账合并", "(type, code)"),
            ("idx_台账_复合_年月编码", "调查点台账合并", "(year, month, code)"),
            
            # 村代码前缀索引 - 优化乡镇查询
            ("idx_台账_村代码前缀", "调查点台账合并", "(SUBSTR(hudm, 1, 12))"),
            
            # 金额索引 - 优化统计查询
            ("idx_台账_金额", "调查点台账合并", "(money)"),
            
            # 覆盖索引 - 包含常用字段
            ("idx_台账_覆盖_统计", "调查点台账合并", "(year, month, type, code, money, hudm)"),
        ]
        
        for idx_name, table_name, columns in indexes:
            try:
                # 检查索引是否已存在
                cursor = self.conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (idx_name,))
                if cursor.fetchone():
                    logger.info(f"索引 {idx_name} 已存在，跳过")
                    continue
                
                # 创建索引
                sql = f"CREATE INDEX {idx_name} ON {table_name} {columns}"
                start_time = time.time()
                cursor.execute(sql)
                end_time = time.time()
                
                logger.info(f"创建索引 {idx_name} 成功，耗时 {end_time - start_time:.2f} 秒")
                
            except Exception as e:
                logger.error(f"创建索引 {idx_name} 失败: {e}")
        
        self.conn.commit()
        logger.info("索引创建完成")
    
    def create_materialized_views(self):
        """创建物化视图（使用表模拟）"""
        logger.info("开始创建物化视图...")
        
        try:
            cursor = self.conn.cursor()
            
            # 1. 创建乡镇统计汇总表
            cursor.execute("DROP TABLE IF EXISTS town_statistics_cache")
            cursor.execute("""
                CREATE TABLE town_statistics_cache (
                    乡镇名称 TEXT PRIMARY KEY,
                    记账笔数 INTEGER,
                    户数 INTEGER,
                    收入笔数 INTEGER,
                    支出笔数 INTEGER,
                    收入总额 REAL,
                    支出总额 REAL,
                    未编码笔数 INTEGER,
                    已编码笔数 INTEGER,
                    更新时间 DATETIME
                )
            """)
            
            # 2. 填充乡镇统计数据
            cursor.execute("""
                INSERT INTO town_statistics_cache
                SELECT 
                    v.所在乡镇街道 as 乡镇名称,
                    COUNT(t.id) as 记账笔数,
                    COUNT(DISTINCT t.hudm) as 户数,
                    COUNT(CASE WHEN t.type = 1 THEN 1 END) as 收入笔数,
                    COUNT(CASE WHEN t.type = 2 THEN 1 END) as 支出笔数,
                    SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) as 收入总额,
                    SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) as 支出总额,
                    COUNT(CASE WHEN t.code IS NULL THEN 1 END) as 未编码笔数,
                    COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) as 已编码笔数,
                    datetime('now') as 更新时间
                FROM v_town_village_list v
                LEFT JOIN 调查点台账合并 t ON SUBSTR(t.hudm, 1, 12) = v.村代码
                GROUP BY v.所在乡镇街道
            """)
            
            # 3. 创建月度统计汇总表
            cursor.execute("DROP TABLE IF EXISTS month_statistics_cache")
            cursor.execute("""
                CREATE TABLE month_statistics_cache (
                    年份 INTEGER,
                    月份 INTEGER,
                    记账笔数 INTEGER,
                    户数 INTEGER,
                    收入笔数 INTEGER,
                    支出笔数 INTEGER,
                    收入总额 REAL,
                    支出总额 REAL,
                    未编码笔数 INTEGER,
                    已编码笔数 INTEGER,
                    更新时间 DATETIME,
                    PRIMARY KEY (年份, 月份)
                )
            """)
            
            # 4. 填充月度统计数据
            cursor.execute("""
                INSERT INTO month_statistics_cache
                SELECT 
                    t.year as 年份,
                    t.month as 月份,
                    COUNT(*) as 记账笔数,
                    COUNT(DISTINCT t.hudm) as 户数,
                    COUNT(CASE WHEN t.type = 1 THEN 1 END) as 收入笔数,
                    COUNT(CASE WHEN t.type = 2 THEN 1 END) as 支出笔数,
                    SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) as 收入总额,
                    SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) as 支出总额,
                    COUNT(CASE WHEN t.code IS NULL THEN 1 END) as 未编码笔数,
                    COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) as 已编码笔数,
                    datetime('now') as 更新时间
                FROM 调查点台账合并 t
                GROUP BY t.year, t.month
            """)
            
            self.conn.commit()
            logger.info("物化视图创建完成")
            
        except Exception as e:
            logger.error(f"创建物化视图失败: {e}")
            self.conn.rollback()
    
    def analyze_tables(self):
        """分析表统计信息"""
        logger.info("开始分析表统计信息...")
        
        tables = ['调查点台账合并', '调查点户名单', '调查品种编码', 'v_town_village_list']
        
        for table in tables:
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"ANALYZE {table}")
                logger.info(f"分析表 {table} 完成")
            except Exception as e:
                logger.error(f"分析表 {table} 失败: {e}")
        
        self.conn.commit()
        logger.info("表统计信息分析完成")
    
    def vacuum_database(self):
        """清理和优化数据库"""
        logger.info("开始清理数据库...")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("VACUUM")
            logger.info("数据库清理完成")
        except Exception as e:
            logger.error(f"数据库清理失败: {e}")
    
    def test_performance(self):
        """测试优化后的性能"""
        logger.info("开始性能测试...")
        
        cursor = self.conn.cursor()
        
        # 测试概览查询
        overview_sql = """
        SELECT
            COUNT(*), COUNT(DISTINCT t.hudm), COUNT(DISTINCT (t.year || '-' || t.month)),
            SUM(CASE WHEN t.money > 0 THEN t.money ELSE 0 END),
            SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END),
            SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END),
            COUNT(CASE WHEN t.code IS NULL THEN 1 END),
            COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END)
        FROM 调查点台账合并 t
        """
        
        start_time = time.time()
        cursor.execute(overview_sql)
        result = cursor.fetchone()
        end_time = time.time()
        
        logger.info(f"概览查询优化后耗时: {end_time - start_time:.3f} 秒")
        
        # 测试乡镇查询（使用缓存表）
        town_sql = "SELECT * FROM town_statistics_cache"
        
        start_time = time.time()
        cursor.execute(town_sql)
        town_result = cursor.fetchall()
        end_time = time.time()
        
        logger.info(f"乡镇查询优化后耗时: {end_time - start_time:.3f} 秒")
        logger.info(f"查询到 {len(town_result)} 个乡镇")
    
    def run_optimization(self):
        """运行完整的优化流程"""
        logger.info("开始数据库性能优化...")
        
        try:
            self.connect()
            
            # 1. 创建性能索引
            self.create_performance_indexes()
            
            # 2. 创建物化视图
            self.create_materialized_views()
            
            # 3. 分析表统计信息
            self.analyze_tables()
            
            # 4. 清理数据库
            self.vacuum_database()
            
            # 5. 测试性能
            self.test_performance()
            
            logger.info("数据库性能优化完成！")
            
        except Exception as e:
            logger.error(f"优化过程中发生错误: {e}")
        finally:
            self.close()

def main():
    """主函数"""
    optimizer = DatabaseOptimizer()
    optimizer.run_optimization()

if __name__ == "__main__":
    main()
