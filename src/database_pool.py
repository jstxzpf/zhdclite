"""
SQLite数据库连接池模块
"""
import sqlite3
import os
import logging
from queue import Queue, Empty, Full
from contextlib import contextmanager
import threading

# 全局连接池实例
_pool = None
_pool_lock = threading.Lock()

class ConnectionPool:
    def __init__(self, db_path="database.db", pool_name="Internal", max_connections=10, timeout=30):
        self.logger = logging.getLogger(f"{__name__}.{pool_name}")
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool_name = pool_name
        self._pool = Queue(max_connections)
        self._lock = threading.Lock()
        self._connections_in_use = 0
        self._total_connections = 0
        self.db_path = os.path.abspath(db_path)
        self._initialize_pool()

    def _create_connection(self):
        """创建新的SQLite数据库连接"""
        try:
            # 确保数据库文件存在
            if not os.path.exists(self.db_path):
                raise FileNotFoundError(f"SQLite数据库文件未找到: {self.db_path}")
            
            # 创建SQLite连接
            connection = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                check_same_thread=False  # 允许多线程使用
            )
            
            # 设置SQLite连接参数
            connection.execute("PRAGMA foreign_keys = ON")  # 启用外键约束
            connection.execute("PRAGMA journal_mode = WAL")  # 使用WAL模式提高并发性能
            connection.execute("PRAGMA synchronous = NORMAL")  # 平衡性能和安全性
            connection.execute("PRAGMA cache_size = 10000")  # 增加缓存大小
            connection.execute("PRAGMA temp_store = MEMORY")  # 临时表存储在内存中
            
            # 设置行工厂，使结果可以通过列名访问
            connection.row_factory = sqlite3.Row
            
            self.logger.debug(f"[{self.pool_name}] 创建SQLite连接成功: {self.db_path}")
            return connection
        except Exception as e:
            self.logger.error(f"[{self.pool_name}] 创建SQLite连接失败: {e}")
            raise

    def _initialize_pool(self):
        """初始化连接池"""
        try:
            for _ in range(self.max_connections):
                conn = self._create_connection()
                self._pool.put(conn)
                self._total_connections += 1
            self.logger.info(f"[{self.pool_name}] 连接池初始化完成，创建了 {self.max_connections} 个连接")
        except Exception as e:
            self.logger.error(f"[{self.pool_name}] 连接池初始化失败: {e}")
            raise

    def get_connection(self):
        """从连接池获取一个连接"""
        try:
            with self._lock:
                self._connections_in_use += 1
            
            conn = self._pool.get(timeout=self.timeout)
            
            # 测试连接是否有效
            try:
                conn.execute("SELECT 1").fetchone()
            except sqlite3.Error:
                # 连接无效，创建新连接
                self.logger.warning(f"[{self.pool_name}] 检测到无效连接，正在重新创建")
                conn.close()
                conn = self._create_connection()
            
            return conn
        except Empty:
            self.logger.error(f"[{self.pool_name}] 获取连接超时")
            raise Exception("获取数据库连接超时")
        except Exception as e:
            with self._lock:
                self._connections_in_use -= 1
            self.logger.error(f"[{self.pool_name}] 获取连接失败: {e}")
            raise

    def return_connection(self, conn):
        """将连接返回到连接池"""
        try:
            if conn:
                # 回滚任何未提交的事务
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                
                self._pool.put(conn, timeout=1)
            
            with self._lock:
                self._connections_in_use -= 1
        except Full:
            # 连接池已满，关闭连接
            conn.close()
            with self._lock:
                self._total_connections -= 1
        except Exception as e:
            self.logger.error(f"[{self.pool_name}] 返回连接失败: {e}")
            if conn:
                conn.close()
            with self._lock:
                self._connections_in_use -= 1

    @contextmanager
    def get_cursor(self):
        """上下文管理器，自动管理连接和游标"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            yield cursor
            conn.commit()  # 自动提交事务
        except Exception as e:
            if conn:
                conn.rollback()  # 回滚事务
            self.logger.error(f"[{self.pool_name}] 数据库操作失败: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.return_connection(conn)

    def close_all(self):
        """关闭所有连接"""
        self.logger.info(f"[{self.pool_name}] 正在关闭所有数据库连接...")
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break
            except Exception as e:
                self.logger.error(f"[{self.pool_name}] 关闭连接时出错: {e}")
        
        with self._lock:
            self._total_connections = 0
            self._connections_in_use = 0
        
        self.logger.info(f"[{self.pool_name}] 所有连接已关闭")

    def get_stats(self):
        """获取连接池统计信息"""
        with self._lock:
            return {
                'total_connections': self._total_connections,
                'connections_in_use': self._connections_in_use,
                'available_connections': self._pool.qsize(),
                'max_connections': self.max_connections
            }

def get_connection_pool():
    """获取全局连接池实例"""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                # 使用项目根目录下的database.db文件
                db_path = os.path.join(os.getcwd(), 'database.db')
                _pool = ConnectionPool(db_path=db_path, pool_name="SQLite")
    return _pool

def close_connection_pool():
    """关闭全局连接池"""
    global _pool
    if _pool is not None:
        with _pool_lock:
            if _pool is not None:
                _pool.close_all()
                _pool = None
