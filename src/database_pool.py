"""
数据库连接池模块
"""
import pyodbc
import json
import os
import logging
import time
from queue import Queue, Empty, Full
from contextlib import contextmanager
import threading

# 全局连接池实例
_pool = None
_external_pool = None
_pool_lock = threading.Lock()

class ConnectionPool:
    def __init__(self, config_path, pool_name="Internal", max_connections=10, timeout=30):
        self.logger = logging.getLogger(f"{__name__}.{pool_name}")
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool_name = pool_name
        self._pool = Queue(max_connections)
        self._lock = threading.Lock()
        self._connections_in_use = 0
        self._total_connections = 0
        self.config = self._load_config(config_path)
        self._initialize_pool()

    def _load_config(self, config_path):
        """加载数据库配置"""
        # 优先使用环境变量
        if os.getenv('DATABASE_HOST') and self.pool_name == "Internal":
            return {
                'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
                'server': os.getenv('DATABASE_HOST'),
                'database': os.getenv('DATABASE_NAME'),
                'uid': os.getenv('DATABASE_USER', 'sa'),
                'pwd': os.getenv('DATABASE_PASSWORD', ''),
                'encrypt': os.getenv('DB_ENCRYPT', 'yes'),
                'trustServerCertificate': os.getenv('DB_TRUST_CERT', 'yes')
            }
        # 否则，从文件加载
        full_config_path = os.path.join(os.getcwd(), config_path)
        if not os.path.exists(full_config_path):
            raise FileNotFoundError(f"配置文件未找到: {full_config_path}")
        with open(full_config_path, 'r') as f:
            return json.load(f)

    def _create_connection(self):
        """创建一个新的数据库连接"""
        conn_str = ';'.join([
            f"DRIVER={self.config['driver']}",
            f"SERVER={self.config['server']}",
            f"DATABASE={self.config['database']}",
            f"ENCRYPT={self.config.get('encrypt', 'yes')}",
            f"TrustServerCertificate={self.config.get('trustServerCertificate', 'yes')}",
            f"UID={self.config['uid']}",
            f"PWD={self.config['pwd']}",
            "Connection Timeout=60",
            "Command Timeout=600"
        ])
        try:
            conn = pyodbc.connect(conn_str, timeout=60)
            conn.timeout = 600
            self.logger.info(f"({self.pool_name}) 创建了一个新的数据库连接")
            return conn
        except pyodbc.Error as e:
            self.logger.error(f"({self.pool_name}) 创建连接失败: {e}")
            raise

    def _initialize_pool(self):
        """初始化连接池"""
        for _ in range(self.max_connections // 2): # 启动时先创建一半的连接
            try:
                conn = self._create_connection()
                self._pool.put(conn)
                self._total_connections += 1
            except Exception as e:
                self.logger.warning(f"({self.pool_name}) 初始化连接池时创建连接失败: {e}")

    def get_connection(self):
        """从池中获取一个连接"""
        with self._lock:
            if self._connections_in_use >= self.max_connections:
                self.logger.warning(f"({self.pool_name}) 连接池已满，等待释放...")
                raise TimeoutError("获取数据库连接超时")

        try:
            # 尝试从队列中获取连接
            conn = self._pool.get(timeout=self.timeout)
            # 检查连接是否仍然有效
            if not self._is_connection_valid(conn):
                self.logger.warning(f"({self.pool_name}) 检测到无效连接，正在重新创建...")
                self._total_connections -= 1
                conn = self._create_connection()
                self._total_connections += 1
        except Empty:
            # 如果队列为空，并且我们还没有达到最大连接数，就创建一个新的连接
            with self._lock:
                if self._total_connections < self.max_connections:
                    try:
                        conn = self._create_connection()
                        self._total_connections += 1
                    except Exception as e:
                        raise ConnectionError(f"({self.pool_name}) 无法创建新连接: {e}")
                else:
                    # 如果已经达到最大连接数，则等待
                    self.logger.warning(f"({self.pool_name}) 连接池已空且达到最大连接数，等待释放...")
                    try:
                        conn = self._pool.get(timeout=self.timeout)
                    except Empty:
                        raise TimeoutError("获取数据库连接超时")

        with self._lock:
            self._connections_in_use += 1
        self.logger.debug(f"({self.pool_name}) 获取连接。当前使用中: {self._connections_in_use}/{self._total_connections}")
        return conn

    def release_connection(self, conn):
        """将连接释放回池中"""
        if conn:
            try:
                # 检查连接是否有效，无效则不放回
                if not self._is_connection_valid(conn):
                    self.logger.warning(f"({self.pool_name}) 释放了一个无效的连接，将其丢弃。")
                    with self._lock:
                        self._total_connections -= 1
                    return
                # 将连接放回队列
                self._pool.put(conn, block=False)
            except Full:
                # 如果池已满，则关闭此连接
                self.logger.warning(f"({self.pool_name}) 连接池已满，关闭多余的连接。")
                conn.close()
                with self._lock:
                    self._total_connections -= 1
            finally:
                with self._lock:
                    self._connections_in_use -= 1
                self.logger.debug(f"({self.pool_name}) 释放连接。当前使用中: {self._connections_in_use}/{self._total_connections}")

    def _is_connection_valid(self, conn):
        """检查连接是否仍然有效"""
        try:
            if conn.closed:
                return False
            conn.execute("SELECT 1")
            return True
        except pyodbc.Error:
            return False

    @contextmanager
    def get_cursor(self):
        """获取游标的上下文管理器，自动处理连接的获取和释放"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            yield cursor
            conn.commit() # 默认提交事务
        except Exception as e:
            self.logger.error(f"({self.pool_name}) 数据库操作出错: {e}")
            if conn:
                try:
                    conn.rollback()
                    self.logger.info(f"({self.pool_name}) 事务已回滚。")
                except pyodbc.Error as rb_e:
                    self.logger.error(f"({self.pool_name}) 回滚失败: {rb_e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.release_connection(conn)

def get_connection_pool():
    """获取内部数据库连接池的单例"""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ConnectionPool('config/mssql.json', pool_name="Internal")
    return _pool

def get_external_connection_pool():
    """获取外部数据库连接池的单例"""
    global _external_pool
    if _external_pool is None:
        with _pool_lock:
            if _external_pool is None:
                _external_pool = ConnectionPool('config/wbfwq.json', pool_name="External")
    return _external_pool