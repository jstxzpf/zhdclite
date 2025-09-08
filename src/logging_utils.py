"""
日志工具模块
提供统一的日志配置和管理功能
"""

import logging
import os
from datetime import datetime
from functools import wraps


class LoggerManager:
    """日志管理器"""
    
    _configured = False
    _loggers = {}
    
    @classmethod
    def configure_logging(cls, level=logging.INFO, log_dir='logs'):
        """
        配置应用程序的日志系统
        
        Args:
            level: 日志级别
            log_dir: 日志文件目录
        """
        if cls._configured:
            return
            
        # 创建日志目录
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 配置根日志记录器
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(
                    os.path.join(log_dir, f'app_{datetime.now().strftime("%Y%m%d")}.log'),
                    encoding='utf-8'
                ),
                logging.StreamHandler()
            ]
        )
        
        cls._configured = True
    
    @classmethod
    def get_logger(cls, name, level=None):
        """
        获取配置好的日志记录器
        
        Args:
            name: 日志记录器名称
            level: 可选的日志级别
            
        Returns:
            logging.Logger: 配置好的日志记录器
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        # 确保日志系统已配置
        if not cls._configured:
            cls.configure_logging()
        
        logger = logging.getLogger(name)
        if level:
            logger.setLevel(level)
            
        cls._loggers[name] = logger
        return logger


def get_module_logger(module_name=None):
    """
    获取模块日志记录器的便捷函数
    
    Args:
        module_name: 模块名称，默认使用调用者的 __name__
        
    Returns:
        logging.Logger: 日志记录器
    """
    if module_name is None:
        import inspect
        frame = inspect.currentframe().f_back
        module_name = frame.f_globals.get('__name__', 'unknown')
    
    return LoggerManager.get_logger(module_name)


def log_function_call(logger=None):
    """
    记录函数调用的装饰器
    
    Args:
        logger: 可选的日志记录器
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            func_logger = logger or get_module_logger(func.__module__)
            func_logger.debug(f"调用函数: {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                func_logger.debug(f"函数 {func.__name__} 执行成功")
                return result
            except Exception as e:
                func_logger.error(f"函数 {func.__name__} 执行失败: {str(e)}")
                raise
                
        return wrapper
    return decorator


def log_execution_time(logger=None):
    """
    记录函数执行时间的装饰器
    
    Args:
        logger: 可选的日志记录器
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            import time
            func_logger = logger or get_module_logger(func.__module__)
            
            start_time = time.time()
            func_logger.debug(f"开始执行函数: {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                func_logger.info(f"函数 {func.__name__} 执行完成，耗时: {execution_time:.2f}秒")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                func_logger.error(f"函数 {func.__name__} 执行失败，耗时: {execution_time:.2f}秒，错误: {str(e)}")
                raise
                
        return wrapper
    return decorator


# 预配置的日志记录器实例
app_logger = LoggerManager.get_logger('app')
db_logger = LoggerManager.get_logger('database')
api_logger = LoggerManager.get_logger('api')
file_logger = LoggerManager.get_logger('file_operations')