"""
API响应辅助类
提供统一的API响应格式，减少重复代码
"""

from flask import jsonify
from typing import Any, Union
import logging

logger = logging.getLogger(__name__)

class ResponseHelper:
    """API响应辅助类，提供统一的响应格式"""
    
    @staticmethod
    def success_response(
        data: Any = None, 
        message: str = "操作成功", 
        status_code: int = 200
    ):
        """
        返回成功响应
        
        Args:
            data: 响应数据
            message: 响应消息
            status_code: HTTP状态码
            
        Returns:
            Flask响应对象
        """
        response_data = {
            'success': True,
            'message': message
        }
        
        if data is not None:
            response_data['data'] = data
            
        return jsonify(response_data), status_code
    
    @staticmethod
    def error_response(
        error: Union[str, Exception], 
        message: str = "操作失败", 
        status_code: int = 500,
        include_error_details: bool = True
    ):
        """
        返回错误响应
        
        Args:
            error: 错误信息或异常对象
            message: 错误消息
            status_code: HTTP状态码
            include_error_details: 是否包含错误详情
            
        Returns:
            Flask响应对象
        """
        response_data = {
            'success': False,
            'message': message
        }
        
        if include_error_details:
            response_data['error'] = str(error)
            
        logger.error(f"API错误响应: {message} - {str(error)}")
        
        return jsonify(response_data), status_code
    
    @staticmethod
    def validation_error_response(
        field: str, 
        message: str = "参数验证失败"
    ):
        """
        返回参数验证错误响应
        
        Args:
            field: 验证失败的字段
            message: 错误消息
            
        Returns:
            Flask响应对象
        """
        return ResponseHelper.error_response(
            f"字段 '{field}' 验证失败",
            message,
            status_code=400
        )
    
    @staticmethod
    def missing_params_response(required_params: list):
        """
        返回缺少参数的错误响应
        
        Args:
            required_params: 必需参数列表
            
        Returns:
            Flask响应对象
        """
        params_str = "、".join(required_params)
        return ResponseHelper.error_response(
            f"缺少必需参数: {params_str}",
            "参数不完整",
            status_code=400
        )
    
    @staticmethod
    def file_error_response(message: str = "文件处理失败"):
        """
        返回文件操作错误响应
        
        Args:
            message: 错误消息
            
        Returns:
            Flask响应对象
        """
        return ResponseHelper.error_response(
            "文件操作异常",
            message,
            status_code=400
        )
    
    @staticmethod
    def database_error_response(error: Exception):
        """
        返回数据库操作错误响应
        
        Args:
            error: 数据库异常
            
        Returns:
            Flask响应对象
        """
        return ResponseHelper.error_response(
            error,
            "数据库操作失败",
            status_code=500
        )

class FileResponseHelper:
    """文件响应辅助类"""
    
    @staticmethod
    def file_not_selected_response():
        """文件未选择响应"""
        return "未选择文件", 400
    
    @staticmethod
    def file_upload_success_response(filename: str, records_count: int = None):
        """文件上传成功响应"""
        message = f"文件 {filename} 上传成功"
        if records_count is not None:
            message += f"，共处理 {records_count} 条记录"
        return message, 200
    
    @staticmethod
    def file_processing_error_response(filename: str, error: str):
        """文件处理错误响应"""
        return f"文件 {filename} 处理失败: {error}", 500

def handle_api_exception(func):
    """
    API异常处理装饰器
    
    Args:
        func: 被装饰的函数
        
    Returns:
        包装后的函数
    """
    from functools import wraps
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            return ResponseHelper.validation_error_response("参数", str(e))
        except KeyError as e:
            return ResponseHelper.missing_params_response([str(e)])
        except Exception as e:
            logger.exception(f"API异常: {func.__name__}")
            return ResponseHelper.database_error_response(e)
    
    return wrapper