"""
工具函数模块
包含共享的工具函数和装饰器
"""

import logging
import traceback
import re
import os
from functools import wraps
from flask import jsonify

# 文件上传配置
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

def sanitize_filename(filename, max_length=200):
    """
    统一的文件名清理函数
    
    Args:
        filename: 原始文件名
        max_length: 文件名最大长度（不包含扩展名）
        
    Returns:
        清理后的文件名
    """
    if not filename:
        return filename
        
    # 移除或替换不允许的字符
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # 限制文件名长度（不包括扩展名）
    name, ext = os.path.splitext(filename)
    if len(name) > max_length:
        name = name[:max_length]
    
    return name + ext

def validate_file_extension(filename, allowed_extensions=None):
    """
    统一的文件扩展名验证函数
    
    Args:
        filename: 文件名
        allowed_extensions: 允许的扩展名集合，默认使用ALLOWED_EXTENSIONS
        
    Returns:
        是否为允许的扩展名
    """
    if not filename or '.' not in filename:
        return False
        
    if allowed_extensions is None:
        allowed_extensions = ALLOWED_EXTENSIONS
        
    extension = filename.rsplit('.', 1)[1].lower()
    return extension in allowed_extensions

# 保持向后兼容的函数
def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return validate_file_extension(filename)

def allowed_excel_file(filename):
    """检查Excel文件扩展名是否允许"""
    excel_extensions = {'xlsx', 'xls'}
    return validate_file_extension(filename, excel_extensions)

def allowed_csv_file(filename):
    """检查CSV文件扩展名是否允许"""
    csv_extensions = {'csv'}
    return validate_file_extension(filename, csv_extensions)

def validate_file_size(file):
    """验证文件大小"""
    file.seek(0, 2)  # 移动到文件末尾
    size = file.tell()
    file.seek(0)  # 重置文件指针
    return size <= MAX_FILE_SIZE

def handle_errors(f):
    """统一错误处理装饰器，智能处理不同类型的响应"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            result = f(*args, **kwargs)

            # 如果是字符串响应，需要判断是HTML还是普通消息
            if isinstance(result, str):
                # 检查是否是HTML内容（包含HTML标签）
                if result.strip().startswith('<!DOCTYPE') or result.strip().startswith('<html') or '<html>' in result:
                    # 这是HTML响应，直接返回
                    return result
                else:
                    # 这是普通消息，包装成JSON格式
                    return jsonify({
                        'success': True,
                        'message': result
                    })

            # 其他类型的响应（如Response对象、元组等）直接返回
            return result

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error in {f.__name__}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return jsonify({
                'success': False,
                'message': f"操作失败: {str(e)}",
                'error': str(e)
            }), 500
    return decorated_function
