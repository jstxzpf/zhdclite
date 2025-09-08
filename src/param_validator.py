"""
参数验证装饰器和工具类
提供统一的参数验证功能，减少重复代码
"""

from functools import wraps
from flask import request
from typing import List, Dict, Any, Optional, Union
import logging

logger = logging.getLogger(__name__)

def validate_required_params(required_params: List[str], source: str = 'args'):
    """
    参数验证装饰器
    
    Args:
        required_params: 必需参数列表
        source: 参数来源 ('args', 'form', 'json')
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from src.response_helper import ResponseHelper
            
            # 根据来源获取参数
            if source == 'args':
                params_dict = request.args
            elif source == 'form':
                params_dict = request.form
            elif source == 'json':
                params_dict = request.get_json() or {}
            else:
                return ResponseHelper.error_response(
                    f"不支持的参数来源: {source}",
                    "参数验证配置错误",
                    status_code=500
                )
            
            # 检查必需参数
            missing_params = []
            for param in required_params:
                if param not in params_dict or not params_dict[param]:
                    missing_params.append(param)
            
            if missing_params:
                return ResponseHelper.missing_params_response(missing_params)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

def validate_year_month_params(func):
    """
    年份月份参数验证装饰器（兼容旧版本）

    Args:
        func: 被装饰的函数

    Returns:
        包装后的函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        from src.response_helper import ResponseHelper

        # 优先检查新的时间段参数
        start_year = request.args.get('start_year')
        start_month = request.args.get('start_month')
        end_year = request.args.get('end_year')
        end_month = request.args.get('end_month')

        # 如果有时间段参数，使用时间段验证
        if start_year or start_month or end_year or end_month:
            return validate_date_range_params(func)(*args, **kwargs)

        # 否则使用旧的年月验证（向后兼容）
        year = request.args.get('year')
        month = request.args.get('month')

        if year or month:
            try:
                if year:
                    year_int = int(year)
                    if not (2020 <= year_int <= 2030):
                        return ResponseHelper.validation_error_response('year', '年份超出有效范围(2020-2030)')

                if month:
                    month_int = int(month)
                    if not (1 <= month_int <= 12):
                        return ResponseHelper.validation_error_response('month', '月份超出有效范围(1-12)')

            except ValueError:
                return ResponseHelper.validation_error_response('year或month', '参数必须为数字')

        return func(*args, **kwargs)
    return wrapper

def validate_date_range_params(func):
    """
    时间段范围参数验证装饰器

    Args:
        func: 被装饰的函数

    Returns:
        包装后的函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        from src.response_helper import ResponseHelper

        start_year = request.args.get('start_year')
        start_month = request.args.get('start_month')
        end_year = request.args.get('end_year')
        end_month = request.args.get('end_month')

        # 验证参数格式
        try:
            if start_year:
                start_year_int = int(start_year)
                if not (2020 <= start_year_int <= 2030):
                    return ResponseHelper.validation_error_response('start_year', '开始年份超出有效范围(2020-2030)')

            if start_month:
                start_month_int = int(start_month)
                if not (1 <= start_month_int <= 12):
                    return ResponseHelper.validation_error_response('start_month', '开始月份超出有效范围(1-12)')

            if end_year:
                end_year_int = int(end_year)
                if not (2020 <= end_year_int <= 2030):
                    return ResponseHelper.validation_error_response('end_year', '结束年份超出有效范围(2020-2030)')

            if end_month:
                end_month_int = int(end_month)
                if not (1 <= end_month_int <= 12):
                    return ResponseHelper.validation_error_response('end_month', '结束月份超出有效范围(1-12)')

            # 验证时间范围逻辑
            if start_year and start_month and end_year and end_month:
                start_date = start_year_int * 100 + start_month_int
                end_date = end_year_int * 100 + end_month_int
                if start_date > end_date:
                    return ResponseHelper.validation_error_response('date_range', '开始时间不能晚于结束时间')

        except ValueError:
            return ResponseHelper.validation_error_response('时间参数', '参数必须为数字')

        return func(*args, **kwargs)
    return wrapper

def validate_file_upload(allowed_extensions: Optional[List[str]] = None):
    """
    文件上传验证装饰器
    
    Args:
        allowed_extensions: 允许的文件扩展名列表
        
    Returns:
        装饰器函数
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from src.response_helper import FileResponseHelper
            from src.utils import allowed_file
            
            # 检查文件是否存在
            if 'file' not in request.files:
                return FileResponseHelper.file_not_selected_response()
            
            file = request.files['file']
            
            # 检查文件名是否为空
            if not file or file.filename == '':
                return FileResponseHelper.file_not_selected_response()
            
            # 检查文件扩展名
            if allowed_extensions and not allowed_file(file.filename, allowed_extensions):
                allowed_str = ', '.join(allowed_extensions)
                return f"不支持的文件类型，请上传 {allowed_str} 文件", 400
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

class ParamValidator:
    """参数验证工具类"""
    
    @staticmethod
    def validate_year(year: str) -> bool:
        """验证年份参数"""
        try:
            year_int = int(year)
            return 2020 <= year_int <= 2030
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_month(month: str) -> bool:
        """验证月份参数"""
        try:
            month_int = int(month)
            return 1 <= month_int <= 12
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_numeric_string(value: str, min_val: float = None, max_val: float = None) -> bool:
        """验证数字字符串"""
        try:
            num_val = float(value)
            if min_val is not None and num_val < min_val:
                return False
            if max_val is not None and num_val > max_val:
                return False
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def clean_string_param(value: str) -> Optional[str]:
        """清理字符串参数"""
        if not value:
            return None
        
        value = value.strip()
        if value in ('', 'null', 'undefined', 'nan'):
            return None
        
        return value
    
    @staticmethod
    def get_validated_filter_params() -> Dict[str, Any]:
        """
        获取并验证筛选参数
        
        Returns:
            验证后的参数字典
        """
        params = {}
        
        # 验证年份
        year = request.args.get('year')
        if year and ParamValidator.validate_year(year):
            params['year'] = year
        
        # 验证月份
        month = request.args.get('month')
        if month and ParamValidator.validate_month(month):
            params['month'] = month
        
        # 验证乡镇
        town = ParamValidator.clean_string_param(request.args.get('town'))
        if town:
            params['town'] = town
        
        # 验证村庄
        village = ParamValidator.clean_string_param(request.args.get('village'))
        if village:
            params['village'] = village
        
        # 验证户代码
        household = ParamValidator.clean_string_param(request.args.get('household'))
        if household:
            params['household'] = household
        
        return params

class FilterBuilder:
    """筛选条件构建工具类"""
    
    def __init__(self, table_alias: str = 't'):
        self.table_alias = table_alias
        self.conditions = []
        self.params = []
    
    def add_year_filter(self, year: str):
        """添加年份筛选条件"""
        if year and ParamValidator.validate_year(year):
            self.conditions.append(f"{self.table_alias}.year = ?")
            self.params.append(year)
    
    def add_month_filter(self, month: str):
        """添加月份筛选条件"""
        if month and ParamValidator.validate_month(month):
            self.conditions.append(f"{self.table_alias}.month = ?")
            self.params.append(month)

    def add_date_range_filter(self, start_year: str = None, start_month: str = None,
                             end_year: str = None, end_month: str = None):
        """添加时间段范围筛选条件"""
        date_conditions = []

        # 构建开始时间条件
        if start_year and start_month:
            if ParamValidator.validate_year(start_year) and ParamValidator.validate_month(start_month):
                date_conditions.append(f"({self.table_alias}.year > ? OR ({self.table_alias}.year = ? AND {self.table_alias}.month >= ?))")
                self.params.extend([start_year, start_year, start_month])
        elif start_year:
            if ParamValidator.validate_year(start_year):
                date_conditions.append(f"{self.table_alias}.year >= ?")
                self.params.append(start_year)
        elif start_month:
            if ParamValidator.validate_month(start_month):
                date_conditions.append(f"{self.table_alias}.month >= ?")
                self.params.append(start_month)

        # 构建结束时间条件
        if end_year and end_month:
            if ParamValidator.validate_year(end_year) and ParamValidator.validate_month(end_month):
                date_conditions.append(f"({self.table_alias}.year < ? OR ({self.table_alias}.year = ? AND {self.table_alias}.month <= ?))")
                self.params.extend([end_year, end_year, end_month])
        elif end_year:
            if ParamValidator.validate_year(end_year):
                date_conditions.append(f"{self.table_alias}.year <= ?")
                self.params.append(end_year)
        elif end_month:
            if ParamValidator.validate_month(end_month):
                date_conditions.append(f"{self.table_alias}.month <= ?")
                self.params.append(end_month)

        # 合并时间条件
        if date_conditions:
            combined_condition = " AND ".join(date_conditions)
            self.conditions.append(f"({combined_condition})")
    
    def add_town_filter(self, town: str, town_codes: Dict[str, str]):
        """添加乡镇筛选条件（旧版本，基于代码前缀）"""
        if town and town in town_codes:
            self.conditions.append(f"LEFT({self.table_alias}.hudm, 9) = ?")
            self.params.append(town_codes[town])

    def add_town_filter_with_mapping(self, town: str, mapping: Dict):
        """添加乡镇筛选条件，基于v_town_village_list视图映射"""
        if town and mapping and town in mapping.get('town_to_villages', {}):
            village_codes = mapping['town_to_villages'][town]
            if village_codes:
                placeholders = ','.join(['?' for _ in village_codes])
                self.conditions.append(f"LEFT({self.table_alias}.hudm, 12) IN ({placeholders})")
                self.params.extend(village_codes)
    
    def add_village_filter(self, village_code: str):
        """添加村庄筛选条件，基于村庄代码"""
        if village_code:
            # village_code是从v_town_village_list视图获取的村代码
            self.conditions.append(f"LEFT({self.table_alias}.hudm, 12) = ?")
            self.params.append(village_code)
    
    def add_household_filter(self, household: str):
        """添加户代码筛选条件"""
        if household:
            self.conditions.append(f"{self.table_alias}.hudm = ?")
            self.params.append(household)
    
    def build_where_clause(self) -> str:
        """构建WHERE子句"""
        if self.conditions:
            return "WHERE " + " AND ".join(self.conditions)
        return ""
    
    def get_params(self) -> List[Any]:
        """获取参数列表"""
        return self.params.copy()
    
    @classmethod
    def build_from_request(cls, table_alias: str = 't', town_codes: Optional[Dict[str, str]] = None, exclude_town: bool = False):
        """
        从请求参数构建筛选条件
        
        Args:
            table_alias: 表别名
            town_codes: 乡镇代码映射
            exclude_town: 是否排除乡镇筛选
            
        Returns:
            WHERE子句和参数元组
        """
        builder = cls(table_alias)
        params_dict = ParamValidator.get_validated_filter_params()
        
        builder.add_year_filter(params_dict.get('year'))
        builder.add_month_filter(params_dict.get('month'))
        
        if not exclude_town and town_codes:
            builder.add_town_filter(params_dict.get('town'), town_codes)
        
        builder.add_village_filter(params_dict.get('village'))
        builder.add_household_filter(params_dict.get('household'))
        
        return builder.build_where_clause(), builder.get_params()

    @classmethod
    def build_from_request_with_mapping(cls, table_alias: str = 't', mapping: Optional[Dict] = None, exclude_town: bool = False):
        """
        从请求参数构建筛选条件，使用v_town_village_list视图映射

        Args:
            table_alias: 表别名
            mapping: 乡镇村庄映射字典
            exclude_town: 是否排除乡镇筛选

        Returns:
            WHERE子句和参数元组
        """
        builder = cls(table_alias)
        params_dict = ParamValidator.get_validated_filter_params()

        # 优先使用时间段范围筛选
        start_year = request.args.get('start_year')
        start_month = request.args.get('start_month')
        end_year = request.args.get('end_year')
        end_month = request.args.get('end_month')

        if start_year or start_month or end_year or end_month:
            # 使用时间段范围筛选
            builder.add_date_range_filter(start_year, start_month, end_year, end_month)
        else:
            # 向后兼容：使用单独的年月筛选
            builder.add_year_filter(params_dict.get('year'))
            builder.add_month_filter(params_dict.get('month'))

        if not exclude_town and mapping:
            builder.add_town_filter_with_mapping(params_dict.get('town'), mapping)

        builder.add_village_filter(params_dict.get('village'))
        builder.add_household_filter(params_dict.get('household'))

        return builder.build_where_clause(), builder.get_params()