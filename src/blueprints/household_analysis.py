#!/usr/bin/env python3
"""
农户家庭收支调查系统 - 分户审核分析API接口
提供RESTful API接口，支持按户代码、时间范围进行分析
"""

from flask import Blueprint, request
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis_report_generator import AnalysisReportGenerator

from src.param_validator import ParamValidator
from src.response_helper import ResponseHelper

# 全局变量，在init_blueprint中初始化
db = None
report_generator = None
word_generator = None
handle_errors = None

logger = logging.getLogger(__name__)

# 创建蓝图
household_analysis_bp = Blueprint('household_analysis', __name__)


def init_blueprint(database, error_handler):
    """
    初始化蓝图依赖

    Args:
        database: 数据库连接对象
        error_handler: 错误处理装饰器
    """
    global db, report_generator, word_generator, handle_errors
    db = database
    report_generator = AnalysisReportGenerator(database)

    handle_errors = error_handler





@household_analysis_bp.route('/api/household-analysis/single', methods=['POST'])
def analyze_single_household():
    """
    单户分析接口
    
    请求参数:
    {
        "household_code": "户代码",
        "start_year": "开始年份(可选)",
        "start_month": "开始月份(可选)",
        "end_year": "结束年份(可选)",
        "end_month": "结束月份(可选)"
    }
    """
    try:
        data = request.get_json()
        
        # 参数验证
        if not data or 'household_code' not in data:
            return ResponseHelper.error_response('缺少必要参数: household_code', status_code=400)

        household_code = data['household_code'].strip()
        if not household_code:
            return ResponseHelper.error_response('户代码不能为空', status_code=400)

        # 可选参数
        start_year = data.get('start_year')
        start_month = data.get('start_month')
        end_year = data.get('end_year')
        end_month = data.get('end_month')

        # 参数格式验证
        if start_year and not ParamValidator.validate_year(start_year):
            return ResponseHelper.error_response('开始年份格式错误', status_code=400)

        if start_month and not ParamValidator.validate_month(start_month):
            return ResponseHelper.error_response('开始月份格式错误', status_code=400)

        if end_year and not ParamValidator.validate_year(end_year):
            return ResponseHelper.error_response('结束年份格式错误', status_code=400)

        if end_month and not ParamValidator.validate_month(end_month):
            return ResponseHelper.error_response('结束月份格式错误', status_code=400)
        
        logger.info(f"开始分析户: {household_code}")
        
        # 生成分析报告
        report = report_generator.generate_household_analysis_report(
            household_code, start_year, start_month, end_year, end_month
        )
        
        if report.get('success'):
            logger.info(f"户 {household_code} 分析完成")
            return ResponseHelper.success_response(report, '分析完成')
        else:
            logger.error(f"户 {household_code} 分析失败: {report.get('error')}")
            return ResponseHelper.error_response(report.get('error', '分析失败'))

    except Exception as e:
        logger.error(f"单户分析接口异常: {e}")
        return ResponseHelper.error_response(f'分析失败: {str(e)}')





@household_analysis_bp.route('/api/household-analysis/area', methods=['POST'])
def analyze_area_households():
    """
    区域分析接口
    
    请求参数:
    {
        "town_name": "乡镇名称(可选)",
        "village_name": "村庄名称(可选)",
        "start_year": "开始年份(可选)",
        "start_month": "开始月份(可选)",
        "end_year": "结束年份(可选)",
        "end_month": "结束月份(可选)"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return ResponseHelper.error_response('请求参数不能为空', status_code=400)

        town_name = data.get('town_name', '').strip() if data.get('town_name') else None
        village_name = data.get('village_name', '').strip() if data.get('village_name') else None

        if not town_name and not village_name:
            return ResponseHelper.error_response('至少需要指定乡镇或村庄', status_code=400)

        # 可选参数
        start_year = data.get('start_year')
        start_month = data.get('start_month')
        end_year = data.get('end_year')
        end_month = data.get('end_month')

        # 参数格式验证
        if start_year and not ParamValidator.validate_year(start_year):
            return ResponseHelper.error_response('开始年份格式错误', status_code=400)

        if start_month and not ParamValidator.validate_month(start_month):
            return ResponseHelper.error_response('开始月份格式错误', status_code=400)

        if end_year and not ParamValidator.validate_year(end_year):
            return ResponseHelper.error_response('结束年份格式错误', status_code=400)

        if end_month and not ParamValidator.validate_month(end_month):
            return ResponseHelper.error_response('结束月份格式错误', status_code=400)
        
        logger.info(f"开始区域分析: 乡镇={town_name}, 村庄={village_name}")
        
        # 生成区域分析报告
        report = report_generator.generate_area_analysis_report(
            town_name, village_name, start_year, start_month, end_year, end_month
        )
        
        if report.get('success'):
            household_count = report.get('report_metadata', {}).get('区域信息', {}).get('户数', 0)
            logger.info(f"区域分析完成，共分析 {household_count} 户")
            return ResponseHelper.success_response(report, '区域分析完成')
        else:
            logger.error(f"区域分析失败: {report.get('error')}")
            return ResponseHelper.error_response(report.get('error', '区域分析失败'))

    except Exception as e:
        logger.error(f"区域分析接口异常: {e}")
        return ResponseHelper.error_response(f'区域分析失败: {str(e)}')






