"""
统计页面模块蓝图
包含分户、分乡镇、分月的各种统计功能
"""

from flask import Blueprint, request, jsonify, render_template
import logging
import pandas as pd
from datetime import datetime, timedelta
import calendar
import threading

# 导入新的工具类
from src.query_service import QueryService
from src.response_helper import ResponseHelper, handle_api_exception
from src.param_validator import validate_year_month_params, validate_required_params, FilterBuilder

# 创建蓝图
statistics_bp = Blueprint('statistics', __name__)
logger = logging.getLogger(__name__)

# 这些变量将在蓝图注册时从主应用传入
db = None
query_service = None

# 用于缓存从数据库加载的乡镇代码
_town_code_cache = None
_town_code_lock = threading.Lock()

def init_blueprint(database, error_handler):
    """初始化蓝图依赖"""
    global db, query_service
    db = database
    query_service = QueryService(database)
    # error_handler is no longer needed as a global variable

@statistics_bp.app_errorhandler(Exception)
def handle_statistics_error(e):
    """
    为该蓝图下的所有路由提供统一的错误处理。
    - 对 HTTPException 原样返回其状态码
    - 仅对非 HTTPException 返回 500
    """
    from werkzeug.exceptions import HTTPException

    # 如果是已知的HTTP异常（如404），直接返回原异常（保持状态码）
    if isinstance(e, HTTPException):
        return e

    # 记录详细的错误日志
    logger.error(f"An error occurred in the statistics blueprint: {e}", exc_info=True)
    # 返回一个标准的JSON错误响应
    return jsonify({
        'success': False,
        'message': f'服务器内部错误: {str(e)}'
    }), 500

def _get_town_village_mapping():
    """
    从v_town_village_list视图加载完整的乡镇-村庄映射关系，并使用线程安全的缓存。
    返回格式：{
        'town_to_villages': {乡镇名: [村代码列表]},
        'village_to_town': {村代码: 乡镇名},
        'village_names': {村代码: 村名}
    }
    """
    global _town_code_cache
    if _town_code_cache is None:
        with _town_code_lock:
            if _town_code_cache is None:
                logger.info("缓存未命中，正在从v_town_village_list视图加载乡镇村庄映射...")
                sql = "SELECT `村代码`, `所在乡镇街道`, `村居名称` FROM `v_town_village_list` WHERE `所在乡镇街道` IS NOT NULL AND `村居名称` IS NOT NULL"
                result = db.execute_query_safe(sql)

                town_to_villages = {}
                village_to_town = {}
                village_names = {}

                for row in result:
                    village_code = row[0]
                    town_name = row[1]
                    village_name = row[2]

                    # 构建乡镇到村庄代码的映射
                    if town_name not in town_to_villages:
                        town_to_villages[town_name] = []
                    town_to_villages[town_name].append(village_code)

                    # 构建村庄代码到乡镇的映射
                    village_to_town[village_code] = town_name

                    # 构建村庄代码到村庄名称的映射
                    village_names[village_code] = village_name

                _town_code_cache = {
                    'town_to_villages': town_to_villages,
                    'village_to_town': village_to_town,
                    'village_names': village_names
                }
                logger.info(f"乡镇村庄映射缓存加载完成，共 {len(town_to_villages)} 个乡镇，{len(village_names)} 个村庄。")
    return _town_code_cache

def _build_query_filters(table_alias='t', exclude_town=False):
    """
    从请求参数中构建SQL查询的WHERE子句和参数列表。
    使用v_town_village_list视图的映射关系
    """
    mapping = None if exclude_town else _get_town_village_mapping()
    return FilterBuilder.build_from_request_with_mapping(table_alias, mapping, exclude_town)

@statistics_bp.route('/statistics')
def statistics_page():
    """统计页面"""
    return render_template('statistics.html')

@statistics_bp.route('/api/statistics/overview')
@handle_api_exception
def get_overview_statistics():
    """获取总体统计概览"""
    where_clause, params = _build_query_filters()
    
    sql = f"""
    SELECT
        COUNT(*), COUNT(DISTINCT t.hudm), COUNT(DISTINCT (t.year || '-' || t.month)),
        SUM(CASE WHEN t.money > 0 THEN t.money ELSE 0 END),
        SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END),
        SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END),
        COUNT(CASE WHEN t.code IS NULL THEN 1 END),
        COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END)
    FROM `调查点台账合并` t
    {where_clause}
    """
    
    result = db.execute_query_safe(sql, params)
    if result and result[0]:
        data = result[0]
        overview = {
            'total_records': data[0] or 0, 'total_households': data[1] or 0,
            'total_months': data[2] or 0, 'total_amount': float(data[3] or 0),
            'total_income': float(data[4] or 0), 'total_expenditure': float(data[5] or 0),
            'uncoded_records': data[6] or 0, 'coded_records': data[7] or 0
        }
    else:
        overview = {k: 0 for k in ['total_records', 'total_households', 'total_months', 'total_amount', 'total_income', 'total_expenditure', 'uncoded_records', 'coded_records']}

    return ResponseHelper.success_response(overview)

@statistics_bp.route('/api/statistics/by_household')
@validate_year_month_params
@handle_api_exception
def get_household_statistics():
    """获取分户统计数据"""
    where_clause, params = _build_query_filters()
    data = query_service.get_household_statistics(where_clause, params)
    return ResponseHelper.success_response(data)

@statistics_bp.route('/api/statistics/by_town')
@validate_year_month_params
@handle_api_exception
def get_town_statistics():
    """获取分乡镇统计数据（优化版本，避免N+1查询）"""
    # 构建筛选条件（排除乡镇筛选，因为在查询中处理）
    base_where_clause, base_params = _build_query_filters(exclude_town=True)

    # 使用优化后的一次性查询获取所有乡镇统计数据
    town_stats = query_service.get_all_town_statistics(base_where_clause, base_params)

    return ResponseHelper.success_response(town_stats)

@statistics_bp.route('/api/statistics/by_month')
@validate_year_month_params
@handle_api_exception
def get_month_statistics():
    """获取分月统计数据（支持时间段范围筛选）"""
    where_clause, params = _build_query_filters()

    sql = f"""
    SELECT
        t.year,
        t.month,
        COUNT(*) AS `记账笔数`,
        COUNT(DISTINCT t.hudm) AS `户数`,
        COUNT(CASE WHEN t.type = 1 THEN 1 END) AS `收入笔数`,
        COUNT(CASE WHEN t.type = 2 THEN 1 END) AS `支出笔数`,
        SUM(CASE WHEN t.type = 1 THEN t.money ELSE 0 END) AS `收入总额`,
        SUM(CASE WHEN t.type = 2 THEN t.money ELSE 0 END) AS `支出总额`,
        COUNT(CASE WHEN t.code IS NULL THEN 1 END) AS `未编码笔数`,
        COUNT(CASE WHEN t.code IS NOT NULL THEN 1 END) AS `已编码笔数`
    FROM `调查点台账合并` t
    {where_clause}
    GROUP BY t.year, t.month
    ORDER BY t.year, t.month
    """

    result = db.execute_query_safe(sql, params)

    # 将查询结果转换为前端所需的数组结构
    rows = []
    if result:
        for r in result:
            rows.append({
                '年份': str(r[0]),
                '月份': str(r[1]).zfill(2),
                '户数': r[3] or 0,
                '记账笔数': r[2] or 0,
                '收入笔数': r[4] or 0,
                '支出笔数': r[5] or 0,
                '收入总额': float(r[6] or 0),
                '支出总额': float(r[7] or 0),
                '未编码笔数': r[8] or 0,
                '已编码笔数': r[9] or 0
            })

    return ResponseHelper.success_response(rows)

@statistics_bp.route('/api/statistics/consumption_structure')
@validate_year_month_params
@handle_api_exception
def get_consumption_structure():
    """获取消费结构统计数据"""
    where_clause, params = _build_query_filters()
    data = query_service.get_consumption_structure(where_clause, params)
    return ResponseHelper.success_response(data)

def _build_household_filters_for_missing() -> (str, list):
    """仅为漏记账分析构建针对 h 表（调查点户名单）的筛选条件，排除 year/month 条件。
    乡镇/村庄/户代码统一使用 v_town_village_list 映射。
    """
    try:
        from src.param_validator import ParamValidator
        params_dict = ParamValidator.get_validated_filter_params()

        conditions = []
        params = []

        mapping = _get_town_village_mapping()

        # 优先精确到户
        household = params_dict.get('household')
        if household:
            conditions.append("h.户代码 = ?")
            params.append(household)
        else:
            # 村级筛选
            village_code = params_dict.get('village')
            if village_code:
                conditions.append("SUBSTR(h.`户代码`, 1, 12) = ?")
                params.append(village_code)
            else:
                # 乡镇 -> 多村代码
                town = params_dict.get('town')
                if town and mapping and town in mapping.get('town_to_villages', {}):
                    village_codes = mapping['town_to_villages'][town]
                    if village_codes:
                        placeholders = ','.join(['?' for _ in village_codes])
                        conditions.append(f"SUBSTR(h.`户代码`, 1, 12) IN ({placeholders})")
                        params.extend(village_codes)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        return where_clause, params
    except Exception as e:
        logger.error(f"构建筛选条件失败: {e}")
        return "", []


@statistics_bp.route('/api/statistics/missing_days')
@validate_year_month_params
@handle_api_exception
def get_missing_days_statistics():
    """获取漏记账天数统计（支持时间段范围分析）"""
    # 优先使用时间段范围参数
    start_year = request.args.get('start_year')
    start_month = request.args.get('start_month')
    end_year = request.args.get('end_year')
    end_month = request.args.get('end_month')

    # 向后兼容旧参数
    if not start_year or not start_month:
        start_year = request.args.get('year')
        start_month = request.args.get('month')

    if not end_year or not end_month:
        end_year = start_year
        end_month = start_month

    if not start_year or not start_month or not end_year or not end_month:
        return ResponseHelper.validation_error_response('时间参数', '需要提供完整的时间范围参数')

    # 验证时间参数格式
    try:
        int(start_year)
        int(start_month)
        int(end_year)
        int(end_month)
    except ValueError:
        return ResponseHelper.validation_error_response('时间参数', '年份或月份格式不正确')

    # 仅构建针对 h 表的筛选条件，避免 year/month 误加到 h
    h_where_clause, h_params = _build_household_filters_for_missing()

    # 如果是单月份查询，使用原来的方法
    if start_year == end_year and start_month == end_month:
        import calendar
        try:
            days_in_month = calendar.monthrange(int(start_year), int(start_month))[1]
        except ValueError:
            return ResponseHelper.validation_error_response('时间参数', '年份或月份格式不正确')
        data = query_service.get_missing_days_statistics(start_year, start_month, days_in_month, h_where_clause, h_params)
    else:
        # 时间区间查询，使用新方法
        data = query_service.get_missing_days_statistics_range(start_year, start_month, end_year, end_month, h_where_clause, h_params)

    return ResponseHelper.success_response(data)

@statistics_bp.route('/api/statistics/available_filters')
@handle_api_exception
def get_available_filters():
    """获取可用的筛选选项，支持级联筛选"""
    town_filter = request.args.get('town')
    village_filter = request.args.get('village')
    
    # 获取年份和月份选项
    year_result = db.execute_query_safe("SELECT DISTINCT year FROM `调查点台账合并` WHERE year IS NOT NULL ORDER BY year")
    years = sorted([str(row[0]) for row in year_result if row[0] and str(row[0]).isdigit() and 2020 <= int(row[0]) <= 2030])

    month_result = db.execute_query_safe("SELECT DISTINCT month FROM `调查点台账合并` WHERE month IS NOT NULL ORDER BY month")
    months = sorted([str(row[0]).zfill(2) for row in month_result if row[0] and str(row[0]).isdigit() and 1 <= int(row[0]) <= 12])

    mapping = _get_town_village_mapping()
    towns = list(mapping['town_to_villages'].keys())

    # 根据乡镇筛选村庄
    if town_filter and town_filter in mapping['town_to_villages']:
        village_codes = mapping['town_to_villages'][town_filter]
        villages = [{'name': mapping['village_names'][code], 'code': code} for code in village_codes]
        villages.sort(key=lambda x: x['name'])  # 按村庄名称排序
    else:
        # 获取所有村庄
        villages = [{'name': name, 'code': code} for code, name in mapping['village_names'].items()]
        villages.sort(key=lambda x: x['name'])  # 按村庄名称排序

    # 根据村庄筛选户代码
    if village_filter:
        # village_filter现在是村代码，直接使用LEFT(t.hudm, 12)匹配
        household_result = db.execute_query_safe(
            """
            SELECT DISTINCT t.hudm, h.`户主姓名`
            FROM `调查点台账合并` t
            LEFT JOIN `调查点户名单` h ON t.hudm = h.`户代码`
            WHERE SUBSTR(t.hudm, 1, 12) = ?
            ORDER BY t.hudm
            """, [village_filter]
        )
    elif town_filter and town_filter in mapping['town_to_villages']:
        # 根据乡镇获取所有村庄代码，然后筛选户代码
        village_codes = mapping['town_to_villages'][town_filter]
        if village_codes:
            placeholders = ','.join(['?' for _ in village_codes])
            household_result = db.execute_query_safe(
                f"""
                SELECT DISTINCT t.hudm, h.`户主姓名`
                FROM `调查点台账合并` t
                LEFT JOIN `调查点户名单` h ON t.hudm = h.`户代码`
                WHERE SUBSTR(t.hudm, 1, 12) IN ({placeholders})
                ORDER BY t.hudm
                """, village_codes
            )
        else:
            household_result = []
    else:
        household_result = db.execute_query_safe(
            "SELECT DISTINCT t.hudm, h.`户主姓名` FROM `调查点台账合并` t LEFT JOIN `调查点户名单` h ON t.hudm = h.`户代码` ORDER BY t.hudm"
        )
    
    households = [{'code': row[0], 'name': row[1]} for row in household_result if row[0] and row[1]]

    filter_data = {
        'years': years, 
        'months': months, 
        'towns': towns, 
        'villages': villages, 
        'households': households
    }
    return ResponseHelper.success_response(filter_data)

@statistics_bp.route('/api/towns')
@handle_api_exception
def get_towns():
    """获取所有乡镇列表"""
    towns = query_service.get_all_towns()
    return ResponseHelper.success_response(towns)

@statistics_bp.route('/api/villages')
@handle_api_exception
def get_villages():
    """根据乡镇获取村居列表"""
    town_name = request.args.get('town')
    if not town_name:
        return ResponseHelper.error_response('缺少乡镇参数')
    
    villages = query_service.get_villages_by_town(town_name)
    return ResponseHelper.success_response(villages)

@statistics_bp.route('/api/statistics/refresh_cache', methods=['POST'])
@handle_api_exception
def refresh_statistics_cache():
    """刷新统计缓存"""
    success = query_service.refresh_statistics_cache()
    if success:
        return ResponseHelper.success_response({'message': '缓存刷新成功'})
    else:
        return ResponseHelper.error_response('缓存刷新失败')
