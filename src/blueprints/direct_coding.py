"""
直接匹配编码蓝图
提供基于type_name直接匹配的编码功能
"""

from flask import Blueprint, request, jsonify
import logging

logger = logging.getLogger(__name__)

# 创建蓝图
direct_coding_bp = Blueprint('direct_coding', __name__)

# 全局变量，在init_blueprint中初始化
db = None
direct_processor = None
handle_errors = None


def init_blueprint(database, processor, error_handler):
    """
    初始化蓝图依赖
    
    Args:
        database: 数据库连接对象
        processor: 直接匹配处理器
        error_handler: 错误处理装饰器
    """
    global db, direct_processor, handle_errors
    db = database
    direct_processor = processor
    handle_errors = error_handler


@direct_coding_bp.route('/api/direct_coding/statistics', methods=['GET'])
def get_mapping_statistics():
    """获取映射统计信息"""
    try:
        stats = direct_processor.get_mapping_statistics()
        return jsonify({
            'success': True,
            'data': stats,
            'message': '映射统计信息获取成功'
        })
    except Exception as e:
        logger.error(f"获取映射统计信息失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '获取映射统计信息失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/mapping_preview', methods=['GET'])
def get_mapping_preview():
    """获取映射预览"""
    try:
        limit = request.args.get('limit', 20, type=int)
        preview_mappings = direct_processor.get_mapping_preview(limit)
        
        return jsonify({
            'success': True,
            'data': {
                'preview_mappings': preview_mappings,
                'total_mappings': len(direct_processor.mapping_cache) if direct_processor.cache_loaded else 0
            },
            'message': '映射预览获取成功'
        })
    except Exception as e:
        logger.error(f"获取映射预览失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '获取映射预览失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/uncoded_count', methods=['POST'])
def get_uncoded_count():
    """获取未编码记录数量"""
    try:
        count = direct_processor.get_uncoded_count()
        return jsonify({
            'success': True,
            'data': {
                'count': count
            },
            'message': '未编码记录数量获取成功'
        })
    except Exception as e:
        logger.error(f"获取未编码记录数量失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '获取未编码记录数量失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/process', methods=['POST'])
def process_uncoded_records():
    """处理未编码记录"""
    try:
        data = request.get_json() or {}
        limit = data.get('limit', 1000)
        
        # 验证参数
        if not isinstance(limit, int) or limit <= 0:
            return jsonify({
                'success': False,
                'error': '无效的处理数量限制',
                'message': '处理数量限制必须是正整数'
            }), 400
        
        # 处理记录
        result = direct_processor.process_uncoded_records(limit)
        
        if result['success']:
            return jsonify({
                'success': True,
                'data': result,
                'message': result['message']
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', '未知错误'),
                'message': result['message']
            }), 500
            
    except Exception as e:
        logger.error(f"处理未编码记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '处理未编码记录失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/refresh_cache', methods=['POST'])
def refresh_mapping_cache():
    """刷新映射缓存"""
    try:
        success = direct_processor.refresh_cache()
        
        if success:
            stats = direct_processor.get_mapping_statistics()
            return jsonify({
                'success': True,
                'data': stats,
                'message': '映射缓存刷新成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '缓存刷新失败',
                'message': '映射缓存刷新失败'
            }), 500
            
    except Exception as e:
        logger.error(f"刷新映射缓存失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '刷新映射缓存失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/match_test', methods=['POST'])
def test_direct_match():
    """测试直接匹配"""
    try:
        data = request.get_json() or {}
        type_name = data.get('type_name', '').strip()
        
        if not type_name:
            return jsonify({
                'success': False,
                'error': '缺少type_name参数',
                'message': 'type_name参数不能为空'
            }), 400
        
        # 查找匹配
        matched_code = direct_processor.find_direct_match(type_name)
        
        return jsonify({
            'success': True,
            'data': {
                'type_name': type_name,
                'matched_code': matched_code,
                'has_match': matched_code is not None
            },
            'message': '匹配测试完成'
        })
        
    except Exception as e:
        logger.error(f"测试直接匹配失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '测试直接匹配失败'
        }), 500


@direct_coding_bp.route('/api/direct_coding/batch_process', methods=['POST'])
def batch_process_records():
    """批量处理记录"""
    try:
        data = request.get_json() or {}
        batch_size = data.get('batch_size', 500)
        max_batches = data.get('max_batches', 10)
        
        # 验证参数
        if not isinstance(batch_size, int) or batch_size <= 0:
            batch_size = 500
        if not isinstance(max_batches, int) or max_batches <= 0:
            max_batches = 10
        
        total_processed = 0
        total_matched = 0
        total_unmatched = 0
        batch_results = []
        
        for batch_num in range(max_batches):
            # 处理一批记录
            result = direct_processor.process_uncoded_records(batch_size)
            
            if not result['success']:
                break
            
            batch_results.append({
                'batch_number': batch_num + 1,
                'processed': result['total_records'],
                'matched': result['matched_records'],
                'unmatched': result['unmatched_records'],
                'match_rate': result['match_rate']
            })
            
            total_processed += result['total_records']
            total_matched += result['matched_records']
            total_unmatched += result['unmatched_records']
            
            # 如果这批没有记录了，说明处理完了
            if result['total_records'] == 0:
                break
        
        overall_match_rate = (total_matched / total_processed) * 100 if total_processed > 0 else 0
        
        return jsonify({
            'success': True,
            'data': {
                'total_processed': total_processed,
                'total_matched': total_matched,
                'total_unmatched': total_unmatched,
                'overall_match_rate': overall_match_rate,
                'batches_processed': len(batch_results),
                'batch_results': batch_results
            },
            'message': f'批量处理完成：共处理 {total_processed} 条记录，匹配 {total_matched} 条'
        })
        
    except Exception as e:
        logger.error(f"批量处理记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'message': '批量处理记录失败'
        }), 500
