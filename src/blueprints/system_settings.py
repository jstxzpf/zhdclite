"""
系统设置模块蓝图
- 清空调查点户名单
- 清空调查点村名单
- 清空当前账目数据（调查点台账合并）
- 备份/恢复 SQLite 数据库
"""

from flask import Blueprint, request, jsonify, send_file, current_app
import os
import io
import shutil
import logging
from datetime import datetime

from ..database_pool import get_connection_pool, close_connection_pool

system_settings_bp = Blueprint('system_settings', __name__)
logger = logging.getLogger(__name__)

# 运行时注入的依赖
_db = None
_handle_errors = None
_app_config = None


def init_blueprint(database, error_handler, app_config):
    global _db, _handle_errors, _app_config
    _db = database
    _handle_errors = error_handler
    _app_config = app_config


# 工具函数

def _db_path():
    # 优先从连接池获取数据库路径
    try:
        pool = get_connection_pool()
        return pool.db_path
    except Exception:
        # 回退到项目根目录 database.db
        return os.path.abspath(os.path.join(os.getcwd(), 'database.db'))


def _is_valid_sqlite(file_path: str) -> bool:
    try:
        with open(file_path, 'rb') as f:
            header = f.read(16)
        return header == b'SQLite format 3\x00'
    except Exception:
        return False


# ============ 删除数据 ============

@system_settings_bp.route('/api/system/clear-household-list', methods=['DELETE'])
def clear_household_list():
    @_handle_errors
    def _impl():
        logger.warning('请求清空: 调查点户名单')
        affected = 0
        try:
            with _db.pool.get_cursor() as cursor:
                cursor.execute('DELETE FROM 调查点户名单')
                affected = cursor.rowcount if cursor.rowcount != -1 else 0
        except Exception as e:
            logger.exception('清空 调查点户名单 失败')
            raise
        logger.warning(f'清空完成: 调查点户名单, 影响行数={affected}')
        return jsonify({'success': True, 'message': f'已清空调查点户名单（{affected} 行）。'})
    return _impl()


@system_settings_bp.route('/api/system/clear-village-list', methods=['DELETE'])
def clear_village_list():
    @_handle_errors
    def _impl():
        logger.warning('请求清空: 调查点村名单')
        affected = 0
        try:
            with _db.pool.get_cursor() as cursor:
                cursor.execute('DELETE FROM 调查点村名单')
                affected = cursor.rowcount if cursor.rowcount != -1 else 0
        except Exception:
            logger.exception('清空 调查点村名单 失败')
            raise
        logger.warning(f'清空完成: 调查点村名单, 影响行数={affected}')
        return jsonify({'success': True, 'message': f'已清空调查点村名单（{affected} 行）。'})
    return _impl()


@system_settings_bp.route('/api/system/clear-account-data', methods=['DELETE'])
def clear_account_data():
    @_handle_errors
    def _impl():
        logger.warning('请求清空: 调查点台账合并（当前账目数据）')
        affected = 0
        try:
            with _db.pool.get_cursor() as cursor:
                cursor.execute('DELETE FROM 调查点台账合并')
                affected = cursor.rowcount if cursor.rowcount != -1 else 0
        except Exception:
            logger.exception('清空 调查点台账合并 失败')
            raise
        logger.warning(f'清空完成: 调查点台账合并, 影响行数={affected}')
        return jsonify({'success': True, 'message': f'已清空当前账目数据（{affected} 行）。'})
    return _impl()


# ============ 备份/恢复 ============

@system_settings_bp.route('/api/system/backup-database', methods=['GET'])
def backup_database():
    @_handle_errors
    def _impl():
        db_file = _db_path()
        if not os.path.exists(db_file):
            return jsonify({'success': False, 'message': '未找到数据库文件'}), 404

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'database_backup_{ts}.db'
        upload_dir = os.path.abspath(_app_config.get('UPLOAD_FOLDER', 'uploads'))
        os.makedirs(upload_dir, exist_ok=True)
        backup_path = os.path.join(upload_dir, filename)

        logger.info(f'开始备份数据库: {db_file} -> {backup_path}')
        shutil.copy2(db_file, backup_path)
        logger.info('数据库备份完成')

        # 直接提供下载
        return send_file(
            backup_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/octet-stream'
        )
    return _impl()


@system_settings_bp.route('/api/system/restore-database', methods=['POST'])
def restore_database():
    @_handle_errors
    def _impl():
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': '未选择文件'}), 400
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'message': '未选择文件'}), 400
        if not (file.filename.lower().endswith('.db')):
            return jsonify({'success': False, 'message': '仅支持 .db SQLite 文件'}), 400

        upload_dir = os.path.abspath(_app_config.get('UPLOAD_FOLDER', 'uploads'))
        os.makedirs(upload_dir, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_restore_path = os.path.join(upload_dir, f'_restore_upload_{ts}.db')

        file.save(temp_restore_path)
        # 验证SQLite签名
        if not _is_valid_sqlite(temp_restore_path):
            try:
                os.remove(temp_restore_path)
            except Exception:
                pass
            return jsonify({'success': False, 'message': '上传的文件不是有效的SQLite数据库'}), 400

        db_file = _db_path()
        if not os.path.exists(db_file):
            # 如果当前数据库不存在，直接放置
            logger.info('当前数据库不存在，将直接放置上传的数据库文件')
            shutil.copy2(temp_restore_path, db_file)
            try:
                os.remove(temp_restore_path)
            except Exception:
                pass
            return jsonify({'success': True, 'message': '数据库恢复完成（新建放置）。'})

        # 备份现有数据库
        pre_backup_name = f'database_backup_before_restore_{ts}.db'
        pre_backup_path = os.path.join(upload_dir, pre_backup_name)

        logger.info(f'准备恢复数据库，先备份当前数据库到: {pre_backup_path}')

        # 关闭连接池，确保文件句柄释放
        try:
            close_connection_pool()
        except Exception:
            logger.warning('关闭连接池时出现警告，但将继续恢复流程', exc_info=True)

        shutil.copy2(db_file, pre_backup_path)
        logger.info('当前数据库备份完成')

        # 覆盖数据库
        logger.info('开始替换数据库文件')
        shutil.copy2(temp_restore_path, db_file)
        logger.info('数据库文件替换完成')

        # 重新初始化池到 _db
        try:
            _db.pool = get_connection_pool()
            # 简单连通性检查
            with _db.pool.get_cursor() as cursor:
                cursor.execute('SELECT 1')
                _ = cursor.fetchone()
        except Exception:
            logger.exception('恢复后数据库连通性检查失败')
            raise
        finally:
            try:
                os.remove(temp_restore_path)
            except Exception:
                pass

        logger.info('数据库恢复完成')
        return jsonify({'success': True, 'message': '数据库已恢复成功。'})
    return _impl()

