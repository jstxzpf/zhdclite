"""
数据库同步模块蓝图
包含外部到内部数据库同步功能
"""

from flask import Blueprint, jsonify, request
import logging

# 创建蓝图
database_sync_bp = Blueprint('database_sync', __name__)
logger = logging.getLogger(__name__)

# 这些变量将在蓝图注册时从主应用传入
db = None
handle_errors = None

def init_blueprint(database, error_handler):
    """初始化蓝图依赖"""
    global db, handle_errors
    db = database
    handle_errors = error_handler

@database_sync_bp.route('/sync_external_db', methods=['POST'])
def sync_external_db():
    """同步外部数据库"""
    @handle_errors
    def _sync_external_db():
        logger.info("开始同步外部数据库")
        
        try:
            # 执行同步操作
            success = db.sync_external_db()
            if success:
                # 将外部待导入表中的数据合并到调查点台账合并表中
                result = db.execute_query_safe("SELECT COUNT(*) FROM 外部待导入")
                before_count = result[0][0] if result else 0
                logger.info(f"外部待导入表记录数: {before_count}")
                
                # 检查表是否有标识列，并相应地处理数据导入
                has_identity = False  # 初始化变量
                try:
                    # 检查目标表是否有标识列
                    has_identity = db.check_table_has_identity_column('调查点台账合并')

                    # 在单一事务中完成所有IDENTITY_INSERT相关操作，确保原子性
                    inserted_ids = []
                    with db.pool.get_cursor() as cursor:
                        if has_identity:
                            logger.info("目标表有标识列，启用IDENTITY_INSERT")
                            cursor.execute("SET IDENTITY_INSERT 调查点台账合并 ON")
                        else:
                            logger.info("目标表没有标识列，使用普通INSERT")

                        # 执行数据插入（仅插入尚未存在于主表的记录）
                        cursor.execute("""
                            INSERT INTO 调查点台账合并 (
                                hudm, code, amount, money, note, person, year, month,
                                z_guid, date, type, id, type_name, unit_name, ybm, ybz, wton, ntow
                            )
                            SELECT
                                hudm, code, amount, money, note, person, year, month,
                                z_guid, date, type, id, type_name, unit_name, ybm, ybz,
                                ISNULL(wton, '0') as wton,
                                ISNULL(ntow, '0') as ntow
                            FROM 外部待导入
                            WHERE id NOT IN (SELECT id FROM 调查点台账合并)
                        """)

                        # 获取实际插入的记录数
                        inserted_count = cursor.rowcount if cursor.rowcount != -1 else 0
                        logger.info(f"成功插入 {inserted_count} 条记录")

                        # 收集本次实际插入的 id（从外部待导入表获取）
                        ids_rows = db.execute_query_safe("SELECT id FROM 外部待导入")
                        inserted_ids = [row[0] for row in ids_rows] if ids_rows else []

                        if has_identity:
                            # 关闭身份列插入
                            cursor.execute("SET IDENTITY_INSERT 调查点台账合并 OFF")
                            logger.info("IDENTITY_INSERT已关闭")
                    logger.info("数据导入成功")

                    # 在外部库将这些 id 标记为已同步（wton='1'）
                    try:
                        updated = db.mark_external_records_synced(inserted_ids)
                        logger.info(f"外部库标记 wton=1 完成，更新 {updated} 条")
                    except Exception as mark_err:
                        logger.error(f"标记外部库 wton=1 失败: {mark_err}")

                except Exception as insert_error:
                    logger.error(f"数据导入失败: {str(insert_error)}")
                    # 注意：由于使用单一事务，IDENTITY_INSERT会随事务回滚自动重置
                    # 连接池的上下文管理器会自动处理事务回滚
                    raise
                
                # 统计导入后结果
                result = db.execute_query_safe("SELECT COUNT(*) FROM 调查点台账合并")
                after_count = result[0][0] if result else 0
                
                logger.info(f"外部数据库同步完成 - 同步记录数: {before_count}, 总记录数: {after_count}")
                return f"外部数据库同步完成！从外部数据库成功同步 {before_count} 条记录，现有记录总数: {after_count}"
            else:
                logger.warning("同步过程未完成")
                return "同步过程未完成，请检查日志"
        except Exception as e:
            logger.error(f"同步过程发生错误: {str(e)}")
            # 注意：连接池会自动处理事务回滚
            raise
    
    return _sync_external_db()



@database_sync_bp.route('/sync_external_to_internal', methods=['POST'])
def sync_external_to_internal():
    """外部到内部数据同步"""
    @handle_errors
    def _sync_external_to_internal():
        logger.info("开始外部到内部数据同步")

        try:
            # 建立双数据库连接
            db.connect_both_databases()

            # 执行外部到内部同步
            count = db.sync_external_to_internal()

            # 提交事务
            db.commit_both()

            response_data = {
                'success': True,
                'message': f'外部到内部同步完成，成功同步 {count} 条记录',
                'synced_count': count
            }

            logger.info(response_data['message'])
            return jsonify(response_data)

        except Exception as e:
            logger.error(f"外部到内部同步失败: {str(e)}")
            db.rollback_both()
            response_data = {
                'success': False,
                'message': f'外部到内部同步失败: {str(e)}',
                'error_details': str(e)
            }
            return jsonify(response_data), 500
        finally:
            db.close_both_databases()

    return _sync_external_to_internal()





@database_sync_bp.route('/sync_status', methods=['GET'])
def get_sync_status():
    """获取同步状态"""
    @handle_errors
    def _get_sync_status():
        logger.info("获取同步状态")

        try:
            # 获取同步统计信息
            stats = db.get_sync_statistics()

            response_data = {
                'success': True,
                'message': '同步状态获取成功',
                'statistics': stats
            }

            return jsonify(response_data)

        except Exception as e:
            logger.error(f"获取同步状态失败: {str(e)}")
            response_data = {
                'success': False,
                'message': f'获取同步状态失败: {str(e)}',
                'error_details': str(e)
            }
            return jsonify(response_data), 500

    return _get_sync_status()
