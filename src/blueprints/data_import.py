"""
数据导入模块蓝图
包含已编码数据、地方点数据、国家点数据的导入功能
"""

from flask import Blueprint, request
from werkzeug.utils import secure_filename
import os
import logging
import uuid
import pandas as pd
import re

# 创建蓝图
data_import_bp = Blueprint('data_import', __name__)
logger = logging.getLogger(__name__)

# 这些变量将在蓝图注册时从主应用传入
db = None
excel_ops = None
handle_errors = None
allowed_file = None
validate_file_size = None
app_config = None

def init_blueprint(database, excel_operations, error_handler, file_validator, size_validator, config):
    """初始化蓝图依赖"""
    global db, excel_ops, handle_errors, allowed_file, validate_file_size, app_config
    db = database
    excel_ops = excel_operations
    handle_errors = error_handler
    allowed_file = file_validator
    validate_file_size = size_validator
    app_config = config

def _cleanup_file(file_path):
    """清理临时文件"""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"临时文件已清理: {file_path}")
    except Exception as e:
        logger.warning(f"清理临时文件失败: {str(e)}")

def _read_and_process_csv(file_path):
    """封装了读取和预处理国家点CSV文件的逻辑"""
    # 尝试使用不同的编码读取CSV
    encodings_to_try = ['utf-8', 'gbk', 'gb2312']
    df = None
    for encoding in encodings_to_try:
        try:
            # 跳过第一行损坏的表头，并且不将数据的第一行作为表头
            df = pd.read_csv(file_path, encoding=encoding, header=None, skiprows=1, dtype=str)
            logger.info(f"成功使用 {encoding} 编码读取CSV文件，已跳过首行")
            break
        except UnicodeDecodeError:
            continue
    
    if df is None:
         raise ValueError("无法使用常用编码 (UTF-8, GBK) 读取CSV文件")

    if df.empty:
        logger.warning("CSV文件为空")
        raise ValueError("CSV文件为空")
    
    # 修正统计局CSV文件的字段
    df = _fix_statistical_csv_columns(df)
    logger.info("CSV文件字段修正完成")
    return df

def _process_uploaded_file(file, operation_name, allowed_extensions, read_func):
    """通用的文件上传、验证、保存和读取逻辑"""
    # 1. 检查文件是否存在
    if not file or file.filename == '':
        logger.warning(f"{operation_name}时文件名为空")
        return None, ("未选择文件", 400), None

    # 2. 文件扩展名安全验证
    file_ext = file.filename.rsplit('.', 1)[1].lower()
    if '.' not in file.filename or file_ext not in allowed_extensions:
        logger.warning(f"不支持的文件类型: {file.filename}")
        return None, (f"不支持的文件类型，仅支持 {', '.join(allowed_extensions)} 格式", 400), None

    # 3. 文件大小验证
    if not validate_file_size(file):
        logger.warning(f"文件大小超过限制: {file.filename}")
        return None, ("文件大小超过50MB限制", 400), None
    
    # 4. 安全处理文件名并确保唯一性
    filename = f"{uuid.uuid4().hex}_{secure_filename(file.filename)}"
    file_path = os.path.join(app_config['UPLOAD_FOLDER'], filename)
    
    try:
        # 5. 保存文件
        file.save(file_path)
        logger.info(f"文件保存成功: {file_path}")
        
        # 6. 使用回调函数读取和处理文件内容
        df = read_func(file_path)
        return df, None, file_path

    except Exception as e:
        # 统一处理保存或读取过程中的所有异常
        logger.error(f"{operation_name} 失败: {str(e)}")
        # 清理可能已创建的文件
        _cleanup_file(file_path)
        return None, (f"处理文件时出错: {str(e)}", 500), None

def _fix_statistical_csv_columns(df):
    """
    修正统计局CSV文件的字段问题：
    1. 根据新的23列字段结构分配正确字段名
    2. 清理无效数值
    3. 处理时间戳字段
    """
    logger.info(f"待修正的CSV文件原始列数: {len(df.columns)}")
    
    # 新的统计局CSV文件标准字段名（22个字段）
    new_standard_columns_22 = [
        'SID', '县码', '样本编码', '年', '月', '页码', '行码', '编码', '数量', '金额', 
        '数量2', '人码', '是否网购', '记账方式', '品名', '问题类型', '记账说明', 
        '记账审核说明', '记账日期', '创建时间', '更新时间', '账页生成设备标识'
    ]
    
    # 根据列数进行处理
    if len(df.columns) == 22:
        df.columns = new_standard_columns_22
        logger.info("检测到22列数据，成功分配字段名")
    elif len(df.columns) == 21:
        # 如果是21列，可能缺少最后一列
        df.columns = new_standard_columns_22[:-1]  # 使用前21个字段名
        # 添加缺失的最后一列
        df['账页生成设备标识'] = ''
        logger.info("检测到21列数据，成功分配字段名并添加缺失字段")
    else:
        logger.warning(f"列数不匹配: 期望22列，实际{len(df.columns)}列。将尝试按前22列处理。")
        if len(df.columns) > 22:
            df = df.iloc[:, :22]
            df.columns = new_standard_columns_22
        elif len(df.columns) < 22:
            # 如果列数不足，使用现有列数对应的字段名
            df.columns = new_standard_columns_22[:len(df.columns)]
            # 为缺失的列添加空值
            for i in range(len(df.columns), 22):
                df[new_standard_columns_22[i]] = ''
        logger.info(f"已调整列数并分配字段名")

    # 删除完全空白的行
    df = df.dropna(how='all')
    
    # 清理无效的数值数据，将 'n.n' 等非数字值转换为空值 (NaN)
    numeric_columns = ['数量', '金额', '数量2']
    for col in numeric_columns:
        if col in df.columns:
            # 使用 to_numeric 将所有非数字值（包括 'n.n'）强制转换成 NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("已将数值列中的 'n.n' 等无效值转换为空值")

    # 规范化“编码”列为6位纯数字字符串（修复如 311018.0 -> 311018）
    if '编码' in df.columns:
        def _normalize_code(val):
            if pd.isna(val):
                return ''
            s = str(val).strip()
            # 提取所有数字
            digits = ''.join(re.findall(r'\d', s))
            if not digits:
                return ''
            # 截断或补齐为6位
            if len(digits) >= 6:
                digits = digits[:6]
            else:
                digits = digits.zfill(6)
            return digits
        df['编码'] = df['编码'].apply(_normalize_code)
        logger.info("已规范化‘编码’列为6位纯数字字符串")

    # 处理日期时间字段
    if '记账日期' in df.columns:
        # 如果创建时间为空，使用记账日期
        if '创建时间' in df.columns:
            df['创建时间'] = df['创建时间'].fillna(df['记账日期'])
        else:
            df['创建时间'] = df['记账日期']
        logger.info("已处理日期时间字段")
    
    # 处理字段名映射：人码 -> 人代码（为了兼容后续处理）
    if '人码' in df.columns:
        df['人代码'] = df['人码']
        logger.info("已添加人代码字段映射")
    
    logger.info(f"修正后的CSV数据形状: {df.shape}")
    logger.info(f"修正后的列名: {list(df.columns)}")
    
    return df



def _get_next_id_range(data_source, record_count):
    """获取下一个可用的ID范围"""
    id_ranges = {
        'national': (1000000000, 1999999999)
    }
    if data_source not in id_ranges:
        raise ValueError(f"不支持的数据源类型: {data_source}")
    range_start, range_end = id_ranges[data_source]
    max_id_result = db.execute_query_safe(
        "SELECT ISNULL(MAX(id), ?) FROM 调查点台账合并 WHERE id BETWEEN ? AND ?",
        (range_start - 1, range_start, range_end)
    )
    max_existing_id = max_id_result[0][0] if max_id_result and max_id_result[0] is not None else range_start - 1
    start_id = max_existing_id + 1
    end_id = start_id + record_count - 1
    if end_id > range_end:
        raise ValueError(f"{data_source}数据ID范围已满，无法分配{record_count}个新ID")
    logger.info(f"{data_source}数据分配ID范围: {start_id} - {end_id}")
    return start_id, end_id







@data_import_bp.route('/import_national_data', methods=['POST'])
def import_national_data():
    """导入国家点数据 (CSV格式)"""
    @handle_errors
    def _import_national_data():
        logger.info("开始导入国家点数据 (CSV格式)")
        if 'file' not in request.files:
            return "未选择文件", 400

        file = request.files['file']
        df, error, file_path = _process_uploaded_file(
            file, "导入国家点数据", {'csv'}, _read_and_process_csv
        )

        if error:
            return error[0], error[1]
        
        try:
            required_columns = ['SID', '编码', '品名', '人码', '创建时间']
            if not all(col in df.columns for col in required_columns):
                return f"CSV文件缺少必需的列: {[c for c in required_columns if c not in df.columns]}", 400

            import_result = db.import_data(df, '国家点待导入')
            temp_count = import_result['successful_rows']
            logger.info(f"国家点数据成功入库到临时表，共 {temp_count} 条记录")

            if temp_count == 0:
                return "没有新的国家点数据需要导入。", 200

            # 数据验证：检查有效记录数
            logger.info("开始验证国家点数据")
            valid_record_result = db.execute_query_safe("""
                SELECT COUNT(*) FROM 国家点待导入
                WHERE ([SID] IS NOT NULL AND [SID] <> '')
                    AND (TRY_CONVERT(DATETIME, [创建时间], 120) IS NOT NULL)
                    AND ([编码] IS NOT NULL AND [编码] <> '')
                    AND ([品名] IS NOT NULL AND [品名] <> '')
                    AND (([人码] IS NOT NULL AND [人码] <> '') OR ([人代码] IS NOT NULL AND [人代码] <> ''))
            """)
            valid_record_count = valid_record_result[0][0] if valid_record_result else 0
            logger.info(f"有效记录数: {valid_record_count}")

            inserted_count = 0
            updated_count = 0
            type_updated_count = 0

            if valid_record_count > 0:
                # 获取国家点数据的ID分配范围
                national_id_start, _ = _get_next_id_range('national', valid_record_count)
                logger.info(f"国家点数据分配ID范围起始: {national_id_start}")

                # 插入数据到调查点台账合并表
                logger.info("开始插入国家点数据到主表")
                insert_sql = f'''
                INSERT INTO [调查点台账合并] (
                    hudm, code, amount, money, note, person, year, month, z_guid, date,
                    type, id, type_name, unit_name, ybm, ybz, wton, ntow
                )
                SELECT
                    LEFT([SID], 12) + LEFT(RIGHT([SID], 5), 3) AS hudm,
                    CAST([编码] AS VARCHAR(50)) AS code,
                    [数量] AS amount,
                    [金额] AS money,
                    CAST(ISNULL([记账说明], '') AS VARCHAR(255)) AS note,
                    CAST(ISNULL([人码], ISNULL([人代码], '')) AS VARCHAR(255)) AS person,
                    CAST(ISNULL([年], YEAR(TRY_CONVERT(DATETIME, [创建时间], 120))) AS VARCHAR(4)) AS year,
                    RIGHT('0' + CAST(ISNULL([月], MONTH(TRY_CONVERT(DATETIME, [创建时间], 120))) AS VARCHAR(2)), 2) AS month,
                    NEWID() AS z_guid,
                    TRY_CONVERT(SMALLDATETIME, [创建时间], 120) AS date,
                    0 AS type,
                    {national_id_start} + ROW_NUMBER() OVER (ORDER BY [SID]) - 1 AS id,
                    CAST([品名] AS VARCHAR(255)) AS type_name,
                    CAST('' AS VARCHAR(255)) AS unit_name,
                    CAST('' AS VARCHAR(1)) AS ybm,
                    CAST('1' AS VARCHAR(1)) AS ybz,
                    CAST('1' AS VARCHAR(1)) AS wton,
                    CAST('0' AS VARCHAR(1)) AS ntow
                FROM 国家点待导入
                WHERE
                    ([SID] IS NOT NULL AND [SID] <> '') AND
                    (TRY_CONVERT(DATETIME, [创建时间], 120) IS NOT NULL) AND
                    ([编码] IS NOT NULL AND [编码] <> '') AND
                    ([品名] IS NOT NULL AND [品名] <> '') AND
                    (([人码] IS NOT NULL AND [人码] <> '') OR ([人代码] IS NOT NULL AND [人代码] <> ''))
                '''

                # 使用事务方式执行所有SQL操作
                with db.pool.get_cursor() as cursor:
                    cursor.execute(insert_sql)
                    inserted_count = cursor.rowcount
                    logger.info(f"国家点数据成功合并到主表，共插入 {inserted_count} 条记录")

                    # 更新编码匹配信息
                    if inserted_count > 0:
                        update_sql = f'''UPDATE [调查点台账合并]
                            SET type_name = c.帐目指标名称, unit_name = c.单位名称
                            FROM [调查点台账合并] t INNER JOIN [调查品种编码] c ON t.code = c.帐目编码
                            WHERE t.code IS NOT NULL AND t.ybz='1' AND t.id >= {national_id_start}'''
                        cursor.execute(update_sql)
                        updated_count = cursor.rowcount
                        logger.info(f"国家点数据编码匹配完成，共更新 {updated_count} 条记录")

                        # 更新收支类别
                        type_update_sql = f'''UPDATE [调查点台账合并]
                            SET type = CAST(c.收支类别 AS INT)
                            FROM [调查点台账合并] t INNER JOIN [调查品种编码] c ON t.code = c.帐目编码
                            WHERE t.id >= {national_id_start} AND t.code IS NOT NULL AND c.收支类别 IS NOT NULL'''
                        cursor.execute(type_update_sql)
                        type_updated_count = cursor.rowcount
                        logger.info(f"收支类别自动填充完成，共更新 {type_updated_count} 条记录的type字段")

            # 构建返回消息
            summary_message = f"国家点数据导入完成！\n"
            summary_message += f"• 导入到临时表：{temp_count} 条\n"
            summary_message += f"• 插入到主表：{inserted_count} 条\n"
            summary_message += f"• 编码匹配更新：{updated_count} 条\n"
            summary_message += f"• 收支类别填充：{type_updated_count} 条\n"

            invalid_count = temp_count - valid_record_count
            if invalid_count > 0:
                summary_message += f"• 无效记录（未导入）：{invalid_count} 条"

            return summary_message
        finally:
            _cleanup_file(file_path)
    return _import_national_data()