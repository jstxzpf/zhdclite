"""
数据导入模块蓝图
包含已编码数据、地方点数据、国家点数据的导入功能
以及调查点户名单的导入导出功能
"""

from flask import Blueprint, request, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import logging
import uuid
import pandas as pd
import re
from datetime import datetime

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
        "SELECT COALESCE(MAX(id), ?) FROM 调查点台账合并 WHERE id BETWEEN ? AND ?",
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
                    AND ([创建时间] IS NOT NULL AND TRIM([创建时间]) <> '')
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

                # 插入数据到调查点台账合并表（改为：从临时表 SELECT 到 DataFrame，在 Python 端预处理并 executemany 插入）
                logger.info("开始插入国家点数据到主表（Python 端预处理）")

                # 1) 从临时表读取有效记录
                select_sql = """
                SELECT [SID], [编码], [数量], [金额], [记账说明], [人码], [人代码], [年], [月], [创建时间], [品名]
                FROM 国家点待导入
                WHERE ([SID] IS NOT NULL AND TRIM([SID]) <> '')
                  AND ([创建时间] IS NOT NULL AND TRIM([创建时间]) <> '')
                  AND ([编码] IS NOT NULL AND TRIM([编码]) <> '')
                  AND ([品名] IS NOT NULL AND TRIM([品名]) <> '')
                  AND (([人码] IS NOT NULL AND TRIM([人码]) <> '') OR ([人代码] IS NOT NULL AND TRIM([人代码]) <> ''))
                """
                rows = db.execute_query_safe(select_sql)

                if not rows:
                    logger.info("没有符合条件的记录可插入主表。")
                    inserted_count = 0
                else:
                    # 2) 转为 DataFrame 并进行字段预处理
                    records = [dict(r) for r in rows]
                    df_temp = pd.DataFrame.from_records(records)

                    # 统一字符串类型并填充缺失
                    for col in ['SID','编码','数量','金额','记账说明','人码','人代码','年','月','创建时间','品名']:
                        if col not in df_temp.columns:
                            df_temp[col] = ''
                        df_temp[col] = df_temp[col].astype(str).fillna('').str.strip()

                    # 生成 hudm: 前12位 + (末5位的前3位)
                    def build_hudm(sid: str) -> str:
                        if not sid:
                            return ''
                        head12 = sid[:12]
                        tail5_first3 = sid[-5:][:3] if len(sid) >= 5 else ''
                        return head12 + tail5_first3

                    df_temp['hudm'] = df_temp['SID'].apply(build_hudm)

                    # 选择 person 字段：人码 或 人代码
                    df_temp['person'] = df_temp.apply(lambda r: r['人码'] if r['人码'] else (r['人代码'] if r['人代码'] else ''), axis=1)

                    # 解析年份与月份（优先使用 年/月，否则从 创建时间 推断）
                    ts = pd.to_datetime(df_temp['创建时间'], errors='coerce')
                    df_temp['year'] = df_temp.apply(
                        lambda r: r['年'] if r['年'] else (str(ts[r.name].year) if pd.notna(ts[r.name]) else ''), axis=1
                    )
                    df_temp['month'] = df_temp.apply(
                        lambda r: r['月'] if r['月'] else (str(ts[r.name].month).zfill(2) if pd.notna(ts[r.name]) else ''), axis=1
                    )

                    # 生成 z_guid、type、id、固定值列
                    import uuid as _uuid
                    df_temp['z_guid'] = [ _uuid.uuid4().hex for _ in range(len(df_temp)) ]
                    df_temp['type'] = 0
                    df_temp['id'] = list(range(national_id_start, national_id_start + len(df_temp)))
                    df_temp['type_name'] = df_temp['品名']
                    df_temp['unit_name'] = ''
                    df_temp['ybm'] = ''
                    df_temp['ybz'] = '1'
                    df_temp['wton'] = '1'
                    df_temp['ntow'] = '0'

                    # 构建插入所需列顺序
                    insert_columns = [
                        'hudm','编码','数量','金额','记账说明','person','year','month','z_guid','创建时间',
                        'type','id','type_name','unit_name','ybm','ybz','wton','ntow'
                    ]
                    # 将金额/数量保持原值（如需数值化可在此转换）

                    # 3) executemany 插入
                    insert_sql = (
                        "INSERT INTO 调查点台账合并 ("
                        "hudm, code, amount, money, note, person, year, month, z_guid, date, "
                        "type, id, type_name, unit_name, ybm, ybz, wton, ntow"
                        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    )

                    values = [
                        (
                            r['hudm'], r['编码'], r['数量'], r['金额'], r['记账说明'], r['person'], r['year'], r['month'],
                            r['z_guid'], r['创建时间'], r['type'], r['id'], r['type_name'], r['unit_name'], r['ybm'], r['ybz'], r['wton'], r['ntow']
                        )
                        for _, r in df_temp.iterrows()
                    ]

                    with db.pool.get_cursor() as cursor:
                        cursor.executemany(insert_sql, values)
                        inserted_count = len(values)
                        logger.info(f"国家点数据成功合并到主表，共插入 {inserted_count} 条记录")

                    # 更新编码匹配信息
                    if inserted_count > 0:
                        update_sql = f'''UPDATE 调查点台账合并
                            SET type_name = (
                                SELECT c.帐目指标名称 FROM 调查品种编码 c WHERE c.帐目编码 = 调查点台账合并.code
                            ),
                                unit_name = (
                                SELECT c.单位名称 FROM 调查品种编码 c WHERE c.帐目编码 = 调查点台账合并.code
                            )
                            WHERE code IS NOT NULL AND ybz='1' AND id >= {national_id_start}
                        '''
                        cursor.execute(update_sql)
                        updated_count = cursor.rowcount
                        logger.info(f"国家点数据编码匹配完成，共更新 {updated_count} 条记录")

                        # 更新收支类别
                        type_update_sql = f'''UPDATE 调查点台账合并
                            SET type = (
                                SELECT CAST(c.收支类别 AS INTEGER) FROM 调查品种编码 c WHERE c.帐目编码 = 调查点台账合并.code
                            )
                            WHERE id >= {national_id_start} AND code IS NOT NULL AND (
                                SELECT c.收支类别 FROM 调查品种编码 c WHERE c.帐目编码 = 调查点台账合并.code
                            ) IS NOT NULL'''
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


def _read_household_excel(file_path):
    """读取调查点户名单Excel文件"""
    try:
        # 使用excel_ops读取Excel文件
        df = excel_ops.read_excel(file_path)
        logger.info(f"成功读取调查点户名单Excel文件，共 {len(df)} 行数据")

        # 验证必需的列
        required_columns = ['户代码', '户主姓名']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Excel文件缺少必需的列: {missing_columns}")

        # 清理数据
        df = df.dropna(subset=['户代码', '户主姓名'])  # 删除关键字段为空的行
        df = df.drop_duplicates(subset=['户代码'])  # 删除重复的户代码

        # 确保数据类型正确
        df['户代码'] = df['户代码'].astype(str).str.strip()
        df['户主姓名'] = df['户主姓名'].astype(str).str.strip()

        # 处理可选字段
        if '人数' not in df.columns:
            df['人数'] = 1
        else:
            df['人数'] = pd.to_numeric(df['人数'], errors='coerce').fillna(1).astype(int)

        if '所在乡镇街道' not in df.columns:
            df['所在乡镇街道'] = ''
        else:
            df['所在乡镇街道'] = df['所在乡镇街道'].astype(str).fillna('').str.strip()

        if '村居名称' not in df.columns:
            df['村居名称'] = ''
        else:
            df['村居名称'] = df['村居名称'].astype(str).fillna('').str.strip()

        # 处理时间字段（若存在，则解析为标准格式；否则保持缺省以便导入阶段决定）
        for ts_col in ['创建时间', '更新时间']:
            if ts_col in df.columns:
                # 使用 pandas 解析为 datetime，无法解析的置为 NaT
                parsed = pd.to_datetime(df[ts_col], errors='coerce')
                # 格式化为统一字符串，无法解析的保留为 None
                df[ts_col] = parsed.dt.strftime('%Y-%m-%d %H:%M:%S')

        logger.info(f"调查点户名单数据清理完成，有效数据 {len(df)} 行")
        return df

    except Exception as e:
        logger.error(f"读取调查点户名单Excel文件失败: {str(e)}")
        raise


@data_import_bp.route('/export_household_list', methods=['GET'])
def export_household_list():
    """导出调查点户名单到Excel"""
    @handle_errors
    def _export_household_list():
        logger.info("开始导出调查点户名单")

        try:
            # 使用已验证的SQL查询，确保正确回填所在乡镇街道和村居名称
            sql = """
            SELECT
                h.户代码,
                h.户主姓名,
                h.人数,
                CASE
                    WHEN h.所在乡镇街道 IS NOT NULL AND TRIM(h.所在乡镇街道) != '' THEN h.所在乡镇街道
                    WHEN v.所在乡镇街道 IS NOT NULL AND TRIM(v.所在乡镇街道) != '' THEN v.所在乡镇街道
                    WHEN m.所在乡镇街道 IS NOT NULL AND TRIM(m.所在乡镇街道) != '' THEN m.所在乡镇街道
                    ELSE ''
                END AS 所在乡镇街道,
                CASE
                    WHEN h.村居名称 IS NOT NULL AND TRIM(h.村居名称) != '' THEN h.村居名称
                    WHEN v.村居名称 IS NOT NULL AND TRIM(v.村居名称) != '' THEN v.村居名称
                    WHEN m.村居名称 IS NOT NULL AND TRIM(m.村居名称) != '' THEN m.村居名称
                    WHEN h.调查小区名称 IS NOT NULL AND TRIM(h.调查小区名称) != '' THEN h.调查小区名称
                    ELSE ''
                END AS 村居名称,
                h.创建时间,
                h.更新时间,
                h.密码,
                h.调查小区名称,
                h.城乡属性,
                h.住宅地址,
                h.家庭人口,
                h.是否退出
            FROM 调查点户名单 h
            LEFT JOIN v_town_village_list v ON SUBSTR(h.户代码, 1, 12) = v.村代码
            LEFT JOIN 调查点村名单 m ON SUBSTR(h.户代码, 1, 12) = m.户代码前12位
            ORDER BY h.户代码
            """

            result = db.execute_query_safe(sql)

            if not result:
                return jsonify({
                    'success': False,
                    'message': '没有找到调查点户名单数据'
                }), 404

            # 定义列名（与SQL SELECT顺序一致）
            columns = ['户代码', '户主姓名', '人数', '所在乡镇街道', '村居名称', '创建时间', '更新时间',
                      '密码', '调查小区名称', '城乡属性', '住宅地址', '家庭人口', '是否退出']

            df = pd.DataFrame(result, columns=columns)

            # 3.1) 额外回填兜底：
            # - 若“村居名称”仍为空，优先用“调查小区名称”补上
            # - 若“所在乡镇街道”仍为空，尝试从“住宅地址”提取（如：xxx街道办事处 / xxx镇 / xxx乡）
            def _is_empty(x):
                return x is None or (isinstance(x, str) and x.strip() == '')

            def _extract_town(addr: str) -> str:
                if not isinstance(addr, str):
                    return ''
                addr = addr.strip()
                for kw in ['街道办事处', '街道', '镇', '乡']:
                    i = addr.find(kw)
                    if i != -1:
                        return addr[:i + len(kw)]
                return ''

            # 额外回填已经在SQL中处理，这里不再需要
            # if '村居名称' in df.columns and '调查小区名称' in df.columns:
            #     df['村居名称'] = df['村居名称'].where(~df['村居名称'].apply(_is_empty), df['调查小区名称'])

            # if '所在乡镇街道' in df.columns and '住宅地址' in df.columns:
            #     df['所在乡镇街道'] = df.apply(
            #         lambda r: r['所在乡镇街道'] if not _is_empty(r.get('所在乡镇街道')) else _extract_town(r.get('住宅地址')), axis=1
            #     )

            # 4) 生成文件
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"调查点户名单_{timestamp}.xlsx"

            upload_dir = os.path.abspath(app_config['UPLOAD_FOLDER'])
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir, exist_ok=True)

            file_path = os.path.join(upload_dir, filename)

            # 使用excel_ops保存Excel文件
            excel_ops._save_df_to_excel(df, file_path, '调查点户名单')

            logger.info(f"调查点户名单导出成功: {file_path}")

            # 返回文件供下载
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        except Exception as e:
            logger.error(f"导出调查点户名单失败: {str(e)}")
            return jsonify({
                'success': False,
                'message': f'导出失败: {str(e)}'
            }), 500

    return _export_household_list()


@data_import_bp.route('/import_household_list', methods=['POST'])
def import_household_list():
    """导入调查点户名单Excel文件"""
    @handle_errors
    def _import_household_list():
        logger.info("开始导入调查点户名单")

        if 'file' not in request.files:
            return "未选择文件", 400

        file = request.files['file']
        df, error, file_path = _process_uploaded_file(
            file, "导入调查点户名单", {'xlsx', 'xls'}, _read_household_excel
        )

        if error:
            return error[0], error[1]

        try:
            if df.empty:
                return "Excel文件中没有有效数据", 400

            # 统计信息
            total_rows = len(df)
            new_count = 0
            updated_count = 0
            error_count = 0
            error_details = []

            logger.info(f"开始处理 {total_rows} 条调查点户名单记录")

            # 逐行处理数据，使用UPSERT操作
            with db.pool.get_cursor() as cursor:
                for index, row in df.iterrows():
                    try:
                        户代码 = str(row['户代码']).strip()
                        户主姓名 = str(row['户主姓名']).strip()
                        人数 = int(row['人数']) if pd.notna(row['人数']) else 1
                        所在乡镇街道 = str(row['所在乡镇街道']).strip() if pd.notna(row['所在乡镇街道']) else ''
                        村居名称 = str(row['村居名称']).strip() if pd.notna(row['村居名称']) else ''

                        # 验证必需字段
                        if not 户代码 or not 户主姓名:
                            error_count += 1
                            error_details.append(f"第{index+2}行: 户代码或户主姓名为空")
                            continue

                        # 检查记录是否已存在
                        check_sql = "SELECT COUNT(*) FROM 调查点户名单 WHERE 户代码 = ?"
                        cursor.execute(check_sql, (户代码,))
                        exists = cursor.fetchone()[0] > 0

                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # 支持从Excel读取的时间字段（若提供）
                        提供创建时间 = ('创建时间' in df.columns and pd.notna(row.get('创建时间')))
                        提供更新时间 = ('更新时间' in df.columns and pd.notna(row.get('更新时间')))
                        创建时间值 = str(row.get('创建时间')) if 提供创建时间 else current_time
                        更新时间值 = str(row.get('更新时间')) if 提供更新时间 else current_time

                        if exists:
                            # 更新现有记录（不修改创建时间）
                            update_sql = """
                            UPDATE 调查点户名单
                            SET 户主姓名 = ?, 人数 = ?, 所在乡镇街道 = ?, 村居名称 = ?, 更新时间 = ?
                            WHERE 户代码 = ?
                            """
                            cursor.execute(update_sql, (户主姓名, 人数, 所在乡镇街道, 村居名称, 更新时间值, 户代码))
                            updated_count += 1
                            logger.debug(f"更新户代码 {户代码} 的记录")
                        else:
                            # 插入新记录（若Excel提供则使用提供的时间）
                            insert_sql = """
                            INSERT INTO 调查点户名单 (户代码, 户主姓名, 人数, 所在乡镇街道, 村居名称, 创建时间, 更新时间)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """
                            cursor.execute(insert_sql, (户代码, 户主姓名, 人数, 所在乡镇街道, 村居名称, 创建时间值, 更新时间值))
                            new_count += 1
                            logger.debug(f"插入新户代码 {户代码} 的记录")

                    except Exception as e:
                        error_count += 1
                        error_details.append(f"第{index+2}行处理失败: {str(e)}")
                        logger.warning(f"处理第{index+2}行数据失败: {str(e)}")
                        continue

            # 构建返回消息
            summary_message = f"调查点户名单导入完成！\n"
            summary_message += f"• 总处理记录数：{total_rows} 条\n"
            summary_message += f"• 新增记录：{new_count} 条\n"
            summary_message += f"• 更新记录：{updated_count} 条\n"

            if error_count > 0:
                summary_message += f"• 错误记录：{error_count} 条\n"
                if error_details:
                    summary_message += f"• 错误详情：\n"
                    # 只显示前5个错误详情，避免消息过长
                    for detail in error_details[:5]:
                        summary_message += f"  - {detail}\n"
                    if len(error_details) > 5:
                        summary_message += f"  - 还有 {len(error_details) - 5} 个错误未显示\n"

            logger.info(f"调查点户名单导入完成 - 新增: {new_count}, 更新: {updated_count}, 错误: {error_count}")
            return summary_message

        except Exception as e:
            logger.error(f"导入调查点户名单过程中发生错误: {str(e)}")
            return f"导入过程中发生错误: {str(e)}", 500
        finally:
            _cleanup_file(file_path)

    return _import_household_list()


# =============================
# 调查点村名单 管理（导出/导入）
# 参照“调查点户名单管理”的实现风格
# =============================

def _read_village_list_excel(file_path):
    """读取调查点村名单Excel文件，并进行基础清洗与校验"""
    try:
        df = excel_ops.read_excel(file_path)
        logger.info(f"成功读取调查点村名单Excel文件，共 {len(df)} 行数据")

        # 必需字段
        required_columns = ['户代码前12位', '所在乡镇街道', '村居名称']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Excel文件缺少必需的列: {missing_columns}")

        # 清理关键字段
        df['户代码前12位'] = df['户代码前12位'].astype(str).str.strip()
        df['所在乡镇街道'] = df['所在乡镇街道'].astype(str).fillna('').str.strip()
        df['村居名称'] = df['村居名称'].astype(str).fillna('').str.strip()

        # 可选字段
        optional_text_cols = ['调查点类型', '调查员姓名', '调查员电话', '城乡属性']
        for col in optional_text_cols:
            if col not in df.columns:
                df[col] = ''
            else:
                df[col] = df[col].astype(str).fillna('').str.strip()

        # 数量列（可选，数值）
        if '数量' not in df.columns:
            df['数量'] = None
        else:
            df['数量'] = pd.to_numeric(df['数量'], errors='coerce')

        # 删除关键字段为空的行
        df = df.dropna(subset=['户代码前12位'])
        df = df[df['户代码前12位'].str.len() > 0]

        # 去重（以“户代码前12位”为主键）
        df = df.drop_duplicates(subset=['户代码前12位'])

        logger.info(f"调查点村名单数据清理完成，有效数据 {len(df)} 行")
        return df
    except Exception as e:
        logger.error(f"读取调查点村名单Excel文件失败: {str(e)}")
        raise


@data_import_bp.route('/export_village_list', methods=['GET'])
def export_village_list():
    """导出调查点村名单到Excel"""
    @handle_errors
    def _export_village_list():
        logger.info("开始导出调查点村名单")
        try:
            sql = """
            SELECT 户代码前12位, 数量, 调查点类型, 所在乡镇街道, 村居名称, 调查员姓名, 调查员电话, 城乡属性
            FROM 调查点村名单
            ORDER BY 户代码前12位
            """
            result = db.execute_query_safe(sql)
            if not result:
                return jsonify({'success': False, 'message': '没有找到调查点村名单数据'}), 404

            columns = ['户代码前12位', '数量', '调查点类型', '所在乡镇街道', '村居名称', '调查员姓名', '调查员电话', '城乡属性']
            df = pd.DataFrame(result, columns=columns)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"调查点村名单_{timestamp}.xlsx"

            upload_dir = os.path.abspath(app_config['UPLOAD_FOLDER'])
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir, exist_ok=True)
            file_path = os.path.join(upload_dir, filename)

            excel_ops._save_df_to_excel(df, file_path, '调查点村名单')
            logger.info(f"调查点村名单导出成功: {file_path}")

            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        except Exception as e:
            logger.error(f"导出调查点村名单失败: {str(e)}")
            return jsonify({'success': False, 'message': f'导出失败: {str(e)}'}), 500
    return _export_village_list()


@data_import_bp.route('/import_village_list', methods=['POST'])
def import_village_list():
    """导入调查点村名单Excel文件（UPSERT by 户代码前12位）"""
    @handle_errors
    def _import_village_list():
        logger.info("开始导入调查点村名单")
        if 'file' not in request.files:
            return "未选择文件", 400
        file = request.files['file']
        df, error, file_path = _process_uploaded_file(
            file, "导入调查点村名单", {'xlsx', 'xls'}, _read_village_list_excel
        )
        if error:
            return error[0], error[1]
        try:
            if df.empty:
                return "Excel文件中没有有效数据", 400

            total_rows = len(df)
            new_count = 0
            updated_count = 0
            error_count = 0
            error_details = []

            with db.pool.get_cursor() as cursor:
                for idx, row in df.iterrows():
                    try:
                        户代码前12位 = str(row['户代码前12位']).strip()
                        所在乡镇街道 = str(row['所在乡镇街道']).strip() if pd.notna(row['所在乡镇街道']) else ''
                        村居名称 = str(row['村居名称']).strip() if pd.notna(row['村居名称']) else ''
                        调查点类型 = str(row['调查点类型']).strip() if pd.notna(row['调查点类型']) else ''
                        调查员姓名 = str(row['调查员姓名']).strip() if pd.notna(row['调查员姓名']) else ''
                        调查员电话 = str(row['调查员电话']).strip() if pd.notna(row['调查员电话']) else ''
                        城乡属性 = str(row['城乡属性']).strip() if pd.notna(row['城乡属性']) else ''
                        数量 = row['数量'] if pd.notna(row['数量']) else None

                        if not 户代码前12位:
                            error_count += 1
                            error_details.append(f"第{idx+2}行: 户代码前12位为空")
                            continue
                        if not 所在乡镇街道 or not 村居名称:
                            error_count += 1
                            error_details.append(f"第{idx+2}行: 所在乡镇街道或村居名称为空")
                            continue

                        # 是否存在
                        check_sql = "SELECT COUNT(*) FROM 调查点村名单 WHERE 户代码前12位 = ?"
                        cursor.execute(check_sql, (户代码前12位,))
                        exists = cursor.fetchone()[0] > 0

                        if exists:
                            update_sql = """
                            UPDATE 调查点村名单
                            SET 数量 = ?, 调查点类型 = ?, 所在乡镇街道 = ?, 村居名称 = ?, 调查员姓名 = ?, 调查员电话 = ?, 城乡属性 = ?
                            WHERE 户代码前12位 = ?
                            """
                            cursor.execute(update_sql, (数量, 调查点类型, 所在乡镇街道, 村居名称, 调查员姓名, 调查员电话, 城乡属性, 户代码前12位))
                            updated_count += 1
                        else:
                            insert_sql = """
                            INSERT INTO 调查点村名单 (户代码前12位, 数量, 调查点类型, 所在乡镇街道, 村居名称, 调查员姓名, 调查员电话, 城乡属性)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """
                            cursor.execute(insert_sql, (户代码前12位, 数量, 调查点类型, 所在乡镇街道, 村居名称, 调查员姓名, 调查员电话, 城乡属性))
                            new_count += 1
                    except Exception as e:
                        error_count += 1
                        error_details.append(f"第{idx+2}行处理失败: {str(e)}")
                        logger.warning(f"处理第{idx+2}行数据失败: {str(e)}")
                        continue

            summary_message = "调查点村名单导入完成！\n"
            summary_message += f"• 总处理记录数：{total_rows} 条\n"
            summary_message += f"• 新增记录：{new_count} 条\n"
            summary_message += f"• 更新记录：{updated_count} 条\n"
            if error_count > 0:
                summary_message += f"• 错误记录：{error_count} 条\n"
                if error_details:
                    summary_message += "• 错误详情：\n"
                    for d in error_details[:5]:
                        summary_message += f"  - {d}\n"
                    if len(error_details) > 5:
                        summary_message += f"  - 还有 {len(error_details) - 5} 个错误未显示\n"
            logger.info(f"调查点村名单导入完成 - 新增: {new_count}, 更新: {updated_count}, 错误: {error_count}")
            return summary_message
        except Exception as e:
            logger.error(f"导入调查点村名单过程中发生错误: {str(e)}")
            return f"导入过程中发生错误: {str(e)}", 500
        finally:
            _cleanup_file(file_path)
    return _import_village_list()
