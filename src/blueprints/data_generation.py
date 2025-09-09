"""
数据生成模块蓝图
包含生成电子台账和汇总表的功能
"""

from flask import Blueprint, request, send_file, current_app, make_response, Response, jsonify
import logging
import os
import pandas as pd
import zipfile
import tempfile
import shutil
import json
import time
import threading
from queue import Queue
# from src.error_handler import with_error_handling  # 已删除

# 创建蓝图
data_generation_bp = Blueprint('data_generation', __name__)
logger = logging.getLogger(__name__)

# 这些变量将在蓝图注册时从主应用传入
db = None
data_processor = None

# 全局进度存储（用于电子台账生成进度反馈）
progress_storage = {}
progress_lock = threading.Lock()

def update_progress(task_id, current_town, total_towns, current_index, status='processing'):
    """更新任务进度"""
    with progress_lock:
        progress_storage[task_id] = {
            'current_town': current_town,
            'total_towns': total_towns,
            'current_index': current_index,
            'status': status,
            'timestamp': time.time()
        }

def get_progress(task_id):
    """获取任务进度"""
    with progress_lock:
        return progress_storage.get(task_id, None)

def clear_progress(task_id):
    """清除任务进度"""
    with progress_lock:
        if task_id in progress_storage:
            del progress_storage[task_id]

@data_generation_bp.route('/ledger_progress/<task_id>', methods=['GET'])
def get_ledger_progress(task_id):
    """获取电子台账生成进度"""
    progress = get_progress(task_id)
    if progress:
        return jsonify({
            'success': True,
            'progress': progress
        })
    else:
        return jsonify({
            'success': False,
            'message': '未找到任务进度'
        }), 404
excel_ops = None
handle_errors = None

def init_blueprint(database, processor, excel_operations, error_handler):
    """初始化蓝图依赖"""
    global db, data_processor, excel_ops, handle_errors
    db = database
    data_processor = processor
    excel_ops = excel_operations
    handle_errors = error_handler



@data_generation_bp.route('/generate_electronic_ledger', methods=['POST'])
def generate_electronic_ledger():
    """生成电子台账 - 重新设计版本"""
    @handle_errors
    def _generate_electronic_ledger():
        # 支持JSON和表单数据
        if request.is_json:
            data = request.get_json()
            year = data.get('year')
            month = data.get('month')
            town = data.get('town')
            village = data.get('village')
            task_id = data.get('task_id')  # 添加任务ID支持
        else:
            year = request.form.get('year')
            month = request.form.get('month')
            town = request.form.get('town')
            village = request.form.get('village')  # 添加村庄参数支持
            task_id = request.form.get('task_id')  # 添加任务ID支持

        if not all([year, month, town]):
            logger.warning("生成电子台账时缺少必要参数")
            return "缺少必要参数：年度、月份和乡镇", 400

        # 检查是否选择了"全部乡镇"
        if town == "全部乡镇":
            logger.info(f"开始批量生成电子台账 - 年度: {year}, 月份: {month}, 全部乡镇")
            return _generate_all_towns_ledger(year, month, village, task_id)

        logger.info(f"开始生成电子台账 - 年度: {year}, 月份: {month}, 乡镇: {town}, 村庄: {village or '全部'}")

        # 如果有任务ID，更新进度
        if task_id:
            update_progress(task_id, town, 1, 1, 'processing')

        # 使用事务处理
        try:
            # 记录开始时间
            import time
            start_time = time.time()

            # 更新类型信息（SQLite 兼容写法：使用相关子查询代替 UPDATE ... FROM JOIN）
            with db.pool.get_cursor() as cursor:
                cursor.execute('''
UPDATE 调查点台账合并
SET type = (
    SELECT 收支类别 FROM 调查品种编码 c
    WHERE c.帐目编码 = 调查点台账合并.code
)
WHERE EXISTS (
    SELECT 1 FROM 调查品种编码 c
    WHERE c.帐目编码 = 调查点台账合并.code
)''')

            # 使用新的电子台账生成器
            from src.electronic_ledger_generator import ElectronicLedgerGenerator
            from src.electronic_ledger_excel import ElectronicLedgerExcel

            logger.info("开始查询数据库...")
            query_start = time.time()

            # 创建生成器实例
            generator = ElectronicLedgerGenerator(db)
            summary_df, detail_df, consumption_df = generator.generate(year, month, town, village)

            query_time = time.time() - query_start
            logger.info(f"数据库查询完成，耗时: {query_time:.2f}秒")

            # 保存Excel文件
            logger.info("开始生成Excel文件...")
            excel_start = time.time()

            # 创建Excel生成器实例
            excel_generator = ElectronicLedgerExcel()
            file_path = excel_generator.save_electronic_ledger(summary_df, detail_df, consumption_df, town, month, year)

            excel_time = time.time() - excel_start
            logger.info(f"Excel文件生成完成，耗时: {excel_time:.2f}秒")

            # 构建下载文件名：YYYY-MM_乡镇名称_村庄名称_电子台帐.xlsx
            town_village_name = f"{town}_{village}" if village else town
            download_filename = f"{year}-{month.zfill(2)}_{town_village_name}_电子台帐.xlsx"

            # 事务会在上下文管理器退出时自动提交
            logger.info(f"电子台账生成成功 - 文件路径: {file_path}, 下载文件名: {download_filename}")

            # 验证文件是否存在
            if not os.path.exists(file_path):
                logger.error(f"生成的文件不存在: {file_path}")
                return "文件生成失败，请重试", 500

            # 验证文件大小
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"生成的文件为空: {file_path}")
                return "生成的文件为空，请检查数据", 500

            logger.info(f"文件验证通过 - 大小: {file_size} 字节")

            # 记录总耗时
            total_time = time.time() - start_time
            logger.info(f"电子台账生成总耗时: {total_time:.2f}秒")

            # 如果有任务ID，更新完成状态
            if task_id:
                update_progress(task_id, town, 1, 1, 'completed')

            # 设置响应头以确保正确的文件名和编码
            import urllib.parse

            # 对文件名进行URL编码以处理中文字符
            encoded_filename = urllib.parse.quote(download_filename.encode('utf-8'))

            # 创建ASCII安全的文件名用于fallback
            ascii_filename = f"{year}-{month.zfill(2)}_ledger.xlsx"

            response = send_file(
                file_path,
                as_attachment=True,
                download_name=download_filename,  # 使用download_name参数
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

            # 设置兼容的Content-Disposition头，避免中文字符编码问题
            # 使用RFC 5987标准格式，只在filename*中使用UTF-8编码
            response.headers['Content-Disposition'] = f'attachment; filename="{ascii_filename}"; filename*=UTF-8\'\'{encoded_filename}'
            response.headers['Cache-Control'] = 'no-cache'
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

            return response
        except Exception as e:
            logger.error(f"生成电子台账失败: {str(e)}")
            # 注意：连接池会自动处理事务回滚
            raise
    
    def _generate_all_towns_ledger(year, month, village, task_id=None):
        """批量生成所有乡镇的电子台账"""
        from src.query_service import QueryService

        try:
            # 获取数据库实例和查询服务
            # 使用全局变量 db（在蓝图初始化时设置）
            if not db:
                logger.error("数据库连接未初始化")
                return "数据库连接失败", 500

            query_service = QueryService(db)

            # 查询在指定年月有记录的所有乡镇
            logger.info(f"查询 {year}-{month} 有记录的乡镇...")
            towns_with_data = query_service.get_towns_with_data(year, month)

            if not towns_with_data:
                logger.warning(f"在 {year}-{month} 未找到任何有记录的乡镇")
                return f"在 {year}年{month}月未找到任何有记录的乡镇", 404

            logger.info(f"找到 {len(towns_with_data)} 个有记录的乡镇: {', '.join(towns_with_data)}")

            # 创建临时目录存放所有文件
            temp_dir = tempfile.mkdtemp()
            generated_files = []

            try:
                # 为每个乡镇生成电子台账
                from src.electronic_ledger_generator import ElectronicLedgerGenerator
                from src.electronic_ledger_excel import ElectronicLedgerExcel

                generator = ElectronicLedgerGenerator(db)
                excel_generator = ElectronicLedgerExcel()

                total_towns = len(towns_with_data)
                for index, town in enumerate(towns_with_data, 1):
                    try:
                        logger.info(f"正在生成 {town} 的电子台账... ({index}/{total_towns})")

                        # 如果有任务ID，更新进度
                        if task_id:
                            update_progress(task_id, town, total_towns, index, 'processing')

                        # 生成数据
                        summary_df, detail_df, consumption_df = generator.generate(year, month, town, village)

                        # 生成Excel文件到临时目录
                        file_path = excel_generator.save_electronic_ledger_to_dir(
                            summary_df, detail_df, consumption_df, town, month, year, temp_dir
                        )
                        generated_files.append(file_path)

                        logger.info(f"{town} 电子台账生成完成 ({index}/{total_towns})")

                    except Exception as e:
                        logger.error(f"生成 {town} 电子台账失败: {str(e)}")
                        continue

                if not generated_files:
                    return "所有乡镇的电子台账生成都失败了", 500

                # 创建ZIP文件
                zip_filename = f"{year}-{month.zfill(2)}_全部乡镇_电子台账.zip"
                zip_path = os.path.join(temp_dir, zip_filename)

                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in generated_files:
                        if os.path.exists(file_path):
                            # 使用文件名作为ZIP内的路径
                            arcname = os.path.basename(file_path)
                            zipf.write(file_path, arcname)

                logger.info(f"ZIP文件创建完成: {zip_path}, 包含 {len(generated_files)} 个文件")

                # 如果有任务ID，更新完成状态
                if task_id:
                    update_progress(task_id, "全部乡镇", total_towns, total_towns, 'completed')

                # 读取ZIP文件内容
                with open(zip_path, 'rb') as f:
                    zip_data = f.read()

                # 创建响应
                response = make_response(zip_data)
                response.headers['Content-Type'] = 'application/zip'

                # 使用RFC 5987格式支持中文文件名
                import urllib.parse
                encoded_filename = urllib.parse.quote(zip_filename.encode('utf-8'))
                response.headers['Content-Disposition'] = f'attachment; filename*=UTF-8\'\'{encoded_filename}'
                response.headers['Cache-Control'] = 'no-cache'

                return response

            finally:
                # 清理临时目录
                try:
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    logger.warning(f"清理临时目录失败: {e}")

        except Exception as e:
            logger.error(f"批量生成电子台账失败: {str(e)}")
            return f"批量生成失败: {str(e)}", 500

    return _generate_electronic_ledger()

@data_generation_bp.route('/generate_summary_table', methods=['POST'])
def generate_summary_table():
    """生成汇总表"""
    @handle_errors
    def _generate_summary_table():
        # 支持JSON和表单数据
        if request.is_json:
            data = request.get_json()
            year = data.get('year')
            period = data.get('period')
            category = data.get('category')
            sample_point_type = data.get('sample_point_type')
        else:
            year = request.form.get('year')
            period = request.form.get('period')
            category = request.form.get('category')
            sample_point_type = request.form.get('sample_point_type')

        if not all([year, period, category, sample_point_type]):
            logger.warning("生成汇总表时缺少必要参数")
            return "缺少必要参数：年份、期别、类别和样本点类型", 400

        logger.info(f"开始生成汇总表 - 年份: {year}, 期别: {period}, 类别: {category}, 样本点类型: {sample_point_type}")

        try:
            # 1. 根据输入参数构建SQL查询
            start_year = str(int(year) - 1)
            end_year = year

            period_to_month_map = {
                '一季度': '02',
                '上半年': '05',
                '三季度': '08',
                '全年度': '11'
            }
            end_month = period_to_month_map.get(period)
            if not end_month:
                raise ValueError(f"未知的期别: {period}")

            if category == '全部':
                category_filter = "'1', '2', '3'"
            elif category == '农村点':
                category_filter = "'1'"
            elif category == '城镇点':
                category_filter = "'2', '3'"
            else:
                raise ValueError(f"未知的类别: {category}")

            # 处理样本点类型筛选
            sample_point_filter = ""
            if sample_point_type == '国家点':
                sample_point_filter = "AND t1.id BETWEEN 1000000000 AND 1999999999"
            
            sql_query = f"""
                -- ================================================
                -- 使用 CTE (公用表表达式) 优化多步操作
                -- ================================================

                -- CTE 1: 汇总收支明细 (替代 '上半年结构' 表)
                -- 这一步将台账数据按户、按编码、按类型进行聚合
                WITH IncomeExpenseDetails AS (
                    SELECT
                        t1.hudm,
                        调查点户名单.户主姓名,
                        t1.code,
                        调查品种编码.帐目指标名称,
                        t1.type AS 收支类别,
                        COUNT(t1.code) AS 记账笔数,
                        SUM(t1.money) AS 总金额,
                        调查点户名单.人数
                    FROM 调查点台账合并 AS t1
                    INNER JOIN 调查品种编码 ON 调查品种编码.帐目编码 = t1.code
                    INNER JOIN 调查点户名单 ON t1.hudm = 调查点户名单.户代码
                    WHERE
                        ((t1.year = '{start_year}' AND t1.month = '12')
                        OR (t1.year = '{end_year}' AND t1.month <= '{end_month}'))
                        {sample_point_filter}
                    GROUP BY
                        t1.hudm,
                        t1.code,
                        t1.type,
                        调查点户名单.户主姓名,
                        调查品种编码.帐目指标名称,
                        调查点户名单.人数
                ),

                -- CTE 2: 汇总各项收支指标 (替代 '汇总表', '汇总过渡表', 'zpf_ls2' 表)
                -- 这一步将第一步的结果按户进行汇总，并计算各项指标
                AggregatedHouseholdData AS (
                    SELECT
                        hudm,
                        户主姓名,
                        人数,
                        COUNT(code) AS 总结构数,
                        COUNT(CASE WHEN 收支类别 = 1 THEN code END) AS 收入结构,
                        COUNT(CASE WHEN 收支类别 = 2 THEN code END) AS 支出结构,
                        COUNT(CASE WHEN 收支类别 = 2 AND SUBSTR(code, 1, 2) <> '31' THEN code END) AS 非食品结构,
                        SUM(CASE WHEN 收支类别 = 1 AND SUBSTR(code, 1, 2) NOT IN ('25', '26', '42') THEN 总金额 ELSE 0 END) AS 纯收入,
                        SUM(CASE WHEN (SUBSTR(code, 1, 2) BETWEEN '31' AND '38' OR SUBSTR(code, 1, 2) IN ('41','42','43')) THEN 总金额 ELSE 0 END) AS 消费支出,
                        SUM(记账笔数) AS 总笔数,
                        SUM(CASE WHEN 收支类别 = 2 AND SUBSTR(code, 1, 2) = '31' THEN 总金额 ELSE 0 END) AS 食品支出,
                        SUM(CASE WHEN SUBSTR(code, 1, 2) IN ('21', '22', '23', '24') THEN 总金额 ELSE 0 END) AS 总收入,
                        SUM(CASE WHEN SUBSTR(code, 1, 2) = '36' THEN 总金额 ELSE 0 END) AS 娱乐支出,
                        SUM(CASE WHEN SUBSTR(code, 1, 2) = '21' THEN 总金额 ELSE 0 END) AS 工资性收入,
                        SUM(
                            CASE WHEN SUBSTR(code, 1, 2) = '22' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 2) = '51' THEN 总金额 ELSE 0 END
                            + CASE WHEN SUBSTR(code, 1, 2) = '12' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 2) = '13' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 2) = '14' THEN 总金额 ELSE 0 END
                        ) AS 经营收入,
                        SUM(
                            CASE WHEN SUBSTR(code, 1, 2) = '23' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 2) = '52' THEN 总金额 ELSE 0 END
                        ) AS 财产性收入,
                        SUM(
                            CASE WHEN SUBSTR(code, 1, 2) = '24' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 3) = '531' THEN 总金额 ELSE 0 END
                            - CASE WHEN SUBSTR(code, 1, 3) = '534' THEN 总金额 ELSE 0 END
                        ) AS 转移性收入,
                        SUM(
                            CASE WHEN SUBSTR(code, 1, 2) = '51' THEN 总金额 ELSE 0 END
                            + CASE WHEN SUBSTR(code, 1, 2) = '13' THEN 总金额 ELSE 0 END
                            + CASE WHEN SUBSTR(code, 1, 2) = '14' THEN 总金额 ELSE 0 END
                        ) AS 经营成本,
                        SUM(CASE WHEN SUBSTR(code, 1, 2) IN ('21', '22', '23', '24','12') THEN 总金额 ELSE 0 END) -
                            SUM(CASE WHEN SUBSTR(code, 1, 2) IN ('51', '52', '53','13','14') THEN 总金额 ELSE 0 END) AS 可支配收入
                    FROM IncomeExpenseDetails
                    GROUP BY hudm, 户主姓名, 人数
                ),

                -- CTE 3: 关联村信息 (替代 'zpf_ls4' 表)
                -- 这一步将汇总后的户数据与村信息关联
                FinalHouseholdData AS (
                    SELECT
                        a.*,
                        b.户代码前12位,
                        b.所在乡镇街道,
                        b.村居名称,
                        b.城乡属性,
                        b.调查点类型
                    FROM AggregatedHouseholdData AS a
                    INNER JOIN 调查点村名单 AS b ON SUBSTR(a.hudm, 1, 12) = b.户代码前12位
                ),

                -- CTE 4: 主要聚合计算
                MainAggregation AS (
                    SELECT
                        所在乡镇街道,
                        COUNT(DISTINCT hudm) AS 户数,
                        SUM(人数) AS 人数,
                        SUM(总结构数) AS 总结构数,
                        SUM(收入结构) AS 收入结构,
                        SUM(支出结构) AS 支出结构,
                        SUM(非食品结构) AS 非食品结构,
                        SUM(纯收入) AS 纯收入,
                        SUM(消费支出) AS 消费支出,
                        SUM(总笔数) AS 总笔数,
                        SUM(食品支出) AS 食品支出,
                        SUM(总收入) AS 总收入,
                        SUM(娱乐支出) AS 娱乐支出,
                        SUM(工资性收入) AS 工资性收入,
                        SUM(经营收入) AS 经营收入,
                        SUM(财产性收入) AS 财产性收入,
                        SUM(转移性收入) AS 转移性收入,
                        SUM(经营成本) AS 经营成本,
                        SUM(可支配收入) AS 可支配收入_总和
                    FROM FinalHouseholdData
                    WHERE
                        城乡属性 IN ({category_filter})
                    GROUP BY 所在乡镇街道
                ),

                -- CTE 5: 计算人均可支配收入中位数 (已重构)
                PerCapitaIncome AS (
                    SELECT
                        所在乡镇街道,
                        可支配收入 / CAST(人数 AS FLOAT) AS PerCapitaDisposableIncome
                    FROM
                        FinalHouseholdData
                    WHERE
                        人数 > 0 AND 城乡属性 IN ({category_filter})
                ),
                RankedIncome AS (
                    SELECT
                        所在乡镇街道,
                        PerCapitaDisposableIncome,
                        ROW_NUMBER() OVER (PARTITION BY 所在乡镇街道 ORDER BY PerCapitaDisposableIncome) AS RowAsc,
                        ROW_NUMBER() OVER (PARTITION BY 所在乡镇街道 ORDER BY PerCapitaDisposableIncome DESC) AS RowDesc
                    FROM
                        PerCapitaIncome
                ),
                MedianCalculation AS (
                    SELECT
                        所在乡镇街道,
                        AVG(CAST(PerCapitaDisposableIncome AS FLOAT)) AS 人均可支配收入中位数
                    FROM
                        RankedIncome
                    WHERE
                        RowAsc IN (RowDesc, RowDesc - 1, RowDesc + 1)
                    GROUP BY
                        所在乡镇街道
                )

                -- 最终查询: 合并主要聚合结果和中位数结果
                SELECT
                    m.所在乡镇街道,
                    m.户数,
                    m.人数,
                    m.总结构数,
                    m.收入结构,
                    m.支出结构,
                    m.非食品结构,
                    m.纯收入,
                    m.消费支出,
                    m.总笔数,
                    m.食品支出,
                    m.总收入,
                    m.娱乐支出,
                    m.工资性收入,
                    m.经营收入,
                    m.财产性收入,
                    m.转移性收入,
                    m.经营成本,
                    med.人均可支配收入中位数,
                    m.可支配收入_总和 / NULLIF(m.人数, 0) AS 人均可支配收入,
                    m.消费支出 / NULLIF(m.人数, 0) AS 人均消费支出,
                    m.消费支出 * 100 / NULLIF(m.可支配收入_总和, 0) AS 收支比,
                    m.食品支出 * 100 / NULLIF(m.消费支出, 0) AS 恩格尔系数,
                    m.娱乐支出 * 100 / NULLIF(m.消费支出, 0) AS 教育娱乐比,
                    m.总笔数 / NULLIF(m.人数, 0) AS 人均笔数
                FROM MainAggregation m
                LEFT JOIN MedianCalculation med ON m.所在乡镇街道 = med.所在乡镇街道
                ORDER BY m.所在乡镇街道;
            """

            # 2. 执行查询
            logger.info("开始执行汇总查询...")
            # 一次性获取数据和列信息，避免重复查询
            with db.pool.get_cursor() as cursor:
                cursor.execute(sql_query)
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
            df = pd.DataFrame.from_records(rows, columns=columns)
            logger.info("汇总查询执行完毕。")

            # 2.5 根据需求调整数值格式：
            # - 收支比、恩格尔系数、教育娱乐比、人均笔数 保留2位小数
            # - 其他汇总数值全部取整
            keep_two_decimals = {'收支比', '恩格尔系数', '教育娱乐比', '人均笔数'}
            for col in df.columns:
                if col in keep_two_decimals:
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
                elif col != '所在乡镇街道':
                    # 其他非文本列全部取整（四舍五入后转int）
                    df[col] = pd.to_numeric(df[col], errors='coerce').round(0).astype('Int64')

            # 3. 保存到Excel
            file_path = excel_ops.save_summary_table(df, year, period, category)
            logger.info(f"汇总表生成成功 - 文件路径: {file_path}")

            # 4. 发送文件
            download_filename = os.path.basename(file_path)
            return send_file(file_path, as_attachment=True, download_name=download_filename)

        except Exception as e:
            logger.error(f"生成汇总表失败: {str(e)}")
            # 注意：连接池会自动处理事务回滚
            raise

    return _generate_summary_table()
