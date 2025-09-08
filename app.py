from flask import Flask, render_template, jsonify
from src.database import Database
from src.data_processing import DataProcessor
from src.excel_operations import ExcelOperations
from src.utils import handle_errors, allowed_file, validate_file_size, MAX_FILE_SIZE
from src.blueprints.data_generation import data_generation_bp
from src.blueprints.data_import import data_import_bp


from src.blueprints.statistics import statistics_bp
from src.blueprints.household_analysis import household_analysis_bp
import os
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='src/templates')

# 初始化数据库和处理器
db = None
data_processor = None
excel_ops = None


try:
    # 添加启动延迟，等待依赖服务准备就绪
    import time
    logger.info("等待依赖服务启动...")
    time.sleep(5)

    db = Database()
    data_processor = DataProcessor(db)
    excel_ops = ExcelOperations()



    logger.info("应用程序初始化成功")
except Exception as e:
    logger.error(f"数据库连接失败，应用程序将以受限模式启动: {str(e)}")
    # 在受限模式下，仍然初始化Excel操作
    try:
        excel_ops = ExcelOperations()
        logger.info("Excel操作模块初始化成功")
    except Exception as excel_error:
        logger.error(f"Excel操作模块初始化失败: {excel_error}")

    logger.warning("应用程序正在受限模式下运行，某些功能可能不可用")

# 初始化蓝图依赖
from src.blueprints.data_generation import init_blueprint as init_data_generation
from src.blueprints.data_import import init_blueprint as init_data_import


from src.blueprints.statistics import init_blueprint as init_statistics
from src.blueprints.household_analysis import init_blueprint as init_household_analysis

init_data_generation(db, data_processor, excel_ops, handle_errors)
init_data_import(db, excel_ops, handle_errors, allowed_file, validate_file_size, app.config)


init_statistics(db, handle_errors)
init_household_analysis(db, handle_errors)

# 注册蓝图
app.register_blueprint(data_generation_bp)
app.register_blueprint(data_import_bp)


app.register_blueprint(statistics_bp, url_prefix='/')
app.register_blueprint(household_analysis_bp, url_prefix='/')

@app.route('/')
@handle_errors
def index():
    logger.info("访问主页")
    return render_template('index.html')


@app.route('/health')
def health_check():
    """健康检查端点"""
    return jsonify({'status': 'healthy', 'message': '服务正常运行'})

@app.route('/api/system/status')
@handle_errors
def system_status():
    """获取系统状态信息"""
    database_connected = False
    if db is not None:
        try:
            # 检查数据库连接
            result = db.execute_query_safe("SELECT 1")
            database_connected = True if result else False
        except Exception as e:
            logger.warning(f"数据库连接检查失败: {str(e)}")
            database_connected = False

    status = {
        'database_connected': database_connected,
        'timestamp': datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')
    }

    return jsonify(status)

if os.environ.get('FLASK_ENV') != 'production':
    @app.route('/debug')
    def debug_page():
        """调试页面 - 检查JavaScript控制台输出"""
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>系统调试页面</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .info { background: #e7f3ff; padding: 15px; border-radius: 5px; margin: 10px 0; }
                .success { background: #d4edda; padding: 15px; border-radius: 5px; margin: 10px 0; }
                .error { background: #f8d7da; padding: 15px; border-radius: 5px; margin: 10px 0; }
            </style>
        </head>
        <body>
            <h1>🔍 系统调试页面</h1>

            <div class="info">
                <h3>📋 调试说明</h3>
                <p>1. 打开浏览器开发者工具（F12）</p>
                <p>2. 切换到 Console（控制台）选项卡</p>
                <p>3. 刷新此页面，查看控制台输出</p>
                <p>4. 检查是否有JavaScript错误或警告</p>
            </div>

            <div id="status">正在检查系统状态...</div>

            <script>
                console.log('=== 系统调试开始 ===');
                const now = new Date();
                const timeStr = now.getFullYear() + '年' +
                               String(now.getMonth() + 1).padStart(2, '0') + '月' +
                               String(now.getDate()).padStart(2, '0') + '日 ' +
                               String(now.getHours()).padStart(2, '0') + ':' +
                               String(now.getMinutes()).padStart(2, '0') + ':' +
                               String(now.getSeconds()).padStart(2, '0');
                console.log('当前时间:', timeStr);
                console.log('用户代理:', navigator.userAgent);

                // 测试基础功能
                try {
                    console.log('✅ JavaScript基础功能正常');

                    // 测试API
                    fetch('/api/system/status')
                        .then(response => {
                            console.log('API响应状态:', response.status);
                            return response.json();
                        })
                        .then(data => {
                            console.log('✅ 系统状态API正常:', data);
                            document.getElementById('status').innerHTML =
                                '<div class="success">✅ 系统状态正常，数据库连接: ' +
                                (data.database_connected ? '已连接' : '未连接') + '</div>';

                            // 测试统计API
                            return fetch('/api/direct_coding/statistics');
                        })
                        .then(response => response.json())
                        .then(data => {
                            console.log('✅ 统计API正常:', data);
                            document.getElementById('status').innerHTML +=
                                '<div class="success">✅ 统计API正常，总映射数: ' +
                                data.data.total_mappings + '</div>';
                        })
                        .catch(error => {
                            console.error('❌ API测试失败:', error);
                            document.getElementById('status').innerHTML =
                                '<div class="error">❌ API测试失败: ' + error.message + '</div>';
                        });

                } catch (error) {
                    console.error('❌ JavaScript基础功能异常:', error);
                    document.getElementById('status').innerHTML =
                        '<div class="error">❌ JavaScript基础功能异常: ' + error.message + '</div>';
                }

                console.log('=== 系统调试完成 ===');
            </script>
        </body>
        </html>
        '''

if __name__ == '__main__':
    try:
        # 配置上传文件夹
        app.config['UPLOAD_FOLDER'] = 'uploads'
        app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
            logger.info(f"创建上传文件夹: {app.config['UPLOAD_FOLDER']}")

        logger.info("应用程序启动")
        # 从环境变量获取端口，默认为5000
        port = int(os.environ.get('FLASK_RUN_PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    except Exception as e:
        logger.error(f"应用程序启动失败: {str(e)}")
        raise
