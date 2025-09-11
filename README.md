# 项目说明（本地运行版）

本项目为基于 Flask 的住户台账处理与电子台账生成系统。当前仓库已进行系统性清理，移除了 Docker/部署相关文件与临时调试脚本，改为推荐本地直接运行。

## 目录结构（清理后）
- app.py: 应用入口
- src/: 核心业务逻辑与蓝图
- static/: 前端静态资源
- config/: 数据库配置示例（请按说明创建实际配置）
- uploads/: 运行时导出文件目录（已在 .gitignore 中忽略）
- requirements.txt: 依赖列表
- LICENSE, README.md

## 环境要求
- Python 3.10+
- 已安装 SQL Server ODBC Driver（用于 pyodbc）

## 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -U pip
pip install -r requirements.txt
```

## 数据库配置
应用启动时会尝试按以下优先级加载数据库配置：
1) 环境变量（推荐用于容器/CI 环境）
   - DATABASE_HOST, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
   - 可选：DB_DRIVER（默认 ODBC Driver 18 for SQL Server）、DB_ENCRYPT、DB_TRUST_CERT
2) 配置文件（本地开发常用）：config/mssql.json

出于安全考虑，仓库仅提供示例文件：
- config/mssql.json.example

请根据示例创建实际配置文件：

```bash
cp config/mssql.json.example config/mssql.json
# 然后编辑 mssql.json 填入真实连接信息
```

## 运行应用

```bash
# 可选：复制环境变量示例
cp .env.example .env

# 启动（默认端口 8888，可通过 FLASK_RUN_PORT 指定）
python app.py
```

启动后访问：
- http://localhost:8888/         首页
- http://localhost:8888/health   健康检查

提示：若未正确配置数据库，应用会以“受限模式”启动（部分业务接口不可用，但页面与健康检查可用）。

## 常见问题
- ODBC 驱动安装：请确保系统已正确安装并能通过 `pyodbc` 连接 SQL Server。
- Excel 导出路径：系统在项目根目录下的 `uploads/` 中生成导出文件。

## 变更概述（本次清理）
- 移除 Docker 相关（Dockerfile、docker-compose*、.dockerignore、nginx 配置等）
- 移除部署与构建脚本（scripts/、deploy_docker.sh 等）
- 移除调试与临时测试文件（debug_*、test_*、若干报告文档等）
- 删除包含敏感信息的本地配置（config/mssql.json），保留示例文件供本地创建
- 移除外部数据库同步功能（删除 database_sync 蓝图及相关代码）
- 精简 requirements.txt（移除未使用的 aiohttp、asyncio、requests、python-docx、fonttools）

## 许可证
参见 LICENSE。


## 本次代码清理与优化（2025-09-11）
- 删除不再使用的测试/调试脚本与临时文件：
  - test_export_fix.py、debug_household_export.py、add_test_data.py
  - test_household_export_20250910_083643.xlsx（测试导出样例）
  - 旧备份与占位文件：backups/database_backup_.db、database_backup_20250909_032816.db
  - 历史日志：migration.log、migration_failed.log、app_stdout.log
- 日志处理：保留 app.log 文件句柄，已将其内容清空（截断）。
- 保留与保护：
  - 未清理 uploads/ 目录中的业务数据与备份（含 database_backup_*.db），以保障数据可追溯。
  - 未改动 database.db 及其 WAL/SHM 文件，保障运行时数据库完整性。
- 验证：
  - 应用已保持运行，/health 健康检查通过。
  - 核心功能（数据导入、统计分析、户名单管理等）不受影响。

说明：后续如需进一步进行“未使用导入语句/依赖”的自动化清理，建议在允许的前提下引入静态检查工具（如 ruff/flake8）并在 CI 中执行，以更全面地移除冗余导入与死代码。当前变更优先进行安全、无副作用的外部文件与脚本清理。
