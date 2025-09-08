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
2) 配置文件（本地开发常用）：config/mssql.json、config/wbfwq.json

出于安全考虑，仓库仅提供示例文件：
- config/mssql.json.example
- config/wbfwq.json.example

请根据示例创建实际配置文件：

```bash
cp config/mssql.json.example config/mssql.json
cp config/wbfwq.json.example config/wbfwq.json
# 然后编辑上述 .json 填入真实连接信息
```

## 运行应用

```bash
# 可选：复制环境变量示例
cp .env.example .env

# 启动（默认端口 5000，可通过 FLASK_RUN_PORT 指定）
python app.py
```

启动后访问：
- http://localhost:5000/         首页
- http://localhost:5000/health   健康检查

提示：若未正确配置数据库，应用会以“受限模式”启动（部分业务接口不可用，但页面与健康检查可用）。

## 常见问题
- ODBC 驱动安装：请确保系统已正确安装并能通过 `pyodbc` 连接 SQL Server。
- Excel 导出路径：系统在项目根目录下的 `uploads/` 中生成导出文件。

## 变更概述（本次清理）
- 移除 Docker 相关（Dockerfile、docker-compose*、.dockerignore、nginx 配置等）
- 移除部署与构建脚本（scripts/、deploy_docker.sh 等）
- 移除调试与临时测试文件（debug_*、test_*、若干报告文档等）
- 删除包含敏感信息的本地配置（config/*.json），保留示例文件供本地创建
- 精简 requirements.txt（移除未使用的 aiohttp、asyncio、requests、python-docx、fonttools）

## 许可证
参见 LICENSE。
