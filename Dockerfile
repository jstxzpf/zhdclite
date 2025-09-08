# ================================
# 阶段1: 基础镜像构建 (base-image)
# 包含系统依赖和Python依赖包
# ================================

# 定义构建参数，支持DaoCloud镜像加速（前缀替换方式）
ARG REGISTRY_PREFIX="docker.m.daocloud.io"
ARG PYTHON_VERSION="3.9-slim"

# 使用DaoCloud镜像加速（前缀替换方式）
# 例: docker.m.daocloud.io/python:3.9-slim
FROM ${REGISTRY_PREFIX}/python:${PYTHON_VERSION} AS base-image

# 设置构建时环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# 设置镜像标签信息
LABEL maintainer="HOUSEHOLD_DATA_SYSTEM_FLASK" \
      version="1.0" \
      description="Base image with system and Python dependencies"

# 配置阿里云apt源并安装系统依赖（合并为单个RUN指令以减少镜像层数）
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y \
        curl \
        gnupg \
        unixodbc \
        unixodbc-dev \
        freetds-dev \
        freetds-bin \
        tdsodbc \
        fonts-wqy-zenhei \
        fonts-wqy-microhei \
        fontconfig \
        ca-certificates \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && fc-cache -fv \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 配置pip使用阿里云源并升级pip
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    pip config set global.trusted-host mirrors.aliyun.com && \
    pip install --upgrade pip

# 设置工作目录
WORKDIR /app

# 复制requirements.txt并安装Python依赖
COPY requirements.txt .

# 使用阿里云源安装依赖，使用缓存优化
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r requirements.txt

# 创建必要的目录结构
RUN mkdir -p /app/uploads /app/logs /app/config /app/src /app/static

# 创建非root用户，使用与主机相同的UID/GID (1000)
RUN groupadd -g 1000 appuser && useradd -u 1000 -g appuser appuser

# 设置基础权限
RUN chmod -R 755 /app \
    && chown -R appuser:appuser /app

# ================================
# 阶段2: 应用镜像构建 (app-image)
# 基于基础镜像，仅添加源代码
# ================================
FROM base-image AS app-image

# 设置运行时环境变量
ENV FLASK_APP=app.py \
    FLASK_ENV=production \
    FLASK_RUN_HOST=0.0.0.0 \
    FLASK_RUN_PORT=5000

# 设置镜像标签信息
LABEL maintainer="HOUSEHOLD_DATA_SYSTEM_FLASK" \
      version="1.0" \
      description="Application image with source code"

# 复制源代码文件（按重要性和变更频率排序）
COPY --chown=appuser:appuser app.py .
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser static/ ./static/
COPY --chown=appuser:appuser config/ ./config/

# 配置文件已通过COPY命令复制，无需额外处理

# 设置运行时权限
RUN chmod -R 755 /app \
    && chmod -R 777 /app/uploads \
    && chmod -R 777 /app/logs \
    && chmod +x /app/app.py

# 切换到非root用户
USER appuser

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/api/system/status || exit 1

# 暴露端口
EXPOSE 5000

# 运行应用
CMD ["python", "app.py"]

# ================================
# 阶段3: 测试阶段 (test-stage)
# 基于应用镜像，运行测试
# ================================
FROM app-image AS test-stage

# 复制测试代码
COPY --chown=appuser:appuser tests/ ./tests/

# 设置PYTHONPATH
ENV PYTHONPATH=/app

# 运行测试
CMD ["pytest"]
