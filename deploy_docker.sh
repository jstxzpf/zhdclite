#!/bin/bash

# ========================================
# 住户数据整理系统 Docker 部署脚本
# ========================================

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Docker是否安装
check_docker() {
    log_info "检查Docker环境..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    # 检查Docker Compose支持（新版本Docker内置compose命令）
    if ! docker compose version &> /dev/null; then
        log_error "Docker Compose不可用，请确保Docker版本支持compose命令"
        exit 1
    fi
    
    # 检查Docker服务是否运行
    if ! docker info &> /dev/null; then
        log_error "Docker服务未运行，请启动Docker服务"
        exit 1
    fi
    
    log_success "Docker环境检查通过"
}

# 检查必要文件
check_files() {
    log_info "检查必要文件..."
    
    required_files=(
        "Dockerfile"
        "docker-compose.yml"
        "requirements.txt"
        "app.py"
        ".env"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "缺少必要文件: $file"
            exit 1
        fi
    done
    
    log_success "必要文件检查通过"
}

# 创建必要目录
create_directories() {
    log_info "创建必要目录..."
    
    directories=(
        "uploads"
        "logs"
        "config"
        "sql"
    )
    
    for dir in "${directories[@]}"; do
        if [[ ! -d "$dir" ]]; then
            mkdir -p "$dir"
            log_info "创建目录: $dir"
        fi
    done
    
    # 设置目录权限
    chmod 755 uploads logs
    
    log_success "目录创建完成"
}

# 停止现有容器
stop_existing() {
    log_info "停止现有容器..."
    
    if docker compose ps | grep -q "household-data-system"; then
        docker compose down
        log_info "已停止现有容器"
    else
        log_info "没有运行中的容器"
    fi
}

# 构建镜像
build_image() {
    log_info "构建Docker镜像..."
    
    # 清理旧的镜像（可选）
    if [[ "$1" == "--clean" ]]; then
        log_info "清理旧镜像..."
        docker compose down --rmi all --volumes --remove-orphans 2>/dev/null || true
    fi

    # 构建镜像
    docker compose build --no-cache
    
    log_success "镜像构建完成"
}

# 启动服务
start_services() {
    log_info "启动服务..."
    
    # 启动服务
    docker compose up -d
    
    log_success "服务启动完成"
}

# 检查服务状态
check_services() {
    log_info "检查服务状态..."
    
    # 等待服务启动
    sleep 10
    
    # 检查容器状态
    if docker compose ps | grep -q "Up"; then
        log_success "容器运行正常"

        # 显示容器状态
        echo ""
        log_info "容器状态:"
        docker compose ps
        
        # 检查健康状态
        echo ""
        log_info "等待健康检查..."
        sleep 30
        
        # 尝试访问应用
        if curl -f http://localhost:5000/api/system/status &> /dev/null; then
            log_success "应用健康检查通过"
            echo ""
            log_success "🎉 部署成功！"
            echo ""
            echo "访问地址: http://localhost:5000"
            echo "系统状态: http://localhost:5000/api/system/status"
        else
            log_warning "应用可能还在启动中，请稍后检查"
            echo "可以使用以下命令查看日志:"
            echo "docker compose logs -f household-data-app"
        fi
    else
        log_error "容器启动失败"
        echo ""
        log_info "查看错误日志:"
        docker compose logs
        exit 1
    fi
}

# 显示使用帮助
show_help() {
    echo "住户数据整理系统 Docker 部署脚本"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  --clean     清理旧镜像和容器后重新构建"
    echo "  --rebuild   重新构建镜像"
    echo "  --restart   重启服务"
    echo "  --stop      停止服务"
    echo "  --logs      查看日志"
    echo "  --status    查看状态"
    echo "  --help      显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                # 标准部署"
    echo "  $0 --clean        # 清理后重新部署"
    echo "  $0 --restart      # 重启服务"
    echo "  $0 --logs         # 查看日志"
}

# 主函数
main() {
    echo "========================================"
    echo "住户数据整理系统 Docker 部署"
    echo "========================================"
    echo ""
    
    case "${1:-}" in
        --help)
            show_help
            exit 0
            ;;
        --stop)
            stop_existing
            log_success "服务已停止"
            exit 0
            ;;
        --logs)
            docker compose logs -f
            exit 0
            ;;
        --status)
            docker compose ps
            exit 0
            ;;
        --restart)
            log_info "重启服务..."
            docker compose restart
            check_services
            exit 0
            ;;
        --rebuild)
            check_docker
            check_files
            stop_existing
            build_image
            start_services
            check_services
            exit 0
            ;;
        --clean)
            check_docker
            check_files
            create_directories
            stop_existing
            build_image --clean
            start_services
            check_services
            exit 0
            ;;
        "")
            # 标准部署流程
            check_docker
            check_files
            create_directories
            stop_existing
            build_image
            start_services
            check_services
            ;;
        *)
            log_error "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
}

# 执行主函数
main "$@"
