#!/bin/bash

# ================================
# HOUSEHOLD_DATA_SYSTEM_FLASK 多阶段部署脚本
# 自动化构建和部署流程
# ================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"

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

# 显示帮助信息
show_help() {
    cat << EOF
HOUSEHOLD_DATA_SYSTEM_FLASK 多阶段部署脚本

用法: $0 [选项] [环境]

环境:
    dev                     部署开发环境
    prod                    部署生产环境

选项:
    -h, --help              显示此帮助信息
    -b, --build             强制重新构建镜像
    -p, --pull              从仓库拉取镜像（生产环境）
    -d, --down              停止并删除容器
    -l, --logs              显示容器日志
    -s, --status            显示容器状态

示例:
    $0 dev                  # 部署开发环境
    $0 prod                 # 部署生产环境
    $0 dev -b               # 重新构建并部署开发环境
    $0 prod -p              # 拉取镜像并部署生产环境
    $0 -d                   # 停止所有容器
    $0 -s                   # 显示容器状态

EOF
}

# 检查环境
check_environment() {
    log_info "检查部署环境..."
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装"
        exit 1
    fi
    
    # 检查项目根目录
    if [ ! -f "$PROJECT_ROOT/Dockerfile" ]; then
        log_error "未找到Dockerfile，请确保在项目根目录运行"
        exit 1
    fi
    
    # 检查环境变量文件
    if [ ! -f "$ENV_FILE" ]; then
        log_warning "未找到.env文件，将使用默认配置"
        log_info "建议复制.env.example为.env并配置相关参数"
    fi
    
    log_success "环境检查通过"
}

# 加载环境变量
load_env() {
    if [ -f "$ENV_FILE" ]; then
        log_info "加载环境变量: $ENV_FILE"
        # 使用更安全的方式加载环境变量，过滤注释和空行
        set -a
        while IFS= read -r line || [ -n "$line" ]; do
            # 跳过注释行和空行
            if [[ "$line" =~ ^[[:space:]]*# ]] || [[ -z "${line// }" ]]; then
                continue
            fi
            # 导出变量
            if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
                export "${BASH_REMATCH[1]}"="${BASH_REMATCH[2]}"
            fi
        done < "$ENV_FILE"
        set +a
    fi
}

# 构建镜像
build_images() {
    local force_build=$1
    
    log_info "检查是否需要构建镜像..."
    
    if [ "$force_build" = true ]; then
        log_info "强制重新构建镜像"
        "$SCRIPT_DIR/build_multistage.sh" -f
    else
        log_info "智能检查并构建镜像"
        "$SCRIPT_DIR/build_multistage.sh" -c
    fi
}

# 拉取镜像
pull_images() {
    log_info "从镜像仓库拉取镜像..."
    
    if [ -z "$REGISTRY_URL" ]; then
        log_error "未设置REGISTRY_URL，无法拉取镜像"
        exit 1
    fi
    
    local base_image="${REGISTRY_URL}/${BASE_IMAGE_NAME:-household-data-system-base}:${VERSION:-latest}"
    local app_image="${REGISTRY_URL}/${APP_IMAGE_NAME:-household-data-system-app}:${VERSION:-latest}"
    
    docker pull "$base_image" || log_warning "拉取基础镜像失败: $base_image"
    docker pull "$app_image" || log_error "拉取应用镜像失败: $app_image"
    
    # 重新标记镜像
    docker tag "$base_image" "${BASE_IMAGE_NAME:-household-data-system-base}:${VERSION:-latest}" || true
    docker tag "$app_image" "${APP_IMAGE_NAME:-household-data-system-app}:${VERSION:-latest}" || true
}

# 部署开发环境
deploy_dev() {
    log_info "部署开发环境..."
    
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose.dev.yml up -d
    
    log_success "开发环境部署完成"
    log_info "访问地址: http://localhost:5000"
}

# 部署生产环境
deploy_prod() {
    log_info "部署生产环境..."
    
    cd "$PROJECT_ROOT"
    docker compose -f docker-compose.prod.yml up -d
    
    log_success "生产环境部署完成"
    log_info "访问地址: http://localhost:5000"
}

# 停止容器
stop_containers() {
    log_info "停止容器..."
    
    cd "$PROJECT_ROOT"
    
    # 尝试停止所有可能的compose配置
    docker compose -f docker-compose.dev.yml down 2>/dev/null || true
    docker compose -f docker-compose.prod.yml down 2>/dev/null || true
    docker compose down 2>/dev/null || true
    
    log_success "容器已停止"
}

# 显示容器状态
show_status() {
    log_info "容器状态:"
    echo ""
    
    # 显示相关容器
    docker ps -a --filter "name=household" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    log_info "镜像信息:"
    docker images | grep -E "(household-data-system|REPOSITORY)" || echo "未找到相关镜像"
}

# 显示日志
show_logs() {
    log_info "显示容器日志..."
    
    local container_name="household-data-system"
    if docker ps -q -f name="$container_name" > /dev/null; then
        docker logs -f "$container_name"
    else
        # 尝试开发环境容器名
        container_name="household-data-system-dev"
        if docker ps -q -f name="$container_name" > /dev/null; then
            docker logs -f "$container_name"
        else
            log_error "未找到运行中的容器"
            show_status
        fi
    fi
}

# 主函数
main() {
    local environment=""
    local force_build=false
    local pull_images_flag=false
    local show_logs_flag=false
    local show_status_flag=false
    local stop_flag=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -b|--build)
                force_build=true
                shift
                ;;
            -p|--pull)
                pull_images_flag=true
                shift
                ;;
            -d|--down)
                stop_flag=true
                shift
                ;;
            -l|--logs)
                show_logs_flag=true
                shift
                ;;
            -s|--status)
                show_status_flag=true
                shift
                ;;
            dev|prod)
                environment=$1
                shift
                ;;
            *)
                log_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 检查环境
    check_environment
    
    # 加载环境变量
    load_env
    
    # 处理特殊操作
    if [ "$stop_flag" = true ]; then
        stop_containers
        exit 0
    fi
    
    if [ "$show_status_flag" = true ]; then
        show_status
        exit 0
    fi
    
    if [ "$show_logs_flag" = true ]; then
        show_logs
        exit 0
    fi
    
    # 检查环境参数
    if [ -z "$environment" ]; then
        log_error "请指定部署环境: dev 或 prod"
        show_help
        exit 1
    fi
    
    # 构建或拉取镜像
    if [ "$pull_images_flag" = true ]; then
        pull_images
    else
        build_images "$force_build"
    fi
    
    # 部署应用
    case $environment in
        dev)
            deploy_dev
            ;;
        prod)
            deploy_prod
            ;;
        *)
            log_error "无效的环境: $environment"
            exit 1
            ;;
    esac
    
    # 显示部署状态
    echo ""
    show_status
    
    log_success "部署完成！"
}

# 执行主函数
main "$@"
