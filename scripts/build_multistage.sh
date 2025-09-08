#!/bin/bash

# ================================
# HOUSEHOLD_DATA_SYSTEM_FLASK 多阶段构建脚本
# ================================

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置变量
PROJECT_NAME="household-data-system"
BASE_IMAGE_NAME="${PROJECT_NAME}-base"
APP_IMAGE_NAME="${PROJECT_NAME}-app"
REGISTRY_URL="${REGISTRY_URL:-}"  # 可通过环境变量设置镜像仓库地址
VERSION="${VERSION:-latest}"

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
HOUSEHOLD_DATA_SYSTEM_FLASK 多阶段构建脚本

用法: $0 [选项]

选项:
    -h, --help              显示此帮助信息
    -b, --base-only         仅构建基础镜像
    -a, --app-only          仅构建应用镜像（需要基础镜像存在）
    -f, --full              完整构建（基础镜像 + 应用镜像）
    -p, --push              构建后推送到镜像仓库
    -c, --check-deps        检查依赖变更（自动决定是否重建基础镜像）
    -v, --version VERSION   指定镜像版本标签（默认: latest）
    -r, --registry URL      指定镜像仓库地址

示例:
    $0 -f                   # 完整构建
    $0 -b -p                # 仅构建基础镜像并推送
    $0 -a                   # 仅构建应用镜像
    $0 -c                   # 智能检查并构建
    $0 -f -v v1.2.3         # 构建指定版本

环境变量:
    REGISTRY_URL            镜像仓库地址
    VERSION                 镜像版本标签
    DOCKER_BUILDKIT         启用BuildKit（推荐设置为1）

EOF
}

# 检查Docker是否可用
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装或不在PATH中"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker守护进程未运行或权限不足"
        exit 1
    fi
    
    log_info "Docker环境检查通过"
}

# 检查依赖文件是否变更
check_dependencies_changed() {
    local base_image_exists=false
    local deps_changed=false
    
    # 检查基础镜像是否存在
    if docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        base_image_exists=true
        log_info "基础镜像 ${BASE_IMAGE_NAME}:${VERSION} 已存在"
    else
        log_warning "基础镜像 ${BASE_IMAGE_NAME}:${VERSION} 不存在"
        return 0  # 需要构建
    fi
    
    # 检查requirements.txt是否变更
    if [ -f "requirements.txt" ]; then
        local current_hash=$(sha256sum requirements.txt | cut -d' ' -f1)
        local stored_hash=""
        
        # 尝试从镜像标签中获取依赖哈希
        if docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" --format '{{.Config.Labels.deps_hash}}' &> /dev/null; then
            stored_hash=$(docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" --format '{{.Config.Labels.deps_hash}}' 2>/dev/null || echo "")
        fi
        
        if [ "$current_hash" != "$stored_hash" ]; then
            log_warning "requirements.txt已变更，需要重建基础镜像"
            deps_changed=true
        else
            log_info "依赖文件未变更"
        fi
    fi
    
    if [ "$base_image_exists" = false ] || [ "$deps_changed" = true ]; then
        return 0  # 需要构建
    else
        return 1  # 不需要构建
    fi
}

# 构建基础镜像
build_base_image() {
    log_info "开始构建基础镜像: ${BASE_IMAGE_NAME}:${VERSION}"
    
    # 计算依赖文件哈希
    local deps_hash=""
    if [ -f "requirements.txt" ]; then
        deps_hash=$(sha256sum requirements.txt | cut -d' ' -f1)
    fi
    
    # 构建基础镜像，使用DaoCloud镜像加速（方法二：前缀替换）
    docker build \
        --target base-image \
        --tag "${BASE_IMAGE_NAME}:${VERSION}" \
        --build-arg REGISTRY_PREFIX="docker.m.daocloud.io" \
        --build-arg PYTHON_VERSION="3.9-slim" \
        --label "deps_hash=${deps_hash}" \
        --label "build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --label "version=${VERSION}" \
        --label "registry_mirror=docker.m.daocloud.io" \
        .
    
    log_success "基础镜像构建完成: ${BASE_IMAGE_NAME}:${VERSION}"
}

# 构建应用镜像
build_app_image() {
    log_info "开始构建应用镜像: ${APP_IMAGE_NAME}:${VERSION}"
    
    # 检查基础镜像是否存在
    if ! docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        log_error "基础镜像 ${BASE_IMAGE_NAME}:${VERSION} 不存在，请先构建基础镜像"
        exit 1
    fi
    
    # 构建应用镜像，使用DaoCloud镜像加速（方法二：前缀替换）
    docker build \
        --target app-image \
        --tag "${APP_IMAGE_NAME}:${VERSION}" \
        --build-arg REGISTRY_PREFIX="docker.m.daocloud.io" \
        --build-arg PYTHON_VERSION="3.9-slim" \
        --label "base_image=${BASE_IMAGE_NAME}:${VERSION}" \
        --label "build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --label "version=${VERSION}" \
        --label "registry_mirror=docker.m.daocloud.io" \
        .
    
    log_success "应用镜像构建完成: ${APP_IMAGE_NAME}:${VERSION}"
}

# 推送镜像到仓库
push_images() {
    if [ -z "$REGISTRY_URL" ]; then
        log_warning "未设置镜像仓库地址，跳过推送"
        return
    fi
    
    log_info "推送镜像到仓库: $REGISTRY_URL"
    
    # 推送基础镜像
    if docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        local base_remote_tag="${REGISTRY_URL}/${BASE_IMAGE_NAME}:${VERSION}"
        docker tag "${BASE_IMAGE_NAME}:${VERSION}" "$base_remote_tag"
        docker push "$base_remote_tag"
        log_success "基础镜像推送完成: $base_remote_tag"
    fi
    
    # 推送应用镜像
    if docker image inspect "${APP_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        local app_remote_tag="${REGISTRY_URL}/${APP_IMAGE_NAME}:${VERSION}"
        docker tag "${APP_IMAGE_NAME}:${VERSION}" "$app_remote_tag"
        docker push "$app_remote_tag"
        log_success "应用镜像推送完成: $app_remote_tag"
    fi
}

# 显示构建信息
show_build_info() {
    log_info "构建信息:"
    echo "  项目名称: $PROJECT_NAME"
    echo "  版本标签: $VERSION"
    echo "  基础镜像: ${BASE_IMAGE_NAME}:${VERSION}"
    echo "  应用镜像: ${APP_IMAGE_NAME}:${VERSION}"
    if [ -n "$REGISTRY_URL" ]; then
        echo "  镜像仓库: $REGISTRY_URL"
    fi
    echo ""
}

# 主函数
main() {
    local build_base=false
    local build_app=false
    local push_after_build=false
    local check_deps=false
    local full_build=false
    
    # 解析命令行参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -b|--base-only)
                build_base=true
                shift
                ;;
            -a|--app-only)
                build_app=true
                shift
                ;;
            -f|--full)
                full_build=true
                shift
                ;;
            -p|--push)
                push_after_build=true
                shift
                ;;
            -c|--check-deps)
                check_deps=true
                shift
                ;;
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            -r|--registry)
                REGISTRY_URL="$2"
                shift 2
                ;;
            *)
                log_error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # 如果没有指定任何构建选项，默认为智能检查
    if [ "$build_base" = false ] && [ "$build_app" = false ] && [ "$full_build" = false ] && [ "$check_deps" = false ]; then
        check_deps=true
    fi
    
    # 检查Docker环境
    check_docker
    
    # 显示构建信息
    show_build_info
    
    # 启用BuildKit（如果可用）
    export DOCKER_BUILDKIT=1
    
    # 执行构建逻辑
    if [ "$check_deps" = true ]; then
        log_info "智能检查依赖变更..."
        if check_dependencies_changed; then
            log_info "检测到依赖变更，执行完整构建"
            build_base=true
            build_app=true
        else
            log_info "依赖未变更，仅构建应用镜像"
            build_app=true
        fi
    fi
    
    if [ "$full_build" = true ]; then
        build_base=true
        build_app=true
    fi
    
    # 构建基础镜像
    if [ "$build_base" = true ]; then
        build_base_image
    fi
    
    # 构建应用镜像
    if [ "$build_app" = true ]; then
        build_app_image
    fi
    
    # 推送镜像
    if [ "$push_after_build" = true ]; then
        push_images
    fi
    
    log_success "构建流程完成！"
}

# 执行主函数
main "$@"
