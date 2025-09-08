#!/bin/bash

# ================================
# Docker镜像加速测试脚本
# 测试DaoCloud镜像加速（方法二：前缀替换）
# ================================

set -euo pipefail

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

# 测试镜像拉取速度
test_image_pull() {
    local original_image="$1"
    local accelerated_image="$2"
    local description="$3"
    
    log_info "测试 $description"
    log_info "原始镜像: $original_image"
    log_info "加速镜像: $accelerated_image"
    
    # 清理可能存在的镜像
    docker rmi "$original_image" 2>/dev/null || true
    docker rmi "$accelerated_image" 2>/dev/null || true
    
    # 测试原始镜像拉取时间
    log_info "测试原始镜像拉取速度..."
    start_time=$(date +%s)
    if docker pull "$original_image" >/dev/null 2>&1; then
        original_time=$(($(date +%s) - start_time))
        log_success "原始镜像拉取成功，耗时: ${original_time}秒"
    else
        log_error "原始镜像拉取失败"
        original_time=999
    fi
    
    # 清理原始镜像
    docker rmi "$original_image" 2>/dev/null || true
    
    # 测试加速镜像拉取时间
    log_info "测试加速镜像拉取速度..."
    start_time=$(date +%s)
    if docker pull "$accelerated_image" >/dev/null 2>&1; then
        accelerated_time=$(($(date +%s) - start_time))
        log_success "加速镜像拉取成功，耗时: ${accelerated_time}秒"
        
        # 计算加速比例
        if [ $original_time -ne 999 ] && [ $original_time -gt 0 ]; then
            improvement=$((100 - (accelerated_time * 100 / original_time)))
            if [ $improvement -gt 0 ]; then
                log_success "加速效果: 提升 ${improvement}%"
            else
                log_warning "加速效果: 降低 $((0 - improvement))%"
            fi
        fi
    else
        log_error "加速镜像拉取失败"
    fi
    
    # 清理加速镜像
    docker rmi "$accelerated_image" 2>/dev/null || true
    
    echo ""
}

# 主函数
main() {
    log_info "开始测试DaoCloud镜像加速（方法二：前缀替换）"
    echo ""
    
    # 检查Docker是否可用
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装或不在PATH中"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker守护进程未运行或权限不足"
        exit 1
    fi
    
    log_success "Docker环境检查通过"
    echo ""
    
    # 测试不同的镜像仓库
    log_info "=== 测试Docker Hub镜像 ==="
    test_image_pull "python:3.9-slim" "docker.m.daocloud.io/library/python:3.9-slim" "Python基础镜像"
    
    log_info "=== 测试GitHub Container Registry镜像 ==="
    test_image_pull "ghcr.io/github/github-mcp-server:latest" "ghcr.m.daocloud.io/github/github-mcp-server:latest" "GitHub MCP Server"
    
    log_info "=== 测试Microsoft Container Registry镜像 ==="
    test_image_pull "mcr.microsoft.com/mssql/server:2019-latest" "mcr.m.daocloud.io/mssql/server:2019-latest" "SQL Server"
    
    log_info "=== 测试Nginx镜像 ==="
    test_image_pull "nginx:alpine" "docker.m.daocloud.io/library/nginx:alpine" "Nginx Alpine"
    
    log_success "所有测试完成"
    
    # 显示DaoCloud使用建议
    echo ""
    log_info "=== DaoCloud使用建议 ==="
    echo "1. 建议在凌晨时段（北京时间01-07点）执行拉取任务"
    echo "2. 推荐使用明确的版本号标签，避免使用latest标签"
    echo "3. 镜像可能存在最多1小时的缓存延迟"
    echo "4. 所有镜像的hash(sha256)与源保持一致"
    echo ""
    
    log_info "=== 支持的镜像仓库映射 ==="
    echo "docker.io → docker.m.daocloud.io"
    echo "gcr.io → gcr.m.daocloud.io"
    echo "ghcr.io → ghcr.m.daocloud.io"
    echo "k8s.gcr.io → k8s-gcr.m.daocloud.io"
    echo "registry.k8s.io → k8s.m.daocloud.io"
    echo "mcr.microsoft.com → mcr.m.daocloud.io"
    echo "quay.io → quay.m.daocloud.io"
}

# 执行主函数
main "$@"
