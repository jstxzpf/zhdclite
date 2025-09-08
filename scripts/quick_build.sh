#!/bin/bash

# ================================
# 快速构建脚本 - 仅构建应用镜像
# 适用于日常开发中的代码变更
# ================================
#
# 使用方法:
#   ./scripts/quick_build.sh                    # 智能构建（检查缓存）
#   FORCE_BUILD=true ./scripts/quick_build.sh   # 强制重新构建
#   VERSION=v1.0 ./scripts/quick_build.sh       # 指定版本标签
#
# DaoCloud镜像加速特性:
#   - 使用前缀替换方式: docker.m.daocloud.io/library/python:3.9-slim
#   - 自动检测闲时（01:00-07:00）获得最佳性能
#   - 支持多镜像源自动回退
#   - 建议使用明确版本号而非latest
#   - 镜像层缓存在第三方存储，二次拉取更快
#
# 功能特性:
#   - 智能缓存：检查镜像是否已存在且新鲜
#   - 代码变更检测：对比Git提交时间
#   - 网络优化：DaoCloud镜像加速优先
#   - 超时控制：防止构建过程卡住
#   - 详细日志：构建过程可视化
# ================================

set -e

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 项目配置
PROJECT_NAME="household-data-system"
BASE_IMAGE_NAME="${PROJECT_NAME}-base"
APP_IMAGE_NAME="${PROJECT_NAME}-app"
VERSION="${VERSION:-latest}"

# 镜像源配置（使用DaoCloud推荐的前缀替换方式）
MIRROR_REGISTRIES=(
    "docker.m.daocloud.io"           # DaoCloud镜像加速（前缀替换方式）
    "registry.docker-cn.com"         # 阿里云镜像加速
    "docker.mirrors.ustc.edu.cn"     # 中科大镜像加速
    ""                               # 官方源作为最后回退
)

# DaoCloud前缀替换映射表
declare -A REGISTRY_MAPPING=(
    ["docker.io"]="docker.m.daocloud.io"
    ["gcr.io"]="gcr.m.daocloud.io" 
    ["ghcr.io"]="ghcr.m.daocloud.io"
    ["k8s.gcr.io"]="k8s-gcr.m.daocloud.io"
    ["registry.k8s.io"]="k8s.m.daocloud.io"
    ["mcr.microsoft.com"]="mcr.m.daocloud.io"
    ["nvcr.io"]="nvcr.m.daocloud.io"
    ["quay.io"]="quay.m.daocloud.io"
    ["docker.elastic.co"]="elastic.m.daocloud.io"
)

# 测试镜像源连通性并选择最佳源
select_best_mirror() {
    echo -e "${BLUE}[INFO]${NC} 测试DaoCloud镜像加速服务..." >&2
    
    # 检查当前时间，建议闲时使用
    current_hour=$(date +%H)
    if [ $current_hour -ge 1 ] && [ $current_hour -le 7 ]; then
        echo -e "${GREEN}[INFO]${NC} 当前为闲时（01:00-07:00），镜像加速效果最佳" >&2
    else
        echo -e "${YELLOW}[INFO]${NC} 当前为繁忙时段，建议在凌晨1-7点进行大量镜像拉取" >&2
    fi
    
    # 首先测试DaoCloud镜像加速
    if timeout 8 curl -s --head "https://docker.m.daocloud.io" > /dev/null 2>&1; then
        echo -e "${GREEN}[INFO]${NC} 选择DaoCloud镜像加速: docker.m.daocloud.io" >&2
        echo "docker.m.daocloud.io"
        return 0
    else
        echo -e "${YELLOW}[WARNING]${NC} DaoCloud镜像服务连通异常，尝试其他镜像源..." >&2
    fi
    
    # 测试其他镜像源
    for mirror in "${MIRROR_REGISTRIES[@]:1}"; do  # 跳过第一个DaoCloud源
        if [ -z "$mirror" ]; then
            echo -e "${BLUE}[INFO]${NC} 使用Docker官方源" >&2
            echo ""
            return 0
        fi
        
        # 测试镜像源连通性
        if timeout 5 curl -s --head "https://$mirror" > /dev/null 2>&1; then
            echo -e "${GREEN}[INFO]${NC} 选择镜像源: $mirror" >&2
            echo "$mirror"
            return 0
        else
            echo -e "${YELLOW}[WARNING]${NC} 镜像源 $mirror 连通异常，尝试下一个..." >&2
        fi
    done
    
    # 如果所有镜像源都无法连通，使用官方源
    echo -e "${YELLOW}[WARNING]${NC} 所有镜像源测试失败，使用官方源" >&2
    echo ""
    return 0
}

# Docker镜像加速配置函数
configure_docker_mirror() {
    echo -e "${BLUE}[INFO]${NC} 检查Docker镜像加速配置..."
    
    # 检查daemon.json文件
    DAEMON_JSON="/etc/docker/daemon.json"
    
    if [ -f "$DAEMON_JSON" ]; then
        # 检查是否已经配置了镜像加速
        if grep -q "registry-mirrors" "$DAEMON_JSON" 2>/dev/null; then
            echo -e "${GREEN}[INFO]${NC} Docker镜像加速已配置"
            return 0
        fi
    fi
    
    # 如果未配置镜像加速，提供配置指导
    echo -e "${YELLOW}[WARNING]${NC} 未检测到Docker镜像加速配置"
    echo -e "${BLUE}[INFO]${NC} 建议配置以下镜像源以提高下载速度："
    echo -e "${BLUE}[INFO]${NC} 编辑 /etc/docker/daemon.json 添加："
    echo -e "{"
    echo -e "  \"registry-mirrors\": ["
    echo -e "    \"https://docker.m.daocloud.io\","
    echo -e "    \"https://registry.docker-cn.com\","
    echo -e "    \"https://docker.mirrors.ustc.edu.cn\""
    echo -e "  ]"
    echo -e "}"
    echo -e "${BLUE}[INFO]${NC} 配置后需要重启Docker: sudo systemctl restart docker"
    echo ""
}

# 镜像存在性和新鲜度检查函数
check_image_freshness() {
    local image_name="$1"
    local max_age_hours="${2:-24}"  # 默认24小时内的镜像认为是新鲜的
    
    echo -e "${BLUE}[INFO]${NC} 检查镜像 ${image_name} 的存在性和新鲜度..."
    
    # 检查镜像是否存在
    if ! docker image inspect "${image_name}" &> /dev/null; then
        echo -e "${YELLOW}[INFO]${NC} 镜像 ${image_name} 不存在，需要构建"
        return 1
    fi
    
    # 获取镜像创建时间
    local image_created=$(docker image inspect "${image_name}" --format '{{.Created}}' 2>/dev/null)
    if [ -z "$image_created" ]; then
        echo -e "${YELLOW}[WARNING]${NC} 无法获取镜像创建时间，将重新构建"
        return 1
    fi
    
    # 计算镜像年龄（小时）
    local created_timestamp=$(date -d "$image_created" +%s 2>/dev/null || echo "0")
    local current_timestamp=$(date +%s)
    local age_hours=$(( (current_timestamp - created_timestamp) / 3600 ))
    
    if [ $age_hours -gt $max_age_hours ]; then
        echo -e "${YELLOW}[INFO]${NC} 镜像 ${image_name} 已存在但较旧（${age_hours}小时前创建），建议重新构建"
        return 1
    else
        echo -e "${GREEN}[INFO]${NC} 镜像 ${image_name} 存在且新鲜（${age_hours}小时前创建），跳过构建"
        return 0
    fi
}

# 检查源代码是否有变更
check_source_changes() {
    echo -e "${BLUE}[INFO]${NC} 检查源代码变更..."
    
    # 检查是否有未提交的变更
    if ! git diff --quiet HEAD 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
        echo -e "${YELLOW}[INFO]${NC} 检测到源代码变更，需要重新构建"
        return 1
    fi
    
    # 检查镜像是否比最新提交更新
    if docker image inspect "${APP_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        local image_created=$(docker image inspect "${APP_IMAGE_NAME}:${VERSION}" --format '{{.Created}}' 2>/dev/null)
        local last_commit_time=$(git log -1 --format="%cI" 2>/dev/null || echo "")
        
        if [ -n "$image_created" ] && [ -n "$last_commit_time" ]; then
            local image_timestamp=$(date -d "$image_created" +%s 2>/dev/null || echo "0")
            local commit_timestamp=$(date -d "$last_commit_time" +%s 2>/dev/null || echo "1")
            
            if [ $image_timestamp -gt $commit_timestamp ]; then
                echo -e "${GREEN}[INFO]${NC} 镜像比最新代码提交更新，无需重新构建"
                return 0
            fi
        fi
    fi
    
    echo -e "${YELLOW}[INFO]${NC} 需要根据最新代码重新构建镜像"
    return 1
}

# 主执行函数
main() {
    echo -e "${BLUE}[INFO]${NC} 快速构建应用镜像..."

    # 配置Docker镜像加速
    configure_docker_mirror

    # 选择最佳镜像源
    SELECTED_MIRROR=$(select_best_mirror)
    
    # 如果选择了DaoCloud镜像，显示优化提示
    if [[ "$SELECTED_MIRROR" == *"m.daocloud.io"* ]]; then
        show_daocloud_tips
        echo ""
    fi
    
    # 检查基础镜像是否存在
    if ! docker image inspect "${BASE_IMAGE_NAME}:${VERSION}" &> /dev/null; then
        echo -e "${RED}[ERROR]${NC} 基础镜像不存在，请先运行: ./scripts/build_multistage.sh -b"
        exit 1
    fi

    # 智能构建决策：检查是否需要重新构建
    FORCE_BUILD="${FORCE_BUILD:-false}"
    SKIP_BUILD=false

    if [ "$FORCE_BUILD" = "true" ]; then
        echo -e "${BLUE}[INFO]${NC} 强制构建模式，跳过缓存检查"
    elif check_image_freshness "${APP_IMAGE_NAME}:${VERSION}" 12; then
        # 如果镜像存在且在12小时内创建，检查源代码变更
        if check_source_changes; then
            echo -e "${GREEN}[SUCCESS]${NC} 应用镜像 ${APP_IMAGE_NAME}:${VERSION} 已是最新，跳过构建"
            SKIP_BUILD=true
        else
            echo -e "${BLUE}[INFO]${NC} 检测到代码变更，继续构建"
        fi
    else
        echo -e "${BLUE}[INFO]${NC} 镜像不存在或已过期，继续构建"
    fi

    # 如果需要跳过构建，直接显示结果并退出
    if [ "$SKIP_BUILD" = "true" ]; then
        echo -e "${BLUE}[INFO]${NC} 构建时间: $(date)"
        echo -e "${BLUE}[INFO]${NC} 当前镜像信息:"
        docker images | grep -E "(${BASE_IMAGE_NAME}|${APP_IMAGE_NAME})" | head -2
        exit 0
    fi

    # 构建应用镜像（添加超时和重试机制）
    echo -e "${BLUE}[INFO]${NC} 开始构建应用镜像: ${APP_IMAGE_NAME}:${VERSION}"
    echo -e "${BLUE}[INFO]${NC} 使用镜像源: ${SELECTED_MIRROR:-Docker官方源}"
    echo -e "${BLUE}[INFO]${NC} 提示：使用 FORCE_BUILD=true 可强制重新构建"

    DOCKER_BUILD_TIMEOUT=1200  # 20分钟超时
    
    # 构建命令，根据选择的镜像源传递构建参数
    BUILD_ARGS=""
    # 使用DaoCloud镜像加速（方法二：前缀替换）
    BUILD_ARGS="--build-arg REGISTRY_PREFIX=docker.m.daocloud.io --build-arg PYTHON_VERSION=3.9-slim"

    echo -e "${BLUE}[INFO]${NC} 使用DaoCloud镜像加速（前缀替换方法）"
    echo -e "${BLUE}[INFO]${NC} 原始镜像: docker.io/library/python:3.9-slim"
    echo -e "${BLUE}[INFO]${NC} 加速镜像: docker.m.daocloud.io/library/python:3.9-slim"
    
    timeout $DOCKER_BUILD_TIMEOUT docker build \
        --target app-image \
        --tag "${APP_IMAGE_NAME}:${VERSION}" \
        --label "build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        --label "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
        --label "mirror_used=${SELECTED_MIRROR:-official}" \
        $BUILD_ARGS \
        . || {
        
        # 如果构建失败且使用了镜像源，尝试使用官方源重试
        if [ -n "$SELECTED_MIRROR" ]; then
            echo -e "${YELLOW}[WARNING]${NC} 使用镜像源构建失败，尝试使用Docker官方源重试..."
            timeout $DOCKER_BUILD_TIMEOUT docker build \
                --target app-image \
                --tag "${APP_IMAGE_NAME}:${VERSION}" \
                --label "build_date=$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
                --label "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
                --label "mirror_used=official" \
                . || {
                echo -e "${RED}[ERROR]${NC} 构建失败"
                show_build_troubleshooting
                exit 1
            }
        else
            echo -e "${RED}[ERROR]${NC} 构建失败"
            show_build_troubleshooting
            exit 1
        fi
    }

    echo -e "${GREEN}[SUCCESS]${NC} 应用镜像构建完成: ${APP_IMAGE_NAME}:${VERSION}"
    echo -e "${BLUE}[INFO]${NC} 构建时间: $(date)"

    # 显示镜像大小
    echo -e "${BLUE}[INFO]${NC} 镜像大小:"
    docker images | grep -E "(${BASE_IMAGE_NAME}|${APP_IMAGE_NAME})" | head -2
}

# 构建故障排除提示函数
show_build_troubleshooting() {
    echo -e "${BLUE}[INFO]${NC} 构建故障排除建议："
    echo -e "${BLUE}[INFO]${NC} 1. 检查网络连接"
    echo -e "${BLUE}[INFO]${NC} 2. 配置Docker镜像加速（参考上方提示）"
    echo -e "${BLUE}[INFO]${NC} 3. 清理Docker缓存: docker system prune -f"
    echo -e "${BLUE}[INFO]${NC} 4. 使用 FORCE_BUILD=true 强制重新构建"
    echo -e "${BLUE}[INFO]${NC} 5. 在闲时（凌晨1-7点）重试构建，网络更稳定"
    echo -e "${BLUE}[INFO]${NC} 6. 使用明确版本标签: VERSION=v1.0 ./scripts/quick_build.sh"
    echo -e "${BLUE}[INFO]${NC} 7. 检查DaoCloud镜像服务状态: https://github.com/DaoCloud/public-image-mirror"
}

# DaoCloud镜像优化提示函数  
show_daocloud_tips() {
    echo -e "${GREEN}[TIPS]${NC} DaoCloud镜像加速优化建议："
    echo -e "${GREEN}[TIPS]${NC} • 闲时构建（01:00-07:00）获得最佳性能"
    echo -e "${GREEN}[TIPS]${NC} • 使用明确版本号而非latest标签"
    echo -e "${GREEN}[TIPS]${NC} • 镜像层会缓存在第三方存储，首次拉取后速度显著提升"
    echo -e "${GREEN}[TIPS]${NC} • 所有hash(sha256)与源保持一致，安全可靠"
    echo -e "${GREEN}[TIPS]${NC} • 可能存在最多1小时的缓存延迟"
}

# 如果脚本被直接执行（而不是被source），则运行main函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
