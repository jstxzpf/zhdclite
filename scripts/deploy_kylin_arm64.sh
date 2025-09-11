#!/usr/bin/env bash
# 部署脚本：在 ARM64 (aarch64) 架构的 Kylin V10 上安装 Miniconda（若缺失），
# 并创建/检测名为 zhdclite 的 Python 3.12 虚拟环境，安装项目依赖。
# 使用：bash scripts/deploy_kylin_arm64.sh

set -Eeuo pipefail

# ----------------------- 辅助输出函数 -----------------------
log()  { printf "\033[1;34m[INFO]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[WARN]\033[0m %s\n" "$*"; }
err()  { printf "\033[1;31m[ERR ]\033[0m %s\n" "$*"; }

# ----------------------- 目录与上下文 -----------------------
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "$REPO_ROOT"

# ----------------------- 环境与架构检测 -----------------------
ARCH=$(uname -m || true)
OS=$(uname -s || true)
log "检测到系统: OS=${OS}, ARCH=${ARCH}"
if [[ "$ARCH" != "aarch64" && "$ARCH" != "arm64" ]]; then
  warn "当前架构为 ${ARCH}，而非 aarch64/arm64。脚本仍将尝试继续，但请确保选择正确架构的 Miniconda 安装包。"
fi

# ----------------------- 网络下载工具检测 ---------------------
if command -v curl >/dev/null 2>&1; then
  DOWNLOADER="curl -fL --retry 3 --retry-delay 2 -o"
elif command -v wget >/dev/null 2>&1; then
  DOWNLOADER="wget -O"
else
  err "未检测到 curl 或 wget，请先安装其中之一再重试。"
  exit 1
fi

# ----------------------- Conda 安装/初始化 --------------------
MINICONDA_HOME="$HOME/miniconda3"
MINICONDA_BIN="$MINICONDA_HOME/bin"
MINICONDA_CONDA="$MINICONDA_BIN/conda"

ensure_miniconda() {
  if command -v conda >/dev/null 2>&1; then
    log "检测到系统已安装 conda：$(command -v conda)"
    return 0
  fi
  if [[ -x "$MINICONDA_CONDA" ]]; then
    log "检测到本地 Miniconda：$MINICONDA_CONDA"
  else
    log "未检测到 conda，开始下载并安装 Miniconda (aarch64) 到 $MINICONDA_HOME"
    INSTALLER="/tmp/Miniconda3-latest-Linux-aarch64.sh"
    $DOWNLOADER "$INSTALLER" "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh"
    bash "$INSTALLER" -b -p "$MINICONDA_HOME"
  fi

  # 将 conda 注入当前 shell 会话（无需重开终端）
  # shellcheck disable=SC1091
  source "$MINICONDA_HOME/etc/profile.d/conda.sh" || true
  if ! command -v conda >/dev/null 2>&1; then
    # 备用方式：通过 hook 注入
    eval "$("$MINICONDA_CONDA" shell.bash hook)"
  fi

  # 永久化初始化（写入 ~/.bashrc），便于后续交互式终端使用
  if ! grep -q "conda initialize" "$HOME/.bashrc" 2>/dev/null; then
    log "执行 conda init bash（写入 ~/.bashrc）"
    "$MINICONDA_CONDA" init bash || true
  fi
}

# 在当前脚本会话中启用 conda 命令
activate_conda_in_shell() {
  if command -v conda >/dev/null 2>&1; then
    return 0
  fi
  if [[ -x "$MINICONDA_CONDA" ]]; then
    # shellcheck disable=SC1091
    source "$MINICONDA_HOME/etc/profile.d/conda.sh" || true
    eval "$("$MINICONDA_CONDA" shell.bash hook)"
  fi
}

# ----------------------- 创建/检测环境 ------------------------
ENV_NAME="zhdclite"
PY_VERSION="3.12"

ensure_env() {
  activate_conda_in_shell
  if conda env list | awk '{print $1}' | grep -xq "$ENV_NAME"; then
    log "已存在虚拟环境: $ENV_NAME"
  else
    log "创建虚拟环境: $ENV_NAME (Python ${PY_VERSION})"
    conda create -y -n "$ENV_NAME" "python=${PY_VERSION}"
  fi
}

activate_env() {
  activate_conda_in_shell
  # shellcheck disable=SC1091
  conda activate "$ENV_NAME"
  log "已激活环境: $ENV_NAME ($(python -V 2>&1))"
}

check_python_version() {
  ACTUAL_MINOR=$(python -c 'import sys; print("%d.%d" % (sys.version_info[0], sys.version_info[1]))')
  if [[ "$ACTUAL_MINOR" != "$PY_VERSION" ]]; then
    warn "环境 $ENV_NAME 的 Python 版本为 $ACTUAL_MINOR，非期望 ${PY_VERSION}。如需严格 3.12，请删除环境后重建：conda remove -n $ENV_NAME --all && 重新运行本脚本。"
  fi
}

# ----------------------- 安装依赖 ------------------------------
install_deps() {
  activate_env
  check_python_version

  # 升级 pip 基础工具
  python -m pip install -U pip setuptools wheel

  # 优先使用 environment.yml，如不存在则使用 requirements*.txt
  if [[ -f "$REPO_ROOT/environment.yml" || -f "$REPO_ROOT/environment.yaml" ]]; then
    local ENV_FILE
    if [[ -f "$REPO_ROOT/environment.yml" ]]; then ENV_FILE="$REPO_ROOT/environment.yml"; else ENV_FILE="$REPO_ROOT/environment.yaml"; fi
    log "检测到 ${ENV_FILE}，使用 conda env update 同步依赖（不会破坏已存在环境）。"
    conda env update -n "$ENV_NAME" -f "$ENV_FILE" || {
      warn "conda env update 失败，尝试继续使用 pip 安装 requirements.txt（如果存在）。"
    }
  fi

  # requirements*.txt 安装（仅当文件存在）
  local REQ_FILE=""
  if [[ -f "$REPO_ROOT/requirements.txt" ]]; then
    REQ_FILE="$REPO_ROOT/requirements.txt"
  else
    # 查找其他命名
    REQ_FILE=$(ls "$REPO_ROOT"/requirements*.txt 2>/dev/null | head -n1 || true)
  fi

  if [[ -n "$REQ_FILE" && -f "$REQ_FILE" ]]; then
    log "使用 pip 安装依赖：$REQ_FILE"
    pip install -r "$REQ_FILE"
  else
    warn "未找到 requirements*.txt；若项目依赖由 conda environment.yml 完整描述则可忽略。"
  fi
}

# ----------------------- 主流程 -------------------------------
main() {
  log "开始执行部署脚本（Kylin V10 / ARM64）"
  ensure_miniconda
  activate_conda_in_shell
  ensure_env
  activate_env
  install_deps
  log "部署完成。后续使用方法："
  echo "  1) 重新打开终端或执行：source ~/.bashrc"
  echo "  2) 激活环境：conda activate ${ENV_NAME}"
  echo "  3) 运行应用（示例）：python app.py"
}

main "$@"

