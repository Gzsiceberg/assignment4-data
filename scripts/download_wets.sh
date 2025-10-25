#!/usr/bin/env bash

set -euo pipefail

FILE_PATH="data/wet.paths.gz"
URL="http://data.commoncrawl.org/"
MAX_COUNT=10
PARALLEL_JOBS=8  # 并发下载数量
DOWNLOAD_DIR="data/warc_wets/"

# 解析命令行参数
if [ ! -z "${1:-}" ]; then
    MAX_COUNT=$1
fi

if [ ! -z "${2:-}" ]; then
    PARALLEL_JOBS=$2
fi

# 确保下载目录存在
mkdir -p "$DOWNLOAD_DIR"

# 检查必要的工具
if ! command -v aria2c &> /dev/null; then
    echo "错误: aria2c 未安装，请先安装 aria2c"
    exit 1
fi

echo "使用 aria2c 进行下载"

# 生成下载列表
TEMP_LIST=$(mktemp)
trap "rm -f $TEMP_LIST" EXIT

echo "正在生成下载列表（前 $MAX_COUNT 个文件）..."

(zcat "$FILE_PATH" | head -n "$MAX_COUNT" || true) | while IFS= read -r line; do
    file_name=$(basename "$line")
    echo "${URL}${line}"
    echo "  out=${file_name}"
    echo "  dir=${DOWNLOAD_DIR}"
done > "$TEMP_LIST"

# 统计需要下载的文件数
DOWNLOAD_COUNT=$(grep -c "^http" "$TEMP_LIST" || echo "0")

echo "准备下载 $DOWNLOAD_COUNT 个文件（aria2c 会自动跳过已完成的文件）"

if [ "$DOWNLOAD_COUNT" -eq 0 ]; then
    echo "没有需要下载的文件"
    exit 0
fi

# 使用 aria2c 进行并发下载
# aria2c 配置：多线程、断点续传、自动重试
aria2c \
    --input-file="$TEMP_LIST" \
    --max-concurrent-downloads="$PARALLEL_JOBS" \
    --max-connection-per-server=4 \
    --min-split-size=1M \
    --split=4 \
    --continue=true \
    --max-tries=5 \
    --retry-wait=3 \
    --timeout=60 \
    --connect-timeout=30 \
    --console-log-level=notice \
    --summary-interval=10

echo "下载完成！"
