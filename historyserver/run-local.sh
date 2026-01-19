#!/bin/bash
# Local History Server 启动脚本
# 用于在本地开发环境直接运行 historyserver

set -e

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Ray History Server Local Runner ===${NC}"

# 1. 检查二进制文件是否存在
BINARY="./output/bin/historyserver"
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}错误: 二进制文件不存在: $BINARY${NC}"
    echo -e "${YELLOW}请先运行: make buildhistoryserver${NC}"
    exit 1
fi

# 2. 检查 Dashboard 目录
DASHBOARD_DIR="./dashboard"
if [ ! -d "$DASHBOARD_DIR/v2.51.0/client/build" ]; then
    echo -e "${RED}错误: Dashboard 构建目录不存在: $DASHBOARD_DIR/v2.51.0/client/build${NC}"
    echo -e "${YELLOW}请先运行: cd dashboard/v2.51.0/client && npm ci && npm run build${NC}"
    exit 1
fi

# 3. 设置 S3/MinIO 环境变量
# 注意：这些值需要根据你的实际环境修改
export S3DISABLE_SSL="true"
export AWS_S3ID="minioadmin"
export AWS_S3SECRET="minioadmin"
export AWS_S3TOKEN=""
export S3_BUCKET="ray-historyserver"
export S3_ENDPOINT="192.168.129.137:30900"  # 修改为你的 MinIO 地址
export S3_REGION="test"
export S3FORCE_PATH_STYLE="true"

echo -e "${GREEN}环境变量配置:${NC}"
echo "  S3_ENDPOINT: $S3_ENDPOINT"
echo "  S3_BUCKET: $S3_BUCKET"
echo "  AWS_S3ID: $AWS_S3ID"
echo ""

# 4. 可选：使用配置文件（JSON 格式）
CONFIG_FILE="./local-config.json"
CONFIG_ARG=""
if [ -f "$CONFIG_FILE" ]; then
    echo -e "${GREEN}使用配置文件: $CONFIG_FILE${NC}"
    CONFIG_ARG="--runtime-class-config-path=$CONFIG_FILE"
fi

# 5. 启动 History Server
echo -e "${GREEN}启动 History Server...${NC}"
echo -e "${YELLOW}访问地址: http://localhost:8080${NC}"
echo ""

$BINARY \
    --runtime-class-name=s3 \
    --ray-root-dir=log \
    --dashboard-dir=$DASHBOARD_DIR \
    $CONFIG_ARG

# 说明：
# --runtime-class-name=s3   : 使用 S3 存储后端（支持 MinIO）
# --ray-root-dir=log        : Ray 日志在 S3 中的根路径前缀
# --dashboard-dir=./dashboard : Dashboard 静态文件目录（本地相对路径）
# --runtime-class-config-path : 可选，使用 JSON 配置文件（优先级高于环境变量）
