#!/bin/bash
# 快速重启 historyserver（本地调试版本）

set -e

echo "=========================================="
echo "Restarting Historyserver (Local Debug)"
echo "=========================================="
echo ""

# 进入项目目录
cd "$(dirname "$0")/.."

# 1. 重新编译
echo "📦 Step 1: Building historyserver..."
make historyserver
echo "✅ Build complete"
echo ""

# 2. 杀掉旧进程
echo "🔪 Step 2: Killing old historyserver processes..."
pkill -f "historyserver.*--port=8081" || echo "   (No old process found)"
sleep 2
echo "✅ Old processes killed"
echo ""

# 3. 启动新进程
echo "🚀 Step 3: Starting historyserver..."
echo ""
echo "Command:"
echo "./output/bin/historyserver \\"
echo "  --runtime-class-name=cos \\"
echo "  --ray-root-dir=mce-proj-bk0lwhh1 \\"
echo "  --dashboard-dir=./dashboard \\"
echo "  --port=8081"
echo ""
echo "=========================================="
echo "Logs will appear below:"
echo "=========================================="
echo ""

# 启动（前台运行，显示所有日志）
./output/bin/historyserver \
  --runtime-class-name=cos \
  --ray-root-dir=mce-proj-bk0lwhh1 \
  --dashboard-dir=./dashboard \
  --port=8081 2>&1 | tee /tmp/historyserver.log
