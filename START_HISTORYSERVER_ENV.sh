#!/bin/bash
set -e

echo "=========================================="
echo "Starting KubeRay HistoryServer (ENV Mode)"
echo "=========================================="
echo ""

# 停止旧进程
echo "🛑 Stopping old historyserver processes..."
pkill -f "historyserver.*--runtime-class-name" || true
sleep 2

# 检查 COS 环境变量（需要预先设置）
# 使用方法：
#   export COS_BUCKET_URL="https://your-bucket.cos.ap-region.myqcloud.com"
#   export COS_SECRET_ID="your_secret_id"
#   export COS_SECRET_KEY="your_secret_key"

if [ -z "$COS_BUCKET_URL" ]; then
    echo "❌ Error: COS_BUCKET_URL not set!"
    echo "   Please run: export COS_BUCKET_URL=https://your-bucket.cos.ap-region.myqcloud.com"
    exit 1
fi

if [ -z "$COS_SECRET_ID" ]; then
    echo "❌ Error: COS_SECRET_ID not set!"
    echo "   Please run: export COS_SECRET_ID=your_secret_id"
    exit 1
fi

if [ -z "$COS_SECRET_KEY" ]; then
    echo "❌ Error: COS_SECRET_KEY not set!"
    echo "   Please run: export COS_SECRET_KEY=your_secret_key"
    exit 1
fi

# 设置 Grafana/Prometheus 环境变量
export RAY_GRAFANA_IFRAME_HOST="http://118.25.169.144:5001"
export RAY_GRAFANA_HOST="http://118.25.169.144:5001"
export RAY_PROMETHEUS_HOST="http://118.25.169.144:5002"

# 配置参数
RAY_ROOT_DIR="${RAY_ROOT_DIR:-mce-proj-bk0lwhh1}"
PORT="${PORT:-8081}"

echo "✅ Environment Configuration:"
echo "   COS_BUCKET_URL=$COS_BUCKET_URL"
echo "   COS_SECRET_ID=$COS_SECRET_ID"
echo "   RAY_ROOT_DIR=$RAY_ROOT_DIR"
echo "   PORT=$PORT"
echo ""

# 进入项目目录
cd "$(dirname "$0")"

# 检查二进制文件
if [ ! -f "./output/bin/historyserver" ]; then
    echo "🔨 Building historyserver..."
    make historyserver
    echo ""
fi

# 启动（不需要 --runtime-class-config-path）
LOG_FILE="/tmp/historyserver.log"
DASHBOARD_DIR="$(pwd)/historyserver/dashboard"

echo "🚀 Starting historyserver..."
echo "   📋 Logs: $LOG_FILE"
echo "   🎨 Dashboard: $DASHBOARD_DIR"
echo ""

./output/bin/historyserver \
  --runtime-class-name=cos \
  --ray-root-dir="$RAY_ROOT_DIR" \
  --dashboard-dir="$DASHBOARD_DIR" \
  --port="$PORT" \
  > "$LOG_FILE" 2>&1 &

PID=$!
echo "✅ HistoryServer started!"
echo "   PID: $PID"
echo "   Port: $PORT"
echo "   URL: http://localhost:$PORT"
echo ""

# 等待启动
echo "⏳ Waiting for startup (8 seconds)..."
sleep 8

# 检查启动状态
if ps -p $PID > /dev/null 2>&1; then
    echo "✅ Process is running"
    echo ""
    
    # 显示初始日志
    echo "=========================================="
    echo "Startup Logs:"
    echo "=========================================="
    grep -E "(Found.*clusters|FallbackToWorkerLogs|Successfully reconstructed)" "$LOG_FILE" | head -20 || echo "(Waiting for cluster discovery...)"
    echo ""
    
    # 测试 API
    echo "=========================================="
    echo "Testing API:"
    echo "=========================================="
    CLUSTERS=$(curl -s http://localhost:$PORT/clusters/ | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data), 'clusters found')" 2>/dev/null || echo "API not ready yet")
    echo "Clusters: $CLUSTERS"
    echo ""
    
    echo "=========================================="
    echo "✅ HistoryServer is ready!"
    echo "=========================================="
    echo ""
    echo "Commands:"
    echo "  📋 View logs:       tail -f $LOG_FILE"
    echo "  🔍 Check clusters:  curl http://localhost:$PORT/clusters/ | jq"
    echo "  🛑 Stop server:     kill $PID"
    echo ""
else
    echo "❌ Failed to start! Check logs:"
    tail -30 "$LOG_FILE"
    exit 1
fi
