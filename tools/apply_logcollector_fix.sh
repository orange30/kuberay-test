#!/bin/bash
# 快速应用 LogCollector 修复

set -e

echo "=== LogCollector 修复脚本 ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "1. 检查当前 LogCollector 代码..."
COLLECTOR_FILE="historyserver/pkg/collector/logcollector/runtime/logcollector/collector.go"

if ! grep -q "processExportEvents" "$COLLECTOR_FILE"; then
    echo "   ⚠️  processExportEvents 函数不存在，需要先添加"
    echo "   请查看: docs/fix_logcollector_issue.md"
    exit 1
fi

if ! grep -q "Periodic upload triggered" "$COLLECTOR_FILE"; then
    echo "   ⚠️  定时上传机制未添加，正在添加..."
    
    # 创建临时补丁文件
    cat > /tmp/logcollector_periodic_upload.patch << 'EOF'
--- a/historyserver/pkg/collector/logcollector/runtime/logcollector/collector.go
+++ b/historyserver/pkg/collector/logcollector/runtime/logcollector/collector.go
@@ -69,7 +69,14 @@ func (r *RayLogHandler) Run(stop <-chan struct{}) error {
 		go r.WatchSessionLatestLoops() // Watch session_latest symlink changes
 	}
 
+	// 添加定时上传 ticker
+	uploadTicker := time.NewTicker(5 * time.Minute)
+	defer uploadTicker.Stop()
+
+	for {
 	select {
 	case <-sigChan:
 		logrus.Info("Received SIGTERM, processing all logs...")
@@ -79,6 +86,11 @@ func (r *RayLogHandler) Run(stop <-chan struct{}) error {
 		logrus.Info("Received stop signal, processing all logs...")
 		r.processSessionLatestLogs()
 		close(r.ShutdownChan)
+		return nil
+	case <-uploadTicker.C:
+		// 定期上传
+		logrus.Info("Periodic upload triggered")
+		r.processSessionLatestLogs()
+	}
 	}
-	logrus.Warnf("Receive stop single, so stop ray collector ")
-	return nil
 }
EOF
    
    # 应用补丁
    if patch -p1 --dry-run < /tmp/logcollector_periodic_upload.patch > /dev/null 2>&1; then
        patch -p1 < /tmp/logcollector_periodic_upload.patch
        echo "   ✅ 定时上传机制已添加"
    else
        echo "   ⚠️  补丁应用失败，请手动修改"
        echo "   参考: docs/fix_logcollector_issue.md 中的方案 1"
        exit 1
    fi
else
    echo "   ✅ 定时上传机制已存在"
fi

echo ""
echo "2. 构建 LogCollector..."
if command -v make &> /dev/null; then
    make build-logcollector || echo "   ⚠️  make 命令失败，尝试手动构建"
fi

echo ""
echo "3. 构建 Docker 镜像..."
echo "   提示: 请先设置你的镜像仓库地址"
read -p "   输入镜像仓库地址 (例如: ccr.ccs.tencentyun.com/your-namespace): " REGISTRY

if [ -z "$REGISTRY" ]; then
    echo "   ⚠️  未提供镜像仓库地址，跳过镜像构建"
    echo ""
    echo "   你可以手动构建:"
    echo "   docker build -t <your-registry>/ray-logcollector:v2 -f historyserver/Dockerfile.logcollector ."
    echo "   docker push <your-registry>/ray-logcollector:v2"
else
    IMAGE="${REGISTRY}/ray-logcollector:v2"
    echo "   正在构建镜像: $IMAGE"
    
    docker build -t "$IMAGE" -f historyserver/Dockerfile.logcollector .
    
    echo ""
    read -p "   是否推送镜像到仓库? (y/n): " PUSH
    if [ "$PUSH" = "y" ]; then
        docker push "$IMAGE"
        echo "   ✅ 镜像已推送"
        
        echo ""
        echo "4. 更新 RayCluster 配置..."
        echo "   请在 config/raycluster.yaml 中更新 logcollector 镜像:"
        echo "   image: $IMAGE"
    fi
fi

echo ""
echo "=== 修复应用完成 ==="
echo ""
echo "下一步:"
echo "1. 更新 RayCluster 配置（如果尚未更新）"
echo "2. 删除旧集群: kubectl delete rayjob <job-name> -n mce-proj-bk0lwhh1"
echo "3. 部署新集群: kubectl apply -f config/raycluster.yaml"
echo "4. 验证日志上传: kubectl logs -f <head-pod> -c logcollector -n mce-proj-bk0lwhh1"
echo "5. 检查 COS: cd tools && ./check_cos_structure.sh"
echo "6. 测试 API: ./tools/debug_cluster_key.sh"
echo ""
echo "详细文档: docs/fix_logcollector_issue.md"
