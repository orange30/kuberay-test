#!/bin/bash
# 诊断 Worker Log Fallback 功能

set -e

echo "=========================================="
echo "Worker Log Fallback 诊断工具"
echo "=========================================="
echo ""

# 获取 historyserver pod 名称
HISTORYSERVER_POD=$(kubectl get pods -n kuberay -l app=kuberay-historyserver -o jsonpath='{.items[0].metadata.name}')
if [ -z "$HISTORYSERVER_POD" ]; then
    echo "❌ 找不到 historyserver pod"
    exit 1
fi
echo "✅ Historyserver Pod: $HISTORYSERVER_POD"
echo ""

# 检查最近的日志中是否有 FallbackToWorkerLogs 的消息
echo "=========================================="
echo "1. 检查 Fallback 是否被触发"
echo "=========================================="
kubectl logs -n kuberay "$HISTORYSERVER_POD" --tail=200 | grep -i "FallbackToWorkerLogs" || echo "⚠️  没有找到 FallbackToWorkerLogs 相关日志"
echo ""

# 检查是否有 worker-*.out 文件
echo "=========================================="
echo "2. 检查 COS 中是否有 worker-*.out 文件"
echo "=========================================="
kubectl logs -n kuberay "$HISTORYSERVER_POD" --tail=500 | grep -E "worker-.*\.out|Found worker log file" || echo "⚠️  没有找到 worker-*.out 文件的日志"
echo ""

# 检查 task 和 actor 数量
echo "=========================================="
echo "3. 检查 Tasks/Actors API 响应"
echo "=========================================="

# 获取 historyserver service
HISTORYSERVER_SVC=$(kubectl get svc -n kuberay -l app=kuberay-historyserver -o jsonpath='{.items[0].metadata.name}')
if [ -z "$HISTORYSERVER_SVC" ]; then
    echo "❌ 找不到 historyserver service"
    exit 1
fi

# Port forward (在后台运行)
echo "启动 port-forward..."
kubectl port-forward -n kuberay svc/"$HISTORYSERVER_SVC" 8265:8265 > /dev/null 2>&1 &
PORT_FORWARD_PID=$!
sleep 3

# 获取 cluster 列表
echo "获取 cluster 列表..."
CLUSTERS=$(curl -s http://localhost:8265/clusters 2>/dev/null || echo "{}")
echo "Clusters: $CLUSTERS"
echo ""

# 测试 tasks API
echo "测试 /api/v0/tasks API..."
TASKS_RESPONSE=$(curl -s "http://localhost:8265/api/v0/tasks?limit=10" 2>/dev/null || echo "{}")
echo "Tasks 响应: $TASKS_RESPONSE"
TASK_COUNT=$(echo "$TASKS_RESPONSE" | jq -r '.data.summary.numTasks // 0' 2>/dev/null || echo "0")
echo "✅ Task 数量: $TASK_COUNT"
echo ""

# 测试 actors API
echo "测试 /logical/actors API..."
ACTORS_RESPONSE=$(curl -s "http://localhost:8265/logical/actors" 2>/dev/null || echo "{}")
echo "Actors 响应: $ACTORS_RESPONSE"
ACTOR_COUNT=$(echo "$ACTORS_RESPONSE" | jq -r '.data.actors | length // 0' 2>/dev/null || echo "0")
echo "✅ Actor 数量: $ACTOR_COUNT"
echo ""

# 清理 port-forward
kill $PORT_FORWARD_PID 2>/dev/null || true

# 显示完整日志（最近 50 行，包含关键字）
echo "=========================================="
echo "4. 关键日志摘要"
echo "=========================================="
kubectl logs -n kuberay "$HISTORYSERVER_POD" --tail=100 | grep -E "FallbackToWorkerLogs|EnrichTasksFromLogs|worker-.*\.out|Successfully reconstructed|No tasks or actors found" || echo "⚠️  没有找到相关日志"
echo ""

echo "=========================================="
echo "诊断完成"
echo "=========================================="
echo ""
echo "📋 下一步建议："
echo "1. 如果没有看到 'FallbackToWorkerLogs' 日志，说明函数没有被调用"
echo "2. 如果看到 'No log directories found'，检查 COS 路径配置"
echo "3. 如果看到 'Found worker log file' 但没有数据，检查文件解析逻辑"
echo "4. 查看完整日志: kubectl logs -n kuberay $HISTORYSERVER_POD"
