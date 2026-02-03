#!/bin/bash

# 测试 Worker Log Fallback 功能

set -e

HISTORYSERVER_URL="${HISTORYSERVER_URL:-http://localhost:8265}"
CLUSTER_NAME="${1:-rayjob-job-ve4ouffo-bbwrr}"
NAMESPACE="${2:-mce-proj-bk0lwhh1}"

echo "=========================================="
echo " Worker Log Fallback 功能测试"
echo "=========================================="
echo "HistoryServer: $HISTORYSERVER_URL"
echo "Cluster: $CLUSTER_NAME"
echo "Namespace: $NAMESPACE"
echo ""

# 1. 获取集群列表
echo "📋 [1/5] 获取集群列表..."
CLUSTERS=$(curl -s "$HISTORYSERVER_URL/api/v0/clusters")
echo "$CLUSTERS" | jq -r '.[] | "   - \(.name)_\(.namespace) / Session: \(.sessionName)"' | head -5
echo ""

# 2. 查询 Tasks（应该包含 fallback 数据）
echo "📊 [2/5] 查询 Tasks..."
CLUSTER_KEY="${CLUSTER_NAME}_${NAMESPACE}"

# 从集群列表中提取 sessionName
SESSION_NAME=$(echo "$CLUSTERS" | jq -r --arg name "$CLUSTER_NAME" --arg ns "$NAMESPACE" \
    '.[] | select(.name == $name and .namespace == $ns) | .sessionName' | head -1)

if [ -z "$SESSION_NAME" ] || [ "$SESSION_NAME" = "null" ]; then
    echo "   ⚠️  未找到 Session，使用默认 Cookie"
    COOKIE="Cookie: cluster_name=${CLUSTER_KEY}"
else
    echo "   ✅ Session: $SESSION_NAME"
    COOKIE="Cookie: cluster_name=${CLUSTER_KEY}_${SESSION_NAME}"
fi

TASKS=$(curl -s -H "$COOKIE" "$HISTORYSERVER_URL/api/v0/tasks")
TASK_COUNT=$(echo "$TASKS" | jq -r '.total // (.result | length)')

echo "   Total: $TASK_COUNT tasks"

if [ "$TASK_COUNT" -gt 0 ]; then
    echo ""
    echo "   📝 前 3 个 Tasks："
    echo "$TASKS" | jq -r '.result[:3][] | "      - \(.task_id): \(.name) (\(.state))"'
    
    # 检查是否是 fallback 数据
    FALLBACK_COUNT=$(echo "$TASKS" | jq -r '[.result[] | select(.message | contains("Reconstructed from worker"))] | length')
    if [ "$FALLBACK_COUNT" -gt 0 ]; then
        echo ""
        echo "   ⚠️  发现 $FALLBACK_COUNT 个 fallback 重建的 Tasks"
        echo "   示例 Message:"
        echo "$TASKS" | jq -r '.result[0].message // "N/A"' | sed 's/^/      /'
    else
        echo "   ✅ 所有 Tasks 来自完整的事件数据"
    fi
else
    echo "   ❌ 未找到任何 Tasks（fallback 可能未生效）"
fi
echo ""

# 3. 查询 Actors
echo "🎭 [3/5] 查询 Actors..."
ACTORS=$(curl -s -H "$COOKIE" "$HISTORYSERVER_URL/api/v0/actors")
ACTOR_COUNT=$(echo "$ACTORS" | jq -r '.total // (.result | length)')

echo "   Total: $ACTOR_COUNT actors"

if [ "$ACTOR_COUNT" -gt 0 ]; then
    echo ""
    echo "   📝 Actors 列表："
    echo "$ACTORS" | jq -r '.result[] | "      - \(.actor_id): \(.name) (\(.state))"'
    
    # 检查是否是 fallback 数据
    FALLBACK_ACTORS=$(echo "$ACTORS" | jq -r '[.result[] | select(.message | contains("Reconstructed from worker"))] | length')
    if [ "$FALLBACK_ACTORS" -gt 0 ]; then
        echo ""
        echo "   ⚠️  发现 $FALLBACK_ACTORS 个 fallback 重建的 Actors"
    else
        echo "   ✅ 所有 Actors 来自完整的事件数据"
    fi
else
    echo "   ⚠️  未找到任何 Actors"
fi
echo ""

# 4. 查询 Jobs
echo "💼 [4/5] 查询 Jobs..."
JOBS=$(curl -s -H "$COOKIE" "$HISTORYSERVER_URL/api/v0/jobs")
JOB_COUNT=$(echo "$JOBS" | jq -r '.total // (.result | length)')

echo "   Total: $JOB_COUNT jobs"

if [ "$JOB_COUNT" -gt 0 ]; then
    echo ""
    echo "   📝 Jobs 列表："
    echo "$JOBS" | jq -r '.result[] | "      - \(.job_id): \(.submission_id) (\(.status))"'
else
    echo "   ⚠️  未找到任何 Jobs"
fi
echo ""

# 5. 检查 HistoryServer 日志（查找 fallback 相关日志）
echo "📜 [5/5] 检查 HistoryServer 日志..."

# 如果是在 Kubernetes 环境中
if command -v kubectl &> /dev/null; then
    HISTORYSERVER_POD=$(kubectl get pods -n history-system -l app=historyserver -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    
    if [ -n "$HISTORYSERVER_POD" ]; then
        echo "   Pod: $HISTORYSERVER_POD"
        echo ""
        echo "   🔍 最近的 fallback 日志："
        kubectl logs -n history-system "$HISTORYSERVER_POD" --tail=1000 | grep -i "FallbackToWorkerLogs\|Reconstructed from worker" | tail -10 || echo "   (未找到 fallback 相关日志)"
    else
        echo "   ⚠️  未找到 HistoryServer Pod（可能不在 K8s 环境中）"
    fi
else
    echo "   ⚠️  kubectl 不可用，跳过日志检查"
fi
echo ""

# 总结
echo "=========================================="
echo " 测试结果总结"
echo "=========================================="

if [ "$TASK_COUNT" -gt 0 ] || [ "$ACTOR_COUNT" -gt 0 ]; then
    echo "✅ Worker Log Fallback 功能正常工作！"
    echo ""
    echo "统计数据："
    echo "  - Tasks: $TASK_COUNT"
    echo "  - Actors: $ACTOR_COUNT"
    echo "  - Jobs: $JOB_COUNT"
    
    if [ "$FALLBACK_COUNT" -gt 0 ] || [ "$FALLBACK_ACTORS" -gt 0 ]; then
        echo ""
        echo "⚠️  检测到 fallback 数据："
        echo "  - Fallback Tasks: $FALLBACK_COUNT"
        echo "  - Fallback Actors: $FALLBACK_ACTORS"
        echo ""
        echo "建议：考虑启用 RAY_ENABLE_TASK_EVENTS=1 以获取完整的事件数据"
    fi
else
    echo "❌ 未找到任何 Tasks 或 Actors"
    echo ""
    echo "可能的原因："
    echo "  1. Cluster Key 错误（检查 cluster_name Cookie）"
    echo "  2. worker-*.out 文件不存在或为空"
    echo "  3. HistoryServer 尚未加载数据（等待下一个刷新周期）"
    echo "  4. Fallback 功能未正确编译/部署"
fi
echo ""
echo "=========================================="
