#!/bin/bash
# 诊断 HistoryServer ClusterKey 问题

echo "=== HistoryServer ClusterKey 诊断工具 ==="
echo ""

# 1. 检查 cookies.txt 内容
echo "1. 检查 cookies.txt 中的集群信息:"
if [ -f ~/cookies.txt ]; then
    echo "   cluster_name: $(grep -o 'cluster_name\s*[^\s]*' ~/cookies.txt | awk '{print $2}')"
    echo "   cluster_namespace: $(grep -o 'cluster_namespace\s*[^\s]*' ~/cookies.txt | awk '{print $2}')"
    echo "   session_name: $(grep -o 'session_name\s*[^\s]*' ~/cookies.txt | awk '{print $2}')"
else
    echo "   ⚠️  ~/cookies.txt 不存在，需要先创建 Cookie"
    echo "   提示: 访问 http://localhost:8081 并选择集群后会自动创建"
fi
echo ""

# 2. 测试 /clusters API 获取可用集群列表
echo "2. 查询 HistoryServer 发现的集群列表:"
CLUSTERS=$(curl -s http://localhost:8081/clusters)
if [ $? -ne 0 ]; then
    echo "   ⚠️  无法连接到 HistoryServer (http://localhost:8081)"
    echo "   请确认 HistoryServer 是否正在运行"
    exit 1
fi

# 检查响应格式并解析
if echo "$CLUSTERS" | jq -e '.[0]' > /dev/null 2>&1; then
    # 如果是数组格式（HistoryServer 直接返回数组）
    echo "$CLUSTERS" | jq -r '.[] | "   - \(.name)_\(.namespace) / Session: \(.sessionName)"'
    CLUSTER_COUNT=$(echo "$CLUSTERS" | jq -r '. | length')
    echo "   总计: $CLUSTER_COUNT 个集群"
else
    echo "   ⚠️  无法解析集群列表，原始响应:"
    echo "$CLUSTERS" | jq . || echo "$CLUSTERS"
fi
echo ""

# 3. 测试 /debug/clusters API（如果实现了）
echo "3. 查询 ClusterTaskMap 中的集群键:"
DEBUG_RESPONSE=$(curl -s http://localhost:8081/debug/clusters 2>/dev/null)
if [ $? -eq 0 ] && echo "$DEBUG_RESPONSE" | jq -e '.result' > /dev/null 2>&1; then
    echo "$DEBUG_RESPONSE" | jq -r '.data[] | "   - ClusterKey: \(.cluster_key)"'
    echo "$DEBUG_RESPONSE" | jq -r '.data[] | "     TaskCount: \(.task_count), JobCount: \(.job_count), ActorCount: \(.actor_count)"'
else
    echo "   ⚠️  /debug/clusters 接口未实现（这是正常的，需要先应用 debug patch）"
fi
echo ""

# 4. 构建预期的 ClusterKey
echo "4. 根据用户提供的信息构建预期的 ClusterKey:"
EXPECTED_CLUSTER_NAME="rayjob-job-ve4ouffo-bbwrr"
EXPECTED_NAMESPACE="mce-proj-bk0lwhh1"
EXPECTED_SESSION="session_2026-01-31_03-41-12_526238_1"
EXPECTED_KEY="${EXPECTED_CLUSTER_NAME}_${EXPECTED_NAMESPACE}_${EXPECTED_SESSION}"

echo "   集群名:      $EXPECTED_CLUSTER_NAME"
echo "   命名空间:    $EXPECTED_NAMESPACE"
echo "   会话:        $EXPECTED_SESSION"
echo "   完整键:      $EXPECTED_KEY"
echo ""

# 5. 测试 Tasks API（不使用 Cookie，直接测试）
echo "5. 测试 /api/v0/tasks API（不带 Cookie）:"
TASKS_RESPONSE=$(curl -s "http://localhost:8081/api/v0/tasks?limit=10" 2>/dev/null)
if [ $? -eq 0 ]; then
    TASK_COUNT=$(echo "$TASKS_RESPONSE" | jq -r '.data.result.total // 0' 2>/dev/null)
    echo "   返回的任务数量: ${TASK_COUNT:-0}"
    if [ "${TASK_COUNT:-0}" -eq 0 ]; then
        echo "   ⚠️  Tasks API 返回空数据"
    else
        echo "   ✅ Tasks API 返回了数据"
    fi
else
    echo "   ⚠️  无法调用 Tasks API"
fi
echo ""

# 6. 创建正确的 Cookie 并测试
echo "6. 创建正确的 Cookie 并测试:"
COOKIE_FILE="/tmp/historyserver_test_cookies.txt"
cat > "$COOKIE_FILE" << EOF
# Netscape HTTP Cookie File
localhost	FALSE	/	FALSE	0	cluster_name	${EXPECTED_CLUSTER_NAME}
localhost	FALSE	/	FALSE	0	cluster_namespace	${EXPECTED_NAMESPACE}
localhost	FALSE	/	FALSE	0	session_name	${EXPECTED_SESSION}
EOF
echo "   已创建测试 Cookie 文件: $COOKIE_FILE"
echo ""

TASKS_WITH_COOKIE=$(curl -s -b "$COOKIE_FILE" "http://localhost:8081/api/v0/tasks?limit=10" 2>/dev/null)
if [ $? -eq 0 ]; then
    TASK_COUNT_WITH_COOKIE=$(echo "$TASKS_WITH_COOKIE" | jq -r '.data.result.total // 0' 2>/dev/null)
    echo "   使用正确 Cookie 后的任务数量: ${TASK_COUNT_WITH_COOKIE:-0}"
    if [ "${TASK_COUNT_WITH_COOKIE:-0}" -gt 0 ]; then
        echo "   ✅✅✅ 成功！使用正确的 Cookie 后可以获取到数据！"
        echo "   解决方案: 在浏览器中选择正确的集群和会话"
    else
        echo "   ⚠️  即使使用正确的 Cookie 也返回空数据"
        echo "   这说明问题不是 Cookie，而是事件数据本身"
    fi
else
    echo "   ⚠️  无法调用 Tasks API"
fi
echo ""

# 7. 建议
echo "=== 诊断结果和建议 ==="
echo ""

if [ "${TASK_COUNT_WITH_COOKIE:-0}" -gt 0 ]; then
    echo "✅ 问题已确认: Cookie 设置不正确"
    echo ""
    echo "解决方案:"
    echo "1. 在浏览器中访问: http://localhost:8081"
    echo "2. 从集群列表中选择:"
    echo "   - 集群: rayjob-job-ve4ouffo-bbwrr"
    echo "   - 命名空间: mce-proj-bk0lwhh1"
    echo "   - 会话: session_2026-01-31_03-41-12_526238_1"
    echo "3. 或者将 $COOKIE_FILE 复制到 ~/cookies.txt"
    echo ""
    echo "临时使用方法:"
    echo "   cp $COOKIE_FILE ~/cookies.txt"
elif [ "$CLUSTER_COUNT" -eq 0 ]; then
    echo "⚠️  问题已确认: HistoryServer 没有发现任何集群"
    echo ""
    echo "可能的原因:"
    echo "1. metadir 文件格式不正确"
    echo "2. COS 配置错误"
    echo "3. HistoryServer 启动时没有正确扫描 metadir"
    echo ""
    echo "下一步操作:"
    echo "1. 检查 HistoryServer 启动日志"
    echo "2. 使用 cos_inspect 工具验证 COS 结构"
    echo "3. 确认 --ray-root-dir 参数正确"
else
    echo "⚠️  问题已确认: 集群被发现但没有 Task 数据"
    echo ""
    echo "可能的原因:"
    echo "1. job_events 目录为空"
    echo "2. EventHandler 加载事件时出错"
    echo "3. event_JOBS.log 为空"
    echo ""
    echo "下一步操作:"
    echo "1. 应用 debug patch 查看 EventHandler 日志"
    echo "2. 使用 cos_inspect 检查 job_events 目录"
    echo "3. 检查 LogCollector 是否正常工作"
fi
echo ""
echo "需要更多帮助? 查看: docs/tasks_api_issue_summary.md"
