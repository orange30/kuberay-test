#!/bin/bash

# 为所有 production 命名空间批量创建 COS Secret
# 使用方法: ./setup_all_cos_secrets.sh <role-arn>

set -e

ROLE_ARN="${1}"

if [ -z "$ROLE_ARN" ]; then
    echo "使用方法: $0 <role-arn>"
    echo ""
    echo "示例:"
    echo "  $0 qcs::cam::uin/your-uin:roleName/your-role"
    exit 1
fi

NAMESPACES=(
    "mce-proj-production-001"
    "mce-proj-production-004"
    "mce-proj-production-009"
    "mce-proj-production-010"
)

echo "========================================"
echo "  批量创建 COS 临时密钥 Secret"
echo "========================================"
echo ""
echo "Role ARN: $ROLE_ARN"
echo "命名空间: ${NAMESPACES[@]}"
echo ""

# 1. 获取临时密钥（只获取一次，所有命名空间共享）
echo "[INFO] 正在获取临时密钥..."
TEMP_CREDS=$(tccli sts AssumeRole \
    --region ap-guangzhou \
    --RoleArn "$ROLE_ARN" \
    --RoleSessionName "historyserver-batch-$(date +%s)" \
    --DurationSeconds 7200 2>&1)

if [ $? -ne 0 ]; then
    echo "[ERROR] 获取临时密钥失败："
    echo "$TEMP_CREDS"
    exit 1
fi

# 2. 解析凭证
TMP_SECRET_ID=$(echo "$TEMP_CREDS" | jq -r '.Credentials.TmpSecretId')
TMP_SECRET_KEY=$(echo "$TEMP_CREDS" | jq -r '.Credentials.TmpSecretKey')
SESSION_TOKEN=$(echo "$TEMP_CREDS" | jq -r '.Credentials.Token')
EXPIRATION=$(echo "$TEMP_CREDS" | jq -r '.Credentials.Expiration')

if [ -z "$TMP_SECRET_ID" ] || [ "$TMP_SECRET_ID" = "null" ]; then
    echo "[ERROR] 解析临时密钥失败"
    echo "$TEMP_CREDS"
    exit 1
fi

echo "[INFO] 临时密钥获取成功"
echo "  - SecretID: ${TMP_SECRET_ID:0:20}..."
echo "  - 过期时间: $EXPIRATION"
echo ""

# 3. 为每个命名空间创建 Secret
SUCCESS_COUNT=0
FAIL_COUNT=0

for NS in "${NAMESPACES[@]}"; do
    echo "----------------------------------------"
    echo "[INFO] 处理命名空间: $NS"
    
    SECRET_NAME="cos-secret-${NS#mce-proj-}"
    
    # 删除旧 Secret（如果存在）
    if kubectl get secret "$SECRET_NAME" -n "$NS" &>/dev/null; then
        echo "  - 删除旧 Secret..."
        kubectl delete secret "$SECRET_NAME" -n "$NS"
    fi
    
    # 创建新 Secret
    echo "  - 创建 Secret: $SECRET_NAME"
    kubectl create secret generic "$SECRET_NAME" \
        --namespace="$NS" \
        --from-literal=secret-id="$TMP_SECRET_ID" \
        --from-literal=secret-key="$TMP_SECRET_KEY" \
        --from-literal=session-token="$SESSION_TOKEN"
    
    if [ $? -eq 0 ]; then
        echo "  ✅ $NS: 成功"
        ((SUCCESS_COUNT++))
        
        # 尝试重启 HistoryServer（如果存在）
        if kubectl get deployment historyserver-demo -n "$NS" &>/dev/null; then
            echo "  - 重启 HistoryServer..."
            kubectl rollout restart deployment/historyserver-demo -n "$NS" &>/dev/null
        fi
    else
        echo "  ❌ $NS: 失败"
        ((FAIL_COUNT++))
    fi
done

echo ""
echo "========================================"
echo "  批量创建完成"
echo "========================================"
echo ""
echo "总计: ${#NAMESPACES[@]} 个命名空间"
echo "成功: $SUCCESS_COUNT"
echo "失败: $FAIL_COUNT"
echo ""
echo "⏰ 注意: 临时密钥将在 2 小时后过期 ($EXPIRATION)"
echo ""
echo "建议设置定时任务自动刷新："
echo "  crontab -e"
echo "  0 * * * * /path/to/setup_all_cos_secrets.sh \"$ROLE_ARN\""
