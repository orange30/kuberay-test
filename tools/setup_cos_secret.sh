#!/bin/bash

# 为指定命名空间创建 COS 临时密钥 Secret
# 使用方法: ./setup_cos_secret.sh <namespace> <secret-name> <role-arn>

set -e

NAMESPACE="${1}"
SECRET_NAME="${2}"
ROLE_ARN="${3}"

if [ -z "$NAMESPACE" ] || [ -z "$SECRET_NAME" ] || [ -z "$ROLE_ARN" ]; then
    echo "使用方法: $0 <namespace> <secret-name> <role-arn>"
    echo ""
    echo "示例:"
    echo "  $0 mce-proj-production-004 cos-secret-proj-production-004 qcs::cam::uin/your-uin:roleName/your-role"
    exit 1
fi

echo "========================================"
echo "  创建 COS 临时密钥 Secret"
echo "========================================"
echo ""
echo "Namespace: $NAMESPACE"
echo "Secret:    $SECRET_NAME"
echo "Role ARN:  $ROLE_ARN"
echo ""

# 1. 获取临时密钥
echo "[INFO] 正在获取临时密钥..."
TEMP_CREDS=$(tccli sts AssumeRole \
    --region ap-guangzhou \
    --RoleArn "$ROLE_ARN" \
    --RoleSessionName "historyserver-$(date +%s)" \
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

# 3. 删除旧 Secret（如果存在）
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &>/dev/null; then
    echo "[INFO] 删除旧 Secret..."
    kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
fi

# 4. 创建新 Secret
echo "[INFO] 创建新 Secret..."
kubectl create secret generic "$SECRET_NAME" \
    --namespace="$NAMESPACE" \
    --from-literal=secret-id="$TMP_SECRET_ID" \
    --from-literal=secret-key="$TMP_SECRET_KEY" \
    --from-literal=session-token="$SESSION_TOKEN"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Secret 创建成功！"
    echo ""
    echo "下一步："
    echo "  1. 重启 HistoryServer (如果有):"
    echo "     kubectl rollout restart deployment/historyserver-demo -n $NAMESPACE"
    echo ""
    echo "  2. 或重启 RayCluster (如果有):"
    echo "     kubectl delete raycluster <name> -n $NAMESPACE"
    echo "     kubectl apply -f <yaml-file>"
    echo ""
    echo "⏰ 注意: 临时密钥将在 2 小时后过期 ($EXPIRATION)"
else
    echo ""
    echo "❌ Secret 创建失败"
    exit 1
fi
