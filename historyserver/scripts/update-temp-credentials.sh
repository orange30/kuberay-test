#!/bin/bash
# 临时密钥更新脚本
# 用于获取临时密钥并更新 Kubernetes Secret

set -e

# ==================== 配置区 ====================
NAMESPACE="${NAMESPACE:-mce-proj-production-009}"
SECRET_NAME="${SECRET_NAME:-cos-secret-proj-production-009}"
ROLE_ARN="${ROLE_ARN:-}"
DEPLOYMENT_NAME="${DEPLOYMENT_NAME:-historyserver-demo}"
DURATION_SECONDS="${DURATION_SECONDS:-7200}"  # 2 小时

# ==================== 颜色输出 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ==================== 参数检查 ====================
if [ -z "$ROLE_ARN" ]; then
    log_error "ROLE_ARN 环境变量未设置！"
    echo "使用方法："
    echo "  export ROLE_ARN=\"qcs::cam::uin/你的账号ID:roleName/你的角色名\""
    echo "  bash update-temp-credentials.sh"
    exit 1
fi

# 检查必要工具
for cmd in tccli kubectl jq base64; do
    if ! command -v $cmd &> /dev/null; then
        log_error "$cmd 未安装，请先安装"
        exit 1
    fi
done

# ==================== 获取临时密钥 ====================
log_info "正在获取临时密钥..."
log_info "RoleArn: $ROLE_ARN"
log_info "Duration: ${DURATION_SECONDS}s ($(($DURATION_SECONDS / 60)) 分钟)"

SESSION_NAME="historyserver-$(date +%Y%m%d%H%M%S)"

RESPONSE=$(tccli sts AssumeRole \
    --RoleArn "$ROLE_ARN" \
    --RoleSessionName "$SESSION_NAME" \
    --DurationSeconds $DURATION_SECONDS 2>&1)

if [ $? -ne 0 ]; then
    log_error "获取临时密钥失败！"
    echo "$RESPONSE"
    exit 1
fi

# 提取临时密钥
TMP_SECRET_ID=$(echo "$RESPONSE" | jq -r '.Response.Credentials.TmpSecretId')
TMP_SECRET_KEY=$(echo "$RESPONSE" | jq -r '.Response.Credentials.TmpSecretKey')
TMP_TOKEN=$(echo "$RESPONSE" | jq -r '.Response.Credentials.Token')
EXPIRATION=$(echo "$RESPONSE" | jq -r '.Response.Expiration')

if [ "$TMP_SECRET_ID" == "null" ] || [ -z "$TMP_SECRET_ID" ]; then
    log_error "无法从响应中提取临时密钥！"
    echo "响应内容："
    echo "$RESPONSE" | jq .
    exit 1
fi

log_info "✅ 临时密钥获取成功"
log_info "   - SecretID: ${TMP_SECRET_ID:0:20}..."
log_info "   - SecretKey: ${TMP_SECRET_KEY:0:10}...（已隐藏）"
log_info "   - Token 长度: ${#TMP_TOKEN} 字符"
log_info "   - 过期时间: $EXPIRATION"

# ==================== 更新 Secret ====================
log_info ""
log_info "正在更新 Secret..."
log_info "   - Namespace: $NAMESPACE"
log_info "   - Secret Name: $SECRET_NAME"

# 检查 Secret 是否存在
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
    log_info "Secret 已存在，将更新"
    ACTION="更新"
else
    log_info "Secret 不存在，将创建"
    ACTION="创建"
fi

# 创建或更新 Secret
kubectl create secret generic "$SECRET_NAME" \
    --from-literal=secret-id="$TMP_SECRET_ID" \
    --from-literal=secret-key="$TMP_SECRET_KEY" \
    --from-literal=session-token="$TMP_TOKEN" \
    -n "$NAMESPACE" \
    --dry-run=client -o yaml | kubectl apply -f -

if [ $? -eq 0 ]; then
    log_info "✅ Secret ${ACTION}成功"
else
    log_error "Secret ${ACTION}失败"
    exit 1
fi

# 验证 Secret
log_info ""
log_info "验证 Secret..."
SECRET_KEYS=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data}' | jq -r 'keys[]')
log_info "Secret 包含以下 keys:"
echo "$SECRET_KEYS" | while read key; do
    echo "   - $key"
done

# ==================== 重启 Pod ====================
log_info ""
read -p "是否重启 Deployment 以应用新的临时密钥？(y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "正在重启 Deployment: $DEPLOYMENT_NAME"
    kubectl rollout restart deployment "$DEPLOYMENT_NAME" -n "$NAMESPACE"
    
    log_info "等待 Deployment 就绪..."
    kubectl rollout status deployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" --timeout=120s
    
    if [ $? -eq 0 ]; then
        log_info "✅ Deployment 重启成功"
        
        # 获取新 Pod 名称
        POD_NAME=$(kubectl get pod -n "$NAMESPACE" -l app=historyserver -o jsonpath='{.items[0].metadata.name}')
        log_info ""
        log_info "查看新 Pod 日志："
        log_info "  kubectl logs -f -n $NAMESPACE $POD_NAME"
    else
        log_error "Deployment 重启超时"
    fi
else
    log_warn "已跳过重启，请手动重启 Deployment:"
    log_warn "  kubectl rollout restart deployment $DEPLOYMENT_NAME -n $NAMESPACE"
fi

# ==================== 输出总结 ====================
log_info ""
log_info "==================== 总结 ===================="
log_info "✅ 临时密钥已更新到 Secret: $SECRET_NAME"
log_info "📅 过期时间: $EXPIRATION"
log_info ""
log_warn "⚠️  注意："
log_warn "   - 临时密钥将在 $(($DURATION_SECONDS / 60)) 分钟后过期"
log_warn "   - 请在过期前再次运行此脚本刷新密钥"
log_warn "   - 建议设置 CronJob 自动刷新"
log_info ""
log_info "验证命令："
log_info "  # 查看集群列表"
log_info "  kubectl run -it --rm debug --image=alpine --restart=Never -n $NAMESPACE -- \\"
log_info "    sh -c 'apk add curl && curl -s http://historyserver-demo:8080/clusters/'"
log_info ""
log_info "监控日志："
log_info "  kubectl logs -f -n $NAMESPACE -l app=historyserver | grep -i 'cos\\|403\\|auth'"
log_info "=============================================="
