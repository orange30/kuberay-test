#!/bin/bash
# 更新 RayCluster 的临时 COS 凭证
# 用法: ./update-raycluster-temp-credentials.sh [namespace] [secret-name] [role-arn]

set -e

# ==================== 配置 ====================
NAMESPACE="${1:-mce-proj-production-009}"
SECRET_NAME="${2:-cos-secret-proj-production-009}"
ROLE_ARN="${3:-qcs::cam::uin/100123456:roleName/COSAccessRole}"

# 腾讯云地域
REGION="${REGION:-ap-guangzhou}"

# 临时密钥有效期（秒），默认 2 小时
DURATION_SECONDS="${DURATION_SECONDS:-7200}"

# Policy（可选，限制权限）
POLICY="${POLICY:-}"

# ==================== 颜色输出 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# ==================== 检查依赖 ====================
check_dependencies() {
    info "检查依赖工具..."
    
    if ! command -v kubectl &> /dev/null; then
        error "kubectl 未安装"
        exit 1
    fi
    
    if ! command -v tccli &> /dev/null; then
        error "tccli (腾讯云 CLI) 未安装"
        echo "安装方法: pip install tccli"
        exit 1
    fi
    
    if ! command -v jq &> /dev/null; then
        error "jq 未安装"
        echo "安装方法:"
        echo "  macOS: brew install jq"
        echo "  Linux: sudo apt-get install jq 或 sudo yum install jq"
        exit 1
    fi
    
    success "依赖检查通过"
}

# ==================== 获取临时密钥 ====================
get_temp_credentials() {
    info "从腾讯云 STS 获取临时密钥..."
    info "Role ARN: $ROLE_ARN"
    info "有效期: ${DURATION_SECONDS} 秒 ($(($DURATION_SECONDS / 3600)) 小时)"
    
    # 构建 tccli 命令
    CMD="tccli sts AssumeRole --region $REGION --RoleArn '$ROLE_ARN' --RoleSessionName 'RayCluster-$(date +%s)' --DurationSeconds $DURATION_SECONDS"
    
    # 如果提供了 Policy，添加到命令中
    if [ -n "$POLICY" ]; then
        CMD="$CMD --Policy '$POLICY'"
    fi
    
    info "执行命令: $CMD"
    
    # 执行命令并捕获输出
    RESPONSE=$(eval $CMD 2>&1)
    
    if [ $? -ne 0 ]; then
        error "获取临时密钥失败"
        echo "$RESPONSE"
        exit 1
    fi
    
    # 解析响应
    TMP_SECRET_ID=$(echo "$RESPONSE" | jq -r '.Credentials.TmpSecretId')
    TMP_SECRET_KEY=$(echo "$RESPONSE" | jq -r '.Credentials.TmpSecretKey')
    TMP_SESSION_TOKEN=$(echo "$RESPONSE" | jq -r '.Credentials.Token')
    EXPIRATION=$(echo "$RESPONSE" | jq -r '.Expiration')
    
    # 验证结果
    if [ "$TMP_SECRET_ID" == "null" ] || [ "$TMP_SECRET_KEY" == "null" ] || [ "$TMP_SESSION_TOKEN" == "null" ]; then
        error "解析临时密钥失败"
        echo "响应: $RESPONSE"
        exit 1
    fi
    
    success "成功获取临时密钥"
    info "SecretId: ${TMP_SECRET_ID:0:20}..."
    info "SecretKey: ${TMP_SECRET_KEY:0:20}..."
    info "Token 长度: ${#TMP_SESSION_TOKEN} 字符"
    info "过期时间: $EXPIRATION"
}

# ==================== 更新 Kubernetes Secret ====================
update_k8s_secret() {
    info "更新 Kubernetes Secret..."
    info "Namespace: $NAMESPACE"
    info "Secret Name: $SECRET_NAME"
    
    # 检查 Secret 是否存在
    if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
        info "Secret 已存在，将更新..."
        
        # 删除旧 Secret
        kubectl delete secret "$SECRET_NAME" -n "$NAMESPACE"
        success "已删除旧 Secret"
    else
        info "Secret 不存在，将创建新 Secret..."
    fi
    
    # 创建新 Secret
    kubectl create secret generic "$SECRET_NAME" \
        --namespace="$NAMESPACE" \
        --from-literal=secret-id="$TMP_SECRET_ID" \
        --from-literal=secret-key="$TMP_SECRET_KEY" \
        --from-literal=session-token="$TMP_SESSION_TOKEN"
    
    if [ $? -eq 0 ]; then
        success "Secret 更新成功"
    else
        error "Secret 更新失败"
        exit 1
    fi
}

# ==================== 重启受影响的 Pod ====================
restart_pods() {
    info "查找使用该 Secret 的 Pod..."
    
    # 查找 RayCluster 的 Pod
    RAYCLUSTER_PODS=$(kubectl get pods -n "$NAMESPACE" -l ray.io/cluster -o name 2>/dev/null)
    
    if [ -z "$RAYCLUSTER_PODS" ]; then
        warn "未找到 RayCluster Pod"
        return
    fi
    
    info "找到以下 Pod："
    echo "$RAYCLUSTER_PODS"
    
    # 询问是否重启
    read -p "$(echo -e ${YELLOW}是否重启这些 Pod 以应用新凭证？[y/N]:${NC} )" -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        info "重启 Pod..."
        
        for POD in $RAYCLUSTER_PODS; do
            info "删除 Pod: $POD"
            kubectl delete "$POD" -n "$NAMESPACE"
        done
        
        success "Pod 删除完成，K8s 将自动重建"
        info "等待 Pod 重新启动..."
        sleep 5
        
        kubectl get pods -n "$NAMESPACE" -l ray.io/cluster
    else
        warn "跳过 Pod 重启"
        warn "注意：需要重启 Pod 才能使用新的临时密钥"
    fi
}

# ==================== 显示摘要 ====================
show_summary() {
    echo ""
    echo "========================================"
    echo "           临时密钥更新摘要"
    echo "========================================"
    echo -e "${GREEN}✓${NC} 临时密钥获取成功"
    echo -e "${GREEN}✓${NC} Kubernetes Secret 更新成功"
    echo ""
    echo "配置信息:"
    echo "  Namespace: $NAMESPACE"
    echo "  Secret: $SECRET_NAME"
    echo "  过期时间: $EXPIRATION"
    echo ""
    echo "下一步操作:"
    echo "  1. 如果还未重启 Pod，请手动重启："
    echo "     kubectl rollout restart deployment -n $NAMESPACE"
    echo ""
    echo "  2. 验证 Pod 环境变量："
    echo "     kubectl exec -it <pod-name> -n $NAMESPACE -- env | grep COS"
    echo ""
    echo "  3. 查看 Collector 日志："
    echo "     kubectl logs <pod-name> -n $NAMESPACE | grep -i cos"
    echo ""
    echo "  4. 设置定时任务自动刷新（建议每小时刷新）："
    echo "     crontab -e"
    echo "     0 * * * * $0 $NAMESPACE $SECRET_NAME $ROLE_ARN"
    echo ""
    echo "========================================"
}

# ==================== 主流程 ====================
main() {
    echo "========================================"
    echo "  RayCluster 临时 COS 凭证更新脚本"
    echo "========================================"
    echo ""
    
    check_dependencies
    get_temp_credentials
    update_k8s_secret
    restart_pods
    show_summary
    
    success "完成！"
}

# 执行主流程
main
