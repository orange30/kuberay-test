#!/bin/bash
# COS 认证问题诊断脚本
# 用法: ./diagnose_cos_auth.sh [namespace] [secret-name] [pod-name]

set -e

NAMESPACE="${1:-mce-proj-production-010}"
SECRET_NAME="${2:-cos-secret-proj-production-010}"
POD_NAME="${3:-}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info() { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

echo "========================================"
echo "  COS 认证问题诊断"
echo "========================================"
echo ""
info "Namespace: $NAMESPACE"
info "Secret: $SECRET_NAME"
echo ""

# 1. 检查 Secret 是否存在
info "1. 检查 Secret 是否存在..."
if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" &> /dev/null; then
    success "Secret 存在"
    
    # 检查 Secret 创建时间
    CREATION_TIME=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.metadata.creationTimestamp}')
    info "   创建时间: $CREATION_TIME"
    
    # 检查 Secret 字段
    info "   检查字段..."
    if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.secret-id}' | base64 -d &> /dev/null; then
        SECRET_ID=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.secret-id}' | base64 -d)
        success "   ✓ secret-id: ${SECRET_ID:0:20}..."
    else
        error "   ✗ secret-id 不存在或无效"
    fi
    
    if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.secret-key}' | base64 -d &> /dev/null; then
        SECRET_KEY=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.secret-key}' | base64 -d)
        success "   ✓ secret-key: ${SECRET_KEY:0:20}..."
    else
        error "   ✗ secret-key 不存在或无效"
    fi
    
    if kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.session-token}' | base64 -d &> /dev/null; then
        SESSION_TOKEN=$(kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" -o jsonpath='{.data.session-token}' | base64 -d)
        if [ -n "$SESSION_TOKEN" ]; then
            success "   ✓ session-token: ${#SESSION_TOKEN} 字符 (临时密钥)"
            warn "   ⚠️  临时密钥可能已过期！有效期通常为 2 小时"
        else
            info "   ○ session-token 为空 (使用永久密钥)"
        fi
    else
        info "   ○ session-token 不存在 (使用永久密钥)"
    fi
else
    error "Secret 不存在！"
    exit 1
fi
echo ""

# 2. 查找使用该 Secret 的 Pod
info "2. 查找使用该 Secret 的 Pod..."
if [ -z "$POD_NAME" ]; then
    PODS=$(kubectl get pods -n "$NAMESPACE" -o json | jq -r ".items[] | select(.spec.containers[].env[]?.valueFrom.secretKeyRef.name == \"$SECRET_NAME\") | .metadata.name" 2>/dev/null)
    
    if [ -z "$PODS" ]; then
        warn "未找到使用该 Secret 的 Pod"
        info "尝试查找 historyserver Pod..."
        PODS=$(kubectl get pods -n "$NAMESPACE" -l app=historyserver -o name 2>/dev/null | head -1 | cut -d'/' -f2)
    fi
    
    if [ -n "$PODS" ]; then
        POD_NAME=$(echo "$PODS" | head -1)
        success "找到 Pod: $POD_NAME"
    else
        error "未找到任何 Pod"
        exit 1
    fi
else
    success "使用指定的 Pod: $POD_NAME"
fi
echo ""

# 3. 检查 Pod 环境变量
info "3. 检查 Pod 环境变量..."
if kubectl get pod "$POD_NAME" -n "$NAMESPACE" &> /dev/null; then
    info "   检查 COS 相关环境变量..."
    
    COS_BUCKET_URL=$(kubectl exec "$POD_NAME" -n "$NAMESPACE" -- env 2>/dev/null | grep "^COS_BUCKET_URL=" | cut -d'=' -f2)
    COS_SECRET_ID=$(kubectl exec "$POD_NAME" -n "$NAMESPACE" -- env 2>/dev/null | grep "^COS_SECRET_ID=" | cut -d'=' -f2)
    COS_SECRET_KEY=$(kubectl exec "$POD_NAME" -n "$NAMESPACE" -- env 2>/dev/null | grep "^COS_SECRET_KEY=" | cut -d'=' -f2)
    COS_SESSION_TOKEN=$(kubectl exec "$POD_NAME" -n "$NAMESPACE" -- env 2>/dev/null | grep "^COS_SESSION_TOKEN=" | cut -d'=' -f2)
    
    if [ -n "$COS_BUCKET_URL" ]; then
        success "   ✓ COS_BUCKET_URL: $COS_BUCKET_URL"
    else
        error "   ✗ COS_BUCKET_URL 未设置"
    fi
    
    if [ -n "$COS_SECRET_ID" ]; then
        success "   ✓ COS_SECRET_ID: ${COS_SECRET_ID:0:20}..."
    else
        error "   ✗ COS_SECRET_ID 未设置"
    fi
    
    if [ -n "$COS_SECRET_KEY" ]; then
        success "   ✓ COS_SECRET_KEY: ${COS_SECRET_KEY:0:20}..."
    else
        error "   ✗ COS_SECRET_KEY 未设置"
    fi
    
    if [ -n "$COS_SESSION_TOKEN" ]; then
        success "   ✓ COS_SESSION_TOKEN: ${#COS_SESSION_TOKEN} 字符 (临时密钥)"
        warn "   ⚠️  检查临时密钥是否过期（有效期 2 小时）"
    else
        info "   ○ COS_SESSION_TOKEN 未设置 (使用永久密钥)"
    fi
else
    error "Pod 不存在或无法访问"
    exit 1
fi
echo ""

# 4. 检查 Pod 日志中的错误
info "4. 检查最近的 COS 错误..."
ERRORS=$(kubectl logs "$POD_NAME" -n "$NAMESPACE" --tail=100 2>/dev/null | grep -i "InvalidAccessKeyId\|403\|Failed to get object\|Failed to list objects" | tail -5)

if [ -n "$ERRORS" ]; then
    error "发现 COS 认证错误："
    echo "$ERRORS" | while IFS= read -r line; do
        echo "   $line"
    done
    echo ""
    error "⚠️  确认：密钥认证失败！"
else
    success "未发现最近的 COS 错误"
fi
echo ""

# 5. 诊断结论
echo "========================================"
echo "  诊断结论"
echo "========================================"

if [ -n "$COS_SESSION_TOKEN" ] || [ -n "$SESSION_TOKEN" ]; then
    error "❌ 使用临时密钥，但出现 InvalidAccessKeyId 错误"
    echo ""
    echo "可能原因："
    echo "  1. 临时密钥已过期（有效期 2 小时）"
    echo "  2. Secret 中的密钥与实际使用的不一致"
    echo "  3. Pod 需要重启以加载新的 Secret"
    echo ""
    echo "解决方案："
    echo "  1. 更新临时密钥："
    echo "     bash historyserver/scripts/update-temp-credentials.sh $NAMESPACE $SECRET_NAME"
    echo ""
    echo "  2. 重启 Pod："
    echo "     kubectl rollout restart deployment historyserver-demo -n $NAMESPACE"
    echo ""
    echo "  3. 设置自动刷新（推荐）："
    echo "     crontab -e"
    echo "     0 * * * * /path/to/update-temp-credentials.sh $NAMESPACE $SECRET_NAME"
else
    warn "⚠️  使用永久密钥，但出现 InvalidAccessKeyId 错误"
    echo ""
    echo "可能原因："
    echo "  1. 永久密钥已被删除或禁用"
    echo "  2. Secret 中的密钥配置错误"
    echo "  3. 密钥权限不足"
    echo ""
    echo "解决方案："
    echo "  1. 验证永久密钥是否有效："
    echo "     登录腾讯云控制台 -> 访问管理 -> API 密钥管理"
    echo ""
    echo "  2. 更新 Secret："
    echo "     kubectl delete secret $SECRET_NAME -n $NAMESPACE"
    echo "     kubectl create secret generic $SECRET_NAME \\"
    echo "       --namespace=$NAMESPACE \\"
    echo "       --from-literal=secret-id='<正确的SecretID>' \\"
    echo "       --from-literal=secret-key='<正确的SecretKey>'"
    echo ""
    echo "  3. 或切换到临时密钥（推荐）："
    echo "     bash historyserver/scripts/update-temp-credentials.sh $NAMESPACE $SECRET_NAME"
fi

echo ""
echo "========================================"
