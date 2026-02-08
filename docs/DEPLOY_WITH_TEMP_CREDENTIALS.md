# HistoryServer 临时密钥部署指南

## 📋 修改说明

### 代码修改（已完成）✅

**修改文件：**
1. `historyserver/pkg/storage/cos/config.go` - 添加 `SessionToken` 字段支持
2. `historyserver/pkg/storage/cos/cos.go` - 传入 `SessionToken` 到 COS SDK

**核心变更：**
```go
// config 结构体新增字段
type config struct {
    BucketURL    string
    SecretID     string
    SecretKey    string
    SessionToken string  // 🆕 支持临时密钥
    types.RayCollectorConfig
}

// 从环境变量读取临时 Token
c.SessionToken = os.Getenv("COS_SESSION_TOKEN")

// 传入 COS SDK
client := cos.NewClient(b, &http.Client{
    Transport: &cos.AuthorizationTransport{
        SecretID:     c.SecretID,
        SecretKey:    c.SecretKey,
        SessionToken: c.SessionToken,  // 🆕 支持临时密钥
    },
})
```

---

## 🔧 Kubernetes YAML 修改

### 修改要点

在现有的 Pod/Deployment YAML 中，需要添加 **一个新的环境变量** `COS_SESSION_TOKEN`：

```yaml
spec:
  containers:
  - name: historyserver
    env:
    # 现有配置保持不变
    - name: COS_BUCKET_URL
      value: https://zhikuodu-1255429800.cos.ap-guangzhou.myqcloud.com
    - name: COS_SECRET_ID
      valueFrom:
        secretKeyRef:
          key: secret-id
          name: cos-secret-proj-production-009
    - name: COS_SECRET_KEY
      valueFrom:
        secretKeyRef:
          key: secret-key
          name: cos-secret-proj-production-009
    
    # 🆕 新增配置：临时密钥 Token
    - name: COS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          key: session-token              # 🆕 新增 key
          name: cos-secret-proj-production-009
          optional: true                  # 🆕 可选，兼容永久密钥
```

**关键点：**
- ✅ `optional: true` 确保兼容性：当 Secret 中没有 `session-token` 时不会报错
- ✅ 如果使用永久密钥，Secret 中可以不包含 `session-token` 字段
- ✅ 如果使用临时密钥，需要在 Secret 中添加 `session-token` 字段

---

## 📦 Secret 配置

### 方案 1: 使用永久密钥（现有方式，无需修改）

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret-proj-production-009
  namespace: mce-proj-production-009
type: Opaque
data:
  secret-id: <Base64编码的永久SecretID>
  secret-key: <Base64编码的永久SecretKey>
  # 注意：永久密钥不需要 session-token
```

**说明：**
- ✅ 现有 Secret 无需修改
- ✅ 代码兼容，`SessionToken` 为空时不影响永久密钥认证
- ⚠️ 安全性较低，仅建议内网测试环境使用

---

### 方案 2: 使用临时密钥（推荐）

#### 2.1 创建包含临时密钥的 Secret

```bash
# 设置临时密钥变量
export TMP_SECRET_ID="AKID***-临时"
export TMP_SECRET_KEY="***-临时"
export TMP_TOKEN="***-临时token"

# 创建 Secret
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="$TMP_SECRET_ID" \
  --from-literal=secret-key="$TMP_SECRET_KEY" \
  --from-literal=session-token="$TMP_TOKEN" \
  -n mce-proj-production-009
```

#### 2.2 更新现有 Secret

```bash
# 如果 Secret 已存在，使用以下命令更新
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="$TMP_SECRET_ID" \
  --from-literal=secret-key="$TMP_SECRET_KEY" \
  --from-literal=session-token="$TMP_TOKEN" \
  -n mce-proj-production-009 \
  --dry-run=client -o yaml | kubectl apply -f -
```

#### 2.3 验证 Secret 内容

```bash
# 查看 Secret 的 keys
kubectl get secret cos-secret-proj-production-009 -n mce-proj-production-009 -o jsonpath='{.data}' | jq 'keys'

# 预期输出:
# [
#   "secret-id",
#   "secret-key",
#   "session-token"  # 🆕 新增
# ]

# 解码查看 session-token（用于调试）
kubectl get secret cos-secret-proj-production-009 -n mce-proj-production-009 \
  -o jsonpath='{.data.session-token}' | base64 -d
```

---

## 🚀 部署步骤

### 步骤 1: 构建新的 HistoryServer 镜像

```bash
cd /Users/zhikuodu/work/work_ray/kuberay-test/historyserver

# 构建镜像
make docker-build

# 推送镜像（使用新的 tag）
docker tag historyserver:latest ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030
docker push ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030
```

---

### 步骤 2: 获取临时密钥

#### 2.1 使用腾讯云 CLI 获取

```bash
# 安装腾讯云 CLI（如果未安装）
pip install tccli

# 配置永久密钥（用于调用 STS API）
tccli configure set secretId "你的永久SecretID"
tccli configure set secretKey "你的永久SecretKey"
tccli configure set region ap-guangzhou

# 获取临时密钥
tccli sts AssumeRole \
  --RoleArn "qcs::cam::uin/你的账号ID:roleName/你的角色名" \
  --RoleSessionName "historyserver-$(date +%Y%m%d%H%M%S)" \
  --DurationSeconds 7200

# 输出示例：
# {
#     "Response": {
#         "Credentials": {
#             "Token": "临时token字符串...",
#             "TmpSecretId": "AKID临时密钥ID",
#             "TmpSecretKey": "临时密钥Key"
#         },
#         "Expiration": "2026-02-07T12:00:00Z",
#         "ExpiredTime": 1738929600,
#         "RequestId": "xxx"
#     }
# }
```

#### 2.2 提取临时密钥

```bash
# 使用 jq 提取（推荐）
RESPONSE=$(tccli sts AssumeRole \
  --RoleArn "qcs::cam::uin/你的账号ID:roleName/你的角色名" \
  --RoleSessionName "historyserver-$(date +%Y%m%d%H%M%S)" \
  --DurationSeconds 7200)

export TMP_SECRET_ID=$(echo $RESPONSE | jq -r '.Response.Credentials.TmpSecretId')
export TMP_SECRET_KEY=$(echo $RESPONSE | jq -r '.Response.Credentials.TmpSecretKey')
export TMP_TOKEN=$(echo $RESPONSE | jq -r '.Response.Credentials.Token')
export EXPIRATION=$(echo $RESPONSE | jq -r '.Response.Expiration')

echo "临时密钥获取成功，过期时间: $EXPIRATION"
```

---

### 步骤 3: 更新 Secret

```bash
# 方式 1: 创建新的 Secret（如果不存在）
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="$TMP_SECRET_ID" \
  --from-literal=secret-key="$TMP_SECRET_KEY" \
  --from-literal=session-token="$TMP_TOKEN" \
  -n mce-proj-production-009

# 方式 2: 更新现有 Secret
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="$TMP_SECRET_ID" \
  --from-literal=secret-key="$TMP_SECRET_KEY" \
  --from-literal=session-token="$TMP_TOKEN" \
  -n mce-proj-production-009 \
  --dry-run=client -o yaml | kubectl apply -f -

# 验证 Secret 已更新
kubectl get secret cos-secret-proj-production-009 -n mce-proj-production-009 -o yaml
```

---

### 步骤 4: 更新 Deployment/Pod YAML

#### 4.1 如果使用 Deployment

```bash
# 编辑 Deployment
kubectl edit deployment historyserver-demo -n mce-proj-production-009

# 在 spec.template.spec.containers[0].env 中添加：
# - name: COS_SESSION_TOKEN
#   valueFrom:
#     secretKeyRef:
#       key: session-token
#       name: cos-secret-proj-production-009
#       optional: true

# 或者使用 kubectl patch
kubectl patch deployment historyserver-demo -n mce-proj-production-009 --type='json' -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/env/-",
    "value": {
      "name": "COS_SESSION_TOKEN",
      "valueFrom": {
        "secretKeyRef": {
          "key": "session-token",
          "name": "cos-secret-proj-production-009",
          "optional": true
        }
      }
    }
  }
]'
```

#### 4.2 更新镜像版本

```bash
# 更新到新的镜像版本（支持临时密钥）
kubectl set image deployment/historyserver-demo \
  historyserver=ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030 \
  -n mce-proj-production-009
```

---

### 步骤 5: 重启 Pod 并验证

```bash
# 重启 Deployment
kubectl rollout restart deployment historyserver-demo -n mce-proj-production-009

# 等待 Pod 就绪
kubectl rollout status deployment historyserver-demo -n mce-proj-production-009

# 查看新 Pod 日志
POD_NAME=$(kubectl get pod -n mce-proj-production-009 -l app=historyserver -o jsonpath='{.items[0].metadata.name}')
kubectl logs -f -n mce-proj-production-009 $POD_NAME

# 预期日志：
# - 启动成功
# - 能够正常访问 COS
# - 发现集群数据
# - 没有 403 认证错误
```

---

### 步骤 6: 验证功能

```bash
# 获取 Service 地址
SERVICE_IP=$(kubectl get svc historyserver-demo -n mce-proj-production-009 -o jsonpath='{.spec.clusterIP}')

# 测试 Clusters API
kubectl run -it --rm debug --image=alpine --restart=Never -n mce-proj-production-009 -- \
  sh -c "apk add curl && curl -s http://$SERVICE_IP:8080/clusters/"

# 预期输出：返回集群列表 JSON
```

---

## 🔄 临时密钥自动刷新（可选）

由于临时密钥有有效期（通常 2 小时），建议设置自动刷新机制。

### 方案 1: 使用 CronJob 定期刷新 Secret

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: refresh-cos-temp-credentials
  namespace: mce-proj-production-009
spec:
  # 每小时刷新一次
  schedule: "0 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: cos-credential-refresher
          containers:
          - name: refresh
            image: ccr.ccs.tencentyun.com/use-test/tccli:latest
            command:
            - /bin/bash
            - -c
            - |
              set -e
              
              # 获取临时密钥
              RESPONSE=$(tccli sts AssumeRole \
                --RoleArn "$ROLE_ARN" \
                --RoleSessionName "historyserver-$(date +%Y%m%d%H%M%S)" \
                --DurationSeconds 7200)
              
              TMP_SECRET_ID=$(echo $RESPONSE | jq -r '.Response.Credentials.TmpSecretId')
              TMP_SECRET_KEY=$(echo $RESPONSE | jq -r '.Response.Credentials.TmpSecretKey')
              TMP_TOKEN=$(echo $RESPONSE | jq -r '.Response.Credentials.Token')
              
              # 更新 Secret
              kubectl create secret generic cos-secret-proj-production-009 \
                --from-literal=secret-id="$TMP_SECRET_ID" \
                --from-literal=secret-key="$TMP_SECRET_KEY" \
                --from-literal=session-token="$TMP_TOKEN" \
                -n mce-proj-production-009 \
                --dry-run=client -o yaml | kubectl apply -f -
              
              echo "临时密钥已刷新"
            env:
            - name: ROLE_ARN
              value: "qcs::cam::uin/你的账号ID:roleName/你的角色名"
          restartPolicy: OnFailure
```

### 方案 2: 使用 External Secrets Operator

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: cos-temp-credentials
  namespace: mce-proj-production-009
spec:
  refreshInterval: 1h  # 每小时刷新
  secretStoreRef:
    name: tencent-sts-store
    kind: SecretStore
  target:
    name: cos-secret-proj-production-009
  data:
  - secretKey: secret-id
    remoteRef:
      key: cos-temp-credentials
      property: TmpSecretId
  - secretKey: secret-key
    remoteRef:
      key: cos-temp-credentials
      property: TmpSecretKey
  - secretKey: session-token
    remoteRef:
      key: cos-temp-credentials
      property: Token
```

---

## 📊 监控和告警

### 监控指标

1. **COS 认证失败次数**
   - 日志关键字: `403 Forbidden`, `InvalidAccessKeyId`
   - 告警阈值: > 0 次/5分钟

2. **临时密钥过期时间**
   - 剩余时间 < 30 分钟时告警

3. **Secret 更新时间**
   - 超过 2 小时未更新时告警

### 日志查询

```bash
# 查看 COS 认证相关日志
kubectl logs -n mce-proj-production-009 -l app=historyserver | grep -i "cos\|403\|auth"

# 查看临时密钥过期错误
kubectl logs -n mce-proj-production-009 -l app=historyserver | grep -E "403|expired|invalid"
```

---

## ⚠️ 注意事项

### 1. 临时密钥有效期

- ✅ 临时密钥默认有效期为 **2 小时**
- ⚠️ 过期后需要重新获取并更新 Secret
- 🔄 建议每小时刷新一次（提前 1 小时刷新）

### 2. 权限最小化

使用临时密钥时，建议设置最小权限策略：

```json
{
  "version": "2.0",
  "statement": [
    {
      "effect": "allow",
      "action": [
        "name/cos:GetObject",
        "name/cos:HeadObject",
        "name/cos:GetBucket"
      ],
      "resource": [
        "qcs::cos:ap-guangzhou:uid/xxx:zhikuodu-1255429800/*",
        "qcs::cos:ap-guangzhou:uid/xxx:zhikuodu-1255429800/"
      ],
      "condition": {
        "string_like": {
          "cos:prefix": "mce-proj-production-009/*"
        }
      }
    }
  ]
}
```

### 3. 回滚方案

如果临时密钥出现问题，可以快速回滚到永久密钥：

```bash
# 1. 更新 Secret 为永久密钥（去掉 session-token）
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="永久SecretID" \
  --from-literal=secret-key="永久SecretKey" \
  -n mce-proj-production-009 \
  --dry-run=client -o yaml | kubectl apply -f -

# 2. 重启 Pod 生效
kubectl rollout restart deployment historyserver-demo -n mce-proj-production-009
```

### 4. 兼容性说明

- ✅ 代码完全向后兼容，支持永久密钥和临时密钥
- ✅ 如果 Secret 中没有 `session-token`，自动使用永久密钥模式
- ✅ 不影响现有使用永久密钥的部署

---

## 📝 总结

### 核心修改点

1. **代码层面**（已完成）:
   - ✅ 添加 `SessionToken` 字段支持
   - ✅ 从环境变量 `COS_SESSION_TOKEN` 读取
   - ✅ 传入 COS SDK

2. **K8s 配置层面**:
   - ✅ 在 Pod/Deployment 中添加 `COS_SESSION_TOKEN` 环境变量
   - ✅ 在 Secret 中添加 `session-token` 字段
   - ✅ 使用 `optional: true` 保持兼容性

3. **运维层面**:
   - ⚠️ 需要定期刷新临时密钥（每小时）
   - ⚠️ 监控临时密钥过期情况
   - ✅ 支持快速回滚到永久密钥

### 安全收益

- 🔐 临时密钥有时效性，泄露影响有限
- 🔐 可以限制到最小权限（只读 COS）
- 🔐 符合云原生安全最佳实践
- 🔐 支持审计和追踪

### 实施建议

1. **测试环境验证**:
   - 先在测试环境部署验证
   - 确认临时密钥功能正常
   - 验证过期后的行为

2. **生产环境部署**:
   - 使用灰度发布（先更新 1 个 Pod）
   - 监控 COS 访问日志
   - 确认无误后全量发布

3. **自动化刷新**:
   - 部署 CronJob 或使用 External Secrets Operator
   - 设置告警机制
   - 定期检查 Secret 更新时间
