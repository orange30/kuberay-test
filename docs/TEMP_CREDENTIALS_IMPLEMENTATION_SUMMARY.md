# 临时密钥实现总结

## ✅ 已完成的工作

### 1. 代码修改（支持临时密钥）

**修改文件：**
- ✅ `historyserver/pkg/storage/cos/config.go` - 添加 `SessionToken` 字段
- ✅ `historyserver/pkg/storage/cos/cos.go` - 传入 `SessionToken` 到 COS SDK

**核心变更：**
```go
// 1. config 结构体新增字段
type config struct {
    BucketURL    string
    SecretID     string
    SecretKey    string
    SessionToken string  // 🆕 支持临时密钥
    types.RayCollectorConfig
}

// 2. 从环境变量读取
c.SessionToken = os.Getenv("COS_SESSION_TOKEN")

// 3. 传入 COS SDK
client := cos.NewClient(b, &http.Client{
    Transport: &cos.AuthorizationTransport{
        SecretID:     c.SecretID,
        SecretKey:    c.SecretKey,
        SessionToken: c.SessionToken,  // 🆕
    },
})
```

**代码行数：** 仅修改 4 处，新增约 4 行代码

**兼容性：** ✅ 完全向后兼容，支持永久密钥和临时密钥

---

### 2. 文档创建

已创建以下文档：

| 文档名称 | 位置 | 说明 |
|---------|------|------|
| **认证方式分析** | `docs/COS_AUTHENTICATION_ANALYSIS.md` | 详细分析当前认证方式和临时密钥支持 |
| **部署指南** | `docs/DEPLOY_WITH_TEMP_CREDENTIALS.md` | 完整的部署步骤和注意事项 |
| **YAML 修改总结** | `docs/YAML_MODIFICATION_SUMMARY.md` | 简洁的 YAML 修改说明 |
| **实现总结** | `docs/TEMP_CREDENTIALS_IMPLEMENTATION_SUMMARY.md` | 本文档 |

---

### 3. 配置文件和脚本

**Kubernetes YAML 模板：**
- ✅ `historyserver/deploy/historyserver-with-temp-credentials.yaml` - 完整的 Pod 配置示例
- ✅ `historyserver/deploy/cos-temp-secret-example.yaml` - Secret 配置示例

**自动化脚本：**
- ✅ `historyserver/scripts/update-temp-credentials.sh` - 自动获取和更新临时密钥

---

## 🎯 核心修改点总结

### 代码层面（已完成）

```diff
# historyserver/pkg/storage/cos/config.go
type config struct {
    BucketURL    string
    SecretID     string
    SecretKey    string
+   SessionToken string  // 支持临时密钥
    types.RayCollectorConfig
}

func (c *config) complete(...) {
    c.SecretID = os.Getenv("COS_SECRET_ID")
    c.SecretKey = os.Getenv("COS_SECRET_KEY")
+   c.SessionToken = os.Getenv("COS_SESSION_TOKEN")  // 支持临时密钥
}

func (c *config) completeHSConfig(...) {
    c.SecretID = os.Getenv("COS_SECRET_ID")
    c.SecretKey = os.Getenv("COS_SECRET_KEY")
+   c.SessionToken = os.Getenv("COS_SESSION_TOKEN")  // 支持临时密钥
}

# historyserver/pkg/storage/cos/cos.go
func New(c *config) (*CosHandler, error) {
    client := cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:     c.SecretID,
            SecretKey:    c.SecretKey,
+           SessionToken: c.SessionToken,  // 支持临时密钥
        },
    })
}
```

---

### Kubernetes 配置层面（需要实施）

**只需添加 1 个环境变量：**

```yaml
spec:
  containers:
  - name: historyserver
    env:
    # ... 现有环境变量保持不变 ...
    
    # 🆕 新增：支持临时密钥
    - name: COS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          key: session-token
          name: cos-secret-proj-production-009
          optional: true  # 兼容永久密钥
```

**Secret 需要添加 1 个字段：**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret-proj-production-009
  namespace: mce-proj-production-009
type: Opaque
data:
  secret-id: <临时 SecretID (Base64)>
  secret-key: <临时 SecretKey (Base64)>
  session-token: <临时 Token (Base64)>  # 🆕 新增
```

---

## 🚀 实施步骤

### 步骤 1: 构建并推送新镜像

```bash
cd /Users/zhikuodu/work/work_ray/kuberay-test/historyserver

# 构建镜像
make docker-build

# 推送到 CCR
docker tag historyserver:latest ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030
docker push ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030
```

---

### 步骤 2: 更新 Kubernetes 配置

#### 方式 1: 使用 kubectl patch（推荐）

```bash
# 添加 COS_SESSION_TOKEN 环境变量
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

# 更新镜像
kubectl set image deployment/historyserver-demo \
  historyserver=ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030 \
  -n mce-proj-production-009
```

#### 方式 2: 使用 kubectl edit

```bash
kubectl edit deployment historyserver-demo -n mce-proj-production-009

# 在 spec.template.spec.containers[0].env 中添加：
# - name: COS_SESSION_TOKEN
#   valueFrom:
#     secretKeyRef:
#       key: session-token
#       name: cos-secret-proj-production-009
#       optional: true
```

---

### 步骤 3: 获取并更新临时密钥

```bash
# 设置 RoleArn
export ROLE_ARN="qcs::cam::uin/你的账号ID:roleName/你的角色名"

# 使用脚本自动获取和更新
bash /Users/zhikuodu/work/work_ray/kuberay-test/historyserver/scripts/update-temp-credentials.sh
```

**脚本会自动：**
1. ✅ 调用腾讯云 STS API 获取临时密钥
2. ✅ 更新 Kubernetes Secret
3. ✅ 询问是否重启 Deployment
4. ✅ 验证 Secret 更新成功

---

### 步骤 4: 验证功能

```bash
# 1. 检查 Pod 状态
kubectl get pod -n mce-proj-production-009 -l app=historyserver

# 2. 查看环境变量
POD_NAME=$(kubectl get pod -n mce-proj-production-009 -l app=historyserver -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n mce-proj-production-009 $POD_NAME -- env | grep COS

# 预期输出：
# COS_BUCKET_URL=https://...
# COS_SECRET_ID=AKID...
# COS_SECRET_KEY=***
# COS_SESSION_TOKEN=***  # 🆕 应该存在

# 3. 查看日志
kubectl logs -n mce-proj-production-009 $POD_NAME | grep -E "Found.*clusters|COS|403"

# 预期输出：
# Found 6 clusters from storage  ✅
# 没有 403 错误  ✅

# 4. 测试 API
kubectl run -it --rm debug --image=alpine --restart=Never -n mce-proj-production-009 -- \
  sh -c "apk add curl && curl -s http://historyserver-demo:8080/clusters/ | head -20"

# 预期输出：返回 JSON 格式的集群列表  ✅
```

---

## 📊 技术实现细节

### COS Go SDK 支持

**SDK 版本：** `github.com/tencentyun/cos-go-sdk-v5` v0.7.72

**AuthorizationTransport 结构体：**
```go
type AuthorizationTransport struct {
    SecretID     string        // 密钥 ID (永久或临时)
    SecretKey    string        // 密钥 Key (永久或临时)
    SessionToken string        // 🎯 临时密钥 Token
    rwLocker     sync.RWMutex
    Expire       time.Duration
    Transport    http.RoundTripper
}
```

**工作原理：**
1. 当 `SessionToken` 非空时，SDK 会自动将其设置到 HTTP 请求头 `x-cos-security-token`
2. COS 服务端验证临时密钥的有效性
3. 如果 `SessionToken` 为空，SDK 使用永久密钥模式

---

### 环境变量读取逻辑

```go
// 同时支持永久密钥和临时密钥
c.SecretID = os.Getenv("COS_SECRET_ID")          // 必需
c.SecretKey = os.Getenv("COS_SECRET_KEY")        // 必需
c.SessionToken = os.Getenv("COS_SESSION_TOKEN")  // 可选

// 当 SessionToken 为空时，自动使用永久密钥模式
// 当 SessionToken 非空时，使用临时密钥模式
```

---

### Kubernetes Secret 机制

```yaml
# Secret 定义
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret-proj-production-009
data:
  secret-id: <Base64 编码>
  secret-key: <Base64 编码>
  session-token: <Base64 编码>  # 可选

# Pod 引用（optional: true 保证兼容性）
env:
- name: COS_SESSION_TOKEN
  valueFrom:
    secretKeyRef:
      key: session-token
      name: cos-secret-proj-production-009
      optional: true  # 关键！当 key 不存在时不报错
```

---

## 🔐 安全最佳实践

### 1. 临时密钥权限配置

**最小权限策略示例：**
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
        "qcs::cos:ap-guangzhou:uid/xxx:zhikuodu-1255429800/mce-proj-production-009/*"
      ]
    }
  ]
}
```

**说明：**
- ✅ 只授予读取权限（GetObject, HeadObject, GetBucket）
- ✅ 限制到特定的路径前缀（mce-proj-production-009/*）
- ✅ 不授予写入、删除等高危权限

---

### 2. 临时密钥刷新策略

**推荐配置：**
- 临时密钥有效期：**2 小时**
- 刷新间隔：**1 小时**（提前 1 小时刷新）
- 告警阈值：剩余 30 分钟时告警

**实现方式：**

#### 方式 1: CronJob 自动刷新

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: refresh-cos-credentials
  namespace: mce-proj-production-009
spec:
  schedule: "0 * * * *"  # 每小时执行一次
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: refresh
            image: ccr.ccs.tencentyun.com/use-test/tccli:latest
            command:
            - /bin/bash
            - /scripts/update-temp-credentials.sh
            env:
            - name: ROLE_ARN
              value: "qcs::cam::uin/xxx:roleName/xxx"
            - name: NAMESPACE
              value: "mce-proj-production-009"
          restartPolicy: OnFailure
```

#### 方式 2: External Secrets Operator

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: cos-temp-credentials
  namespace: mce-proj-production-009
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: tencent-sts-store
    kind: SecretStore
  target:
    name: cos-secret-proj-production-009
```

---

### 3. 监控和告警

**监控指标：**
1. COS 认证失败次数
2. 临时密钥剩余有效期
3. Secret 更新时间

**告警规则：**
```yaml
# Prometheus AlertRule 示例
- alert: COSAuthenticationFailed
  expr: rate(cos_auth_errors_total[5m]) > 0
  annotations:
    summary: "COS 认证失败"
    description: "临时密钥可能已过期"

- alert: TempCredentialsExpiringSoon
  expr: cos_temp_credentials_ttl_seconds < 1800
  annotations:
    summary: "临时密钥即将过期"
    description: "剩余时间少于 30 分钟"
```

---

## ⚠️ 注意事项

### 1. 兼容性

- ✅ 完全向后兼容，支持永久密钥和临时密钥
- ✅ 可以先部署新代码，后续再切换到临时密钥
- ✅ 回滚方便：移除 Secret 中的 `session-token` 即可

### 2. 临时密钥有效期

- ⚠️ 临时密钥默认有效期为 **2 小时**
- ⚠️ 过期后会导致 403 错误
- ✅ 需要设置自动刷新机制

### 3. 故障处理

**问题：临时密钥过期**
```bash
# 症状：日志中出现 403 错误
kubectl logs -n mce-proj-production-009 -l app=historyserver | grep 403

# 解决方案 1：立即刷新临时密钥
bash historyserver/scripts/update-temp-credentials.sh

# 解决方案 2：快速回滚到永久密钥
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="永久SecretID" \
  --from-literal=secret-key="永久SecretKey" \
  -n mce-proj-production-009 \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl rollout restart deployment historyserver-demo -n mce-proj-production-009
```

---

## 📈 对比总结

| 特性 | 永久密钥 | 临时密钥 |
|------|---------|---------|
| **安全性** | ⚠️ 低（泄露影响永久） | ✅ 高（有时效性） |
| **权限范围** | ⚠️ 完整权限 | ✅ 可限制最小权限 |
| **有效期** | 永久 | 2 小时（可配置） |
| **实施复杂度** | ✅ 简单 | ⚠️ 中等（需要刷新机制） |
| **代码修改** | ✅ 无需修改 | ✅ 少量修改（4 处） |
| **运维成本** | ✅ 低 | ⚠️ 中等（需要监控和刷新） |
| **合规性** | ⚠️ 较低 | ✅ 符合安全最佳实践 |
| **推荐场景** | 内网测试环境 | ✅ 生产环境 |

---

## ✅ 最终检查清单

### 代码层面
- [x] 修改 `config.go` 添加 `SessionToken` 字段
- [x] 修改 `cos.go` 传入 `SessionToken` 到 COS SDK
- [x] 代码编译通过
- [x] 向后兼容性验证

### 文档层面
- [x] 创建认证方式分析文档
- [x] 创建部署指南
- [x] 创建 YAML 修改总结
- [x] 创建实现总结文档
- [x] 创建配置文件示例
- [x] 创建自动化脚本

### 部署层面
- [ ] 构建并推送新镜像
- [ ] 更新 Deployment YAML
- [ ] 获取临时密钥
- [ ] 更新 Secret
- [ ] 重启 Pod
- [ ] 验证功能正常

### 运维层面
- [ ] 设置临时密钥自动刷新
- [ ] 配置监控和告警
- [ ] 测试故障恢复流程
- [ ] 编写操作手册

---

## 📚 相关文档

1. **详细分析**: `docs/COS_AUTHENTICATION_ANALYSIS.md`
2. **部署指南**: `docs/DEPLOY_WITH_TEMP_CREDENTIALS.md`
3. **YAML 修改**: `docs/YAML_MODIFICATION_SUMMARY.md`
4. **配置示例**: `historyserver/deploy/cos-temp-secret-example.yaml`
5. **自动化脚本**: `historyserver/scripts/update-temp-credentials.sh`

---

## 🎯 下一步行动

1. ✅ **构建新镜像**: `make docker-build && docker push`
2. ✅ **测试环境验证**: 先在测试环境部署和验证
3. ✅ **生产环境部署**: 使用灰度发布策略
4. ✅ **监控和告警**: 设置 COS 认证监控
5. ✅ **自动化刷新**: 部署 CronJob 或 External Secrets Operator

---

**实施完成后，你将获得：**
- 🔐 更高的安全性（临时密钥有时效性）
- 🔐 更细的权限控制（最小权限原则）
- 🔐 更好的合规性（符合安全最佳实践）
- ✅ 完全向后兼容（支持永久密钥和临时密钥）
