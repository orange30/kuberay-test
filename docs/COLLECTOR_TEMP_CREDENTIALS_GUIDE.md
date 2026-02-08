# Collector 临时密钥支持指南

## ✅ 核心结论

**Collector 已经支持临时密钥！** 

我们之前对 `historyserver/pkg/storage/cos/config.go` 和 `cos.go` 的修改，已经自动覆盖了 Collector 和 HistoryServer。

---

## 🔍 代码分析

### 1. **Collector 使用 COS 的路径**

```go
// historyserver/cmd/collector/main.go
registry := collector.GetWriterRegistry()
factory, ok := registry[runtimeClassName]  // runtimeClassName = "cos"
writer, err := factory(&globalConfig, jsonData)

// historyserver/pkg/collector/registry.go
var writerRegistry = WriterRegistry{
    "cos": cos.NewWriter,  // 👈 使用 cos.NewWriter
}

// historyserver/pkg/storage/cos/cos.go
func NewWriter(c *types.RayCollectorConfig, jd map[string]interface{}) (storage.StorageWriter, error) {
    config := &config{}
    config.complete(c, jd)  // 👈 调用 complete 方法
    return New(config)
}

// historyserver/pkg/storage/cos/config.go
func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]interface{}) {
    c.SecretID = os.Getenv("COS_SECRET_ID")
    c.SecretKey = os.Getenv("COS_SECRET_KEY")
    c.SessionToken = os.Getenv("COS_SESSION_TOKEN")  // ✅ 支持临时密钥！
}
```

### 2. **HistoryServer 使用 COS 的路径**

```go
// historyserver/pkg/historyserver/server.go
reader, err := readerFactory(hsConfig, jsonData)

// historyserver/pkg/collector/registry.go
var readerRegistry = ReaderRegistry{
    "cos": cos.NewReader,  // 👈 使用 cos.NewReader
}

// historyserver/pkg/storage/cos/cos.go
func NewReader(c *types.RayHistoryServerConfig, jd map[string]interface{}) (storage.StorageReader, error) {
    config := &config{}
    config.completeHSConfig(c, jd)  // 👈 调用 completeHSConfig 方法
    return New(config)
}

// historyserver/pkg/storage/cos/config.go
func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]interface{}) {
    c.SecretID = os.Getenv("COS_SECRET_ID")
    c.SecretKey = os.Getenv("COS_SECRET_KEY")
    c.SessionToken = os.Getenv("COS_SESSION_TOKEN")  // ✅ 支持临时密钥！
}
```

### 3. **最终初始化 COS Client**

```go
// historyserver/pkg/storage/cos/cos.go
func New(c *config) (*CosHandler, error) {
    client := cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:     c.SecretID,
            SecretKey:    c.SecretKey,
            SessionToken: c.SessionToken,  // ✅ 支持临时密钥！
        },
    })
    return &CosHandler{...}, nil
}
```

---

## 📋 Collector YAML 配置

### **使用永久密钥（旧方式）**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: raycluster-head
spec:
  containers:
  - name: ray-head
    image: rayproject/ray:2.9.0
    env:
    - name: COS_BUCKET_URL
      value: "https://your-bucket.cos.ap-guangzhou.myqcloud.com"
    - name: COS_SECRET_ID
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: secret-id
    - name: COS_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: secret-key
    # ❌ 没有 COS_SESSION_TOKEN
```

---

### **使用临时密钥（新方式）** ✅

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: raycluster-head
spec:
  containers:
  - name: ray-head
    image: rayproject/ray:2.9.0
    env:
    - name: COS_BUCKET_URL
      value: "https://your-bucket.cos.ap-guangzhou.myqcloud.com"
    - name: COS_SECRET_ID
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: secret-id
    - name: COS_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: secret-key
    # ✅ 添加临时密钥 Token
    - name: COS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: session-token
          optional: true  # 兼容永久密钥
```

---

## 🔐 Secret 配置

### **创建包含临时密钥的 Secret**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret
  namespace: your-namespace
type: Opaque
stringData:
  secret-id: "AKID***临时SecretID***"
  secret-key: "***临时SecretKey***"
  session-token: "***临时Token（很长的字符串）***"
```

### **创建 Secret 命令**

```bash
# 从环境变量创建
kubectl create secret generic cos-secret \
  --namespace=your-namespace \
  --from-literal=secret-id="$TMP_SECRET_ID" \
  --from-literal=secret-key="$TMP_SECRET_KEY" \
  --from-literal=session-token="$TMP_SESSION_TOKEN"

# 或者使用 YAML 文件
kubectl apply -f cos-secret.yaml
```

---

## 🧪 测试验证

### **1. 检查 Collector Pod 的环境变量**

```bash
# 进入 Collector Pod
kubectl exec -it raycluster-head-xxxxx -n your-namespace -- bash

# 检查环境变量
echo "COS_SECRET_ID: $COS_SECRET_ID"
echo "COS_SECRET_KEY: $COS_SECRET_KEY"
echo "COS_SESSION_TOKEN: $COS_SESSION_TOKEN"

# 如果 COS_SESSION_TOKEN 有值，说明配置成功
```

### **2. 查看 Collector 日志**

```bash
# 查看 Collector 启动日志
kubectl logs raycluster-head-xxxxx -n your-namespace | grep -i "cos\|secret\|token"

# 预期输出（不应该有认证错误）
# Using collector config: ...
# Successfully wrote object xxx, size: xxx bytes
```

### **3. 验证 COS 上传**

```bash
# 查看 COS 上是否有日志文件
# 访问腾讯云 COS 控制台，检查路径：
# your-bucket/your-root-dir/your-cluster-name_cluster-id/logs/
```

---

## ⚙️ 完整的 RayCluster 配置示例

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-test
  namespace: your-namespace
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.9.0
          env:
          # ✅ 1. COS 配置
          - name: COS_BUCKET_URL
            value: "https://your-bucket.cos.ap-guangzhou.myqcloud.com"
          - name: COS_SECRET_ID
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: secret-id
          - name: COS_SECRET_KEY
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: secret-key
          # ✅ 2. 临时密钥 Token（新增）
          - name: COS_SESSION_TOKEN
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: session-token
                optional: true
          
          # ✅ 3. Collector 配置
          - name: RAY_CLUSTER_NAME
            value: "raycluster-test"
          - name: RAY_CLUSTER_ID
            value: "abc123"
          
          volumeMounts:
          - name: collector-config
            mountPath: /var/collector-config
        
        volumes:
        - name: collector-config
          configMap:
            name: collector-config

  workerGroupSpecs:
  - replicas: 2
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.9.0
          env:
          # ✅ Worker 也需要相同的 COS 配置
          - name: COS_BUCKET_URL
            value: "https://your-bucket.cos.ap-guangzhou.myqcloud.com"
          - name: COS_SECRET_ID
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: secret-id
          - name: COS_SECRET_KEY
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: secret-key
          # ✅ Worker 也需要临时密钥 Token
          - name: COS_SESSION_TOKEN
            valueFrom:
              secretKeyRef:
                name: cos-secret
                key: session-token
                optional: true
          
          volumeMounts:
          - name: collector-config
            mountPath: /var/collector-config
        
        volumes:
        - name: collector-config
          configMap:
            name: collector-config
```

---

## 🔄 临时密钥自动刷新方案

由于临时密钥有效期通常为 2 小时，需要定期刷新：

### **方案 1: CronJob 自动更新 Secret**

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: refresh-cos-credentials
  namespace: your-namespace
spec:
  schedule: "0 */1 * * *"  # 每小时执行一次
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: cos-credential-updater
          containers:
          - name: refresh
            image: your-registry/cos-credential-refresher:latest
            env:
            - name: SECRET_NAME
              value: "cos-secret"
            - name: ROLE_ARN
              value: "qcs::cam::uin/your-uin:roleName/your-role"
          restartPolicy: OnFailure
```

### **方案 2: Init Container + Sidecar**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: raycluster-head
spec:
  initContainers:
  - name: init-credentials
    image: your-registry/cos-credential-fetcher:latest
    volumeMounts:
    - name: credentials
      mountPath: /credentials
  
  containers:
  - name: ray-head
    image: rayproject/ray:2.9.0
    env:
    - name: COS_SECRET_ID
      valueFrom:
        secretKeyRef:
          name: cos-secret
          key: secret-id
    # ... 其他环境变量
  
  - name: credential-refresher
    image: your-registry/cos-credential-refresher:latest
    env:
    - name: REFRESH_INTERVAL
      value: "3600"  # 每小时刷新
    volumeMounts:
    - name: credentials
      mountPath: /credentials
  
  volumes:
  - name: credentials
    emptyDir: {}
```

---

## 📊 对比总结

| 特性 | Collector（旧） | Collector（新） |
|------|----------------|----------------|
| **密钥类型** | 永久密钥 | 临时密钥 ✅ |
| **环境变量** | 2 个（ID + Key） | 3 个（ID + Key + Token） |
| **安全性** | ⚠️ 泄露风险高 | ✅ 2 小时自动失效 |
| **代码修改** | ✅ 无需修改 | ✅ **无需修改**（已自动支持） |
| **YAML 修改** | ✅ 现有配置 | ✅ 添加 1 个环境变量 |
| **向后兼容** | ✅ 支持 | ✅ 完全兼容 |

---

## 🎯 关键要点

1. ✅ **Collector 已经支持临时密钥**
   - 我们之前的修改已经覆盖了 Collector 和 HistoryServer
   - 无需额外的代码修改

2. ✅ **只需修改 YAML 配置**
   - 在 RayCluster 的 Pod 配置中添加 `COS_SESSION_TOKEN` 环境变量
   - Head 和 Worker 都需要添加

3. ✅ **完全向后兼容**
   - 如果不提供 `COS_SESSION_TOKEN`，会继续使用永久密钥
   - `optional: true` 保证兼容性

4. ⚠️ **注意临时密钥过期**
   - 临时密钥通常有效期 2 小时
   - 需要定期刷新 Secret 或重启 Pod

---

## 🚀 下一步

1. ✅ **更新 RayCluster YAML**
   - 添加 `COS_SESSION_TOKEN` 环境变量

2. ✅ **创建包含临时密钥的 Secret**
   - 使用 STS 获取临时密钥
   - 创建包含 3 个字段的 Secret

3. ✅ **部署并验证**
   - 应用新的 RayCluster 配置
   - 查看 Collector 日志
   - 检查 COS 上的文件

4. ⚠️ **设置自动刷新**
   - 使用 CronJob 定期更新 Secret
   - 或使用 Sidecar 自动刷新凭证

---

## 📚 相关文档

- [COS 认证方式分析](./COS_AUTHENTICATION_ANALYSIS.md)
- [HistoryServer 临时密钥部署](./DEPLOY_WITH_TEMP_CREDENTIALS.md)
- [临时密钥实现总结](./TEMP_CREDENTIALS_IMPLEMENTATION_SUMMARY.md)
- [快速启动指南](../TEMP_CREDENTIALS_QUICK_START.md)
