# COS 认证方式分析：Collector 和 HistoryServer

## 📋 当前认证方式

### 1. Collector（数据上传端）

**代码位置**: `historyserver/pkg/storage/cos/config.go`

```go
type config struct {
    BucketURL string
    SecretID  string    // 永久密钥 ID
    SecretKey string    // 永久密钥 Key
    types.RayCollectorConfig
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]interface{}) {
    c.RayCollectorConfig = *rcc
    c.SecretID = os.Getenv("COS_SECRET_ID")      // 从环境变量读取
    c.SecretKey = os.Getenv("COS_SECRET_KEY")    // 从环境变量读取
    
    if len(jd) == 0 {
        c.BucketURL = os.Getenv("COS_BUCKET_URL")
    } else {
        if bucketURL, ok := jd["cosBucketURL"]; ok {
            c.BucketURL = bucketURL.(string)
        }
    }
}
```

**认证方式**:
- ✅ 使用永久密钥 (`SecretID` + `SecretKey`)
- ✅ 从环境变量读取: `COS_SECRET_ID`, `COS_SECRET_KEY`
- ✅ 从配置文件读取 `cosBucketURL` 或环境变量 `COS_BUCKET_URL`

---

### 2. HistoryServer（数据读取端）

**代码位置**: `historyserver/pkg/storage/cos/config.go`

```go
func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]interface{}) {
    c.RayCollectorConfig = types.RayCollectorConfig{
        RootDir: rcc.RootDir,
    }
    c.SecretID = os.Getenv("COS_SECRET_ID")      // 从环境变量读取
    c.SecretKey = os.Getenv("COS_SECRET_KEY")    // 从环境变量读取
    
    if len(jd) == 0 {
        c.BucketURL = os.Getenv("COS_BUCKET_URL")
    } else {
        if bucketURL, ok := jd["cosBucketURL"]; ok {
            c.BucketURL = bucketURL.(string)
        }
    }
}
```

**认证方式**:
- ✅ 使用永久密钥 (`SecretID` + `SecretKey`)
- ✅ 从环境变量读取: `COS_SECRET_ID`, `COS_SECRET_KEY`
- ✅ 从配置文件读取 `cosBucketURL` 或环境变量 `COS_BUCKET_URL`

---

### 3. COS 客户端初始化

**代码位置**: `historyserver/pkg/storage/cos/cos.go`

```go
func New(c *config) (*CosHandler, error) {
    u, err := url.Parse(c.BucketURL)
    if err != nil {
        return nil, fmt.Errorf("Invalid COS Bucket URL: %v", err)
    }
    
    b := &cos.BaseURL{BucketURL: u}
    client := cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:  c.SecretID,     // 永久密钥 ID
            SecretKey: c.SecretKey,    // 永久密钥 Key
            // ⚠️ 当前没有设置 SessionToken
        },
    })

    return &CosHandler{
        Client:         client,
        RootDir:        c.RootDir,
        RayClusterName: c.RayClusterName,
        RayClusterID:   c.RayClusterID,
        RayNodeName:    c.RayNodeName,
    }, nil
}
```

---

## 🔐 COS Go SDK 临时密钥支持

### AuthorizationTransport 结构体

**来源**: `github.com/tencentyun/cos-go-sdk-v5` (version: v0.7.72)

```go
type AuthorizationTransport struct {
    SecretID     string        // 密钥 ID (永久或临时)
    SecretKey    string        // 密钥 Key (永久或临时)
    SessionToken string        // 🎯 临时密钥 Token (可选)
    rwLocker     sync.RWMutex
    Expire       time.Duration  // 签名过期时间
    Transport    http.RoundTripper
}
```

**关键发现**:
- ✅ **SDK 原生支持临时密钥！**
- ✅ 使用 `SessionToken` 字段存储临时 Token
- ✅ SDK 会自动将 `SessionToken` 设置到请求头 `x-cos-security-token` 中

---

## ✅ 临时密钥方案可行性分析

### 问题：是否可以使用临时密钥？

**答案：完全可以！** ✅

### 临时密钥组成

腾讯云 COS 临时密钥包含三个部分：
1. **临时 SecretID** (`TmpSecretId`)
2. **临时 SecretKey** (`TmpSecretKey`)
3. **临时 Token** (`Token` 或 `SessionToken`)

### 对比永久密钥

| 特性 | 永久密钥 | 临时密钥 |
|------|---------|---------|
| **安全性** | ⚠️ 泄露风险高 | ✅ 有时效性，风险低 |
| **权限范围** | 完整权限 | ✅ 可限制到最小权限 |
| **有效期** | 永久 | ✅ 可配置（如 2 小时） |
| **使用场景** | 后台服务 | ✅ 前端/客户端/临时授权 |
| **SDK 支持** | ✅ 支持 | ✅ 支持 |

---

## 🔧 实现方案

### 方案 1：直接传入临时密钥（推荐用于 HistoryServer）

#### 1.1 修改配置结构

```go
// historyserver/pkg/storage/cos/config.go

type config struct {
    BucketURL    string
    SecretID     string
    SecretKey    string
    SessionToken string  // 🆕 新增字段
    types.RayCollectorConfig
}

func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]interface{}) {
    c.RayCollectorConfig = types.RayCollectorConfig{
        RootDir: rcc.RootDir,
    }
    
    // 支持永久密钥
    c.SecretID = os.Getenv("COS_SECRET_ID")
    c.SecretKey = os.Getenv("COS_SECRET_KEY")
    
    // 🆕 支持临时密钥
    c.SessionToken = os.Getenv("COS_SESSION_TOKEN")
    
    // Bucket URL
    if len(jd) == 0 {
        c.BucketURL = os.Getenv("COS_BUCKET_URL")
    } else {
        if bucketURL, ok := jd["cosBucketURL"]; ok {
            c.BucketURL = bucketURL.(string)
        }
    }
}
```

#### 1.2 修改客户端初始化

```go
// historyserver/pkg/storage/cos/cos.go

func New(c *config) (*CosHandler, error) {
    u, err := url.Parse(c.BucketURL)
    if err != nil {
        return nil, fmt.Errorf("Invalid COS Bucket URL: %v", err)
    }
    
    b := &cos.BaseURL{BucketURL: u}
    client := cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:     c.SecretID,
            SecretKey:    c.SecretKey,
            SessionToken: c.SessionToken,  // 🆕 传入 SessionToken
        },
    })

    return &CosHandler{
        Client:         client,
        RootDir:        c.RootDir,
        RayClusterName: c.RayClusterName,
        RayClusterID:   c.RayClusterID,
        RayNodeName:    c.RayNodeName,
    }, nil
}
```

#### 1.3 使用方式

```bash
# 方式 1: 使用永久密钥（当前方式）
export COS_SECRET_ID="AKID***"
export COS_SECRET_KEY="****"
export COS_BUCKET_URL="https://bucket.cos.ap-guangzhou.myqcloud.com"

# 方式 2: 使用临时密钥（新增）
export COS_SECRET_ID="AKID***-临时"
export COS_SECRET_KEY="****-临时"
export COS_SESSION_TOKEN="****-临时token"
export COS_BUCKET_URL="https://bucket.cos.ap-guangzhou.myqcloud.com"

./output/bin/historyserver \
  --runtime-class-name=cos \
  --ray-root-dir=mce-proj-bk0lwhh1 \
  --dashboard-dir=./dashboard \
  --port=8081
```

---

### 方案 2：STS 服务自动更新（推荐用于 Collector）

对于长时间运行的 Collector，可以集成 STS SDK 自动刷新临时密钥。

#### 2.1 添加 STS SDK 依赖

```go
// go.mod
require (
    github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common v1.0.x
    github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts v1.0.x
)
```

#### 2.2 实现自动刷新逻辑

```go
// historyserver/pkg/storage/cos/sts_credentials.go

package cos

import (
    "fmt"
    "sync"
    "time"
    "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
    "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts/v20180813"
)

type STSCredentials struct {
    SecretID     string
    SecretKey    string
    SessionToken string
    Expiration   time.Time
    mu           sync.RWMutex
}

type STSProvider struct {
    credentials *STSCredentials
    client      *sts.Client
    roleArn     string
    sessionName string
}

func NewSTSProvider(secretID, secretKey, roleArn, sessionName string) (*STSProvider, error) {
    credential := common.NewCredential(secretID, secretKey)
    client, err := sts.NewClient(credential, "ap-guangzhou", nil)
    if err != nil {
        return nil, err
    }
    
    return &STSProvider{
        credentials: &STSCredentials{},
        client:      client,
        roleArn:     roleArn,
        sessionName: sessionName,
    }, nil
}

func (p *STSProvider) GetCredentials() (*STSCredentials, error) {
    p.credentials.mu.RLock()
    if time.Now().Before(p.credentials.Expiration.Add(-5 * time.Minute)) {
        defer p.credentials.mu.RUnlock()
        return p.credentials, nil
    }
    p.credentials.mu.RUnlock()
    
    // 刷新凭证
    return p.refreshCredentials()
}

func (p *STSProvider) refreshCredentials() (*STSCredentials, error) {
    p.credentials.mu.Lock()
    defer p.credentials.mu.Unlock()
    
    request := sts.NewAssumeRoleRequest()
    request.RoleArn = &p.roleArn
    request.RoleSessionName = &p.sessionName
    durationSeconds := uint64(7200) // 2 小时
    request.DurationSeconds = &durationSeconds
    
    response, err := p.client.AssumeRole(request)
    if err != nil {
        return nil, fmt.Errorf("failed to assume role: %v", err)
    }
    
    expiration, _ := time.Parse(time.RFC3339, *response.Response.Expiration)
    
    p.credentials.SecretID = *response.Response.Credentials.TmpSecretId
    p.credentials.SecretKey = *response.Response.Credentials.TmpSecretKey
    p.credentials.SessionToken = *response.Response.Credentials.Token
    p.credentials.Expiration = expiration
    
    return p.credentials, nil
}
```

#### 2.3 使用 STS Provider

```go
// historyserver/pkg/storage/cos/cos.go

func NewWithSTS(c *config, stsProvider *STSProvider) (*CosHandler, error) {
    u, err := url.Parse(c.BucketURL)
    if err != nil {
        return nil, fmt.Errorf("Invalid COS Bucket URL: %v", err)
    }
    
    b := &cos.BaseURL{BucketURL: u}
    
    // 获取初始临时凭证
    creds, err := stsProvider.GetCredentials()
    if err != nil {
        return nil, fmt.Errorf("failed to get STS credentials: %v", err)
    }
    
    client := cos.NewClient(b, &http.Client{
        Transport: &cos.AuthorizationTransport{
            SecretID:     creds.SecretID,
            SecretKey:    creds.SecretKey,
            SessionToken: creds.SessionToken,
        },
    })
    
    handler := &CosHandler{
        Client:         client,
        RootDir:        c.RootDir,
        RayClusterName: c.RayClusterName,
        RayClusterID:   c.RayClusterID,
        RayNodeName:    c.RayNodeName,
    }
    
    // 启动后台协程定期刷新凭证
    go handler.refreshCredentialsPeriodically(stsProvider)
    
    return handler, nil
}

func (h *CosHandler) refreshCredentialsPeriodically(provider *STSProvider) {
    ticker := time.NewTicker(30 * time.Minute) // 每 30 分钟检查一次
    defer ticker.Stop()
    
    for range ticker.C {
        creds, err := provider.GetCredentials()
        if err != nil {
            logrus.Errorf("Failed to refresh STS credentials: %v", err)
            continue
        }
        
        // 更新客户端凭证
        h.Client = cos.NewClient(
            h.Client.BaseURL, 
            &http.Client{
                Transport: &cos.AuthorizationTransport{
                    SecretID:     creds.SecretID,
                    SecretKey:    creds.SecretKey,
                    SessionToken: creds.SessionToken,
                },
            },
        )
        
        logrus.Infof("STS credentials refreshed, expiration: %v", creds.Expiration)
    }
}
```

---

## 🎯 推荐方案

### 场景 1: HistoryServer (数据读取)

**推荐**: **方案 1 - 直接传入临时密钥**

**理由**:
- HistoryServer 通常部署在内网，安全性较高
- 如果使用 K8s ServiceAccount + IRSA/Workload Identity，可以自动获取临时密钥
- 实现简单，无需额外的 STS SDK 依赖

**配置示例**:
```yaml
# K8s Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: historyserver
spec:
  template:
    spec:
      containers:
      - name: historyserver
        env:
        - name: COS_SECRET_ID
          valueFrom:
            secretKeyRef:
              name: cos-temp-credentials
              key: tmpSecretId
        - name: COS_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: cos-temp-credentials
              key: tmpSecretKey
        - name: COS_SESSION_TOKEN
          valueFrom:
            secretKeyRef:
              name: cos-temp-credentials
              key: sessionToken
        - name: COS_BUCKET_URL
          value: "https://bucket.cos.ap-guangzhou.myqcloud.com"
```

---

### 场景 2: Collector (数据上传)

**推荐**: **方案 2 - STS 自动刷新** 或 **永久密钥（仅限内网）**

**理由**:
- Collector 运行在 Ray Pod 中，生命周期较短（Job 完成后销毁）
- 如果 Job 运行时间超过临时密钥有效期（如 2 小时），需要自动刷新
- 如果 Job 运行时间短（< 1 小时），可以直接使用初始临时密钥

**配置示例**:
```yaml
# RayJob 配置
apiVersion: ray.io/v1
kind: RayJob
spec:
  rayClusterSpec:
    headGroupSpec:
      template:
        spec:
          containers:
          - name: ray-head
            env:
            - name: COS_SECRET_ID
              valueFrom:
                secretKeyRef:
                  name: cos-temp-credentials
                  key: tmpSecretId
            - name: COS_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: cos-temp-credentials
                  key: tmpSecretKey
            - name: COS_SESSION_TOKEN
              valueFrom:
                secretKeyRef:
                  name: cos-temp-credentials
                  key: sessionToken
```

---

## 📊 对比总结

| 特性 | 永久密钥 | 临时密钥 (方案1) | STS 自动刷新 (方案2) |
|------|---------|----------------|---------------------|
| **安全性** | ⚠️ 低 | ✅ 高 | ✅ 最高 |
| **实现复杂度** | ✅ 简单 | ✅ 简单 | ⚠️ 中等 |
| **适用场景** | 内网测试 | 短生命周期服务 | 长时间运行服务 |
| **权限控制** | ⚠️ 完整权限 | ✅ 可限制最小权限 | ✅ 可限制最小权限 |
| **是否需要刷新** | ❌ 否 | ✅ 是（手动） | ✅ 是（自动） |
| **代码修改** | ✅ 无需修改 | ⚠️ 少量修改 | ⚠️ 需要添加 STS 逻辑 |

---

## 🚀 实施步骤

### 步骤 1: 修改代码（支持临时密钥）

```bash
# 1. 修改 config.go
vi historyserver/pkg/storage/cos/config.go

# 2. 修改 cos.go
vi historyserver/pkg/storage/cos/cos.go

# 3. 重新编译
cd historyserver
make build
```

### 步骤 2: 获取临时密钥

```bash
# 使用腾讯云 CLI 获取临时密钥
tccli sts AssumeRole \
  --RoleArn "qcs::cam::uin/xxx:roleName/xxx" \
  --RoleSessionName "historyserver-test" \
  --DurationSeconds 7200

# 输出示例:
# {
#   "Credentials": {
#     "TmpSecretId": "AKID***-临时",
#     "TmpSecretKey": "***-临时",
#     "Token": "***-临时token"
#   },
#   "Expiration": "2026-02-03T12:00:00Z"
# }
```

### 步骤 3: 使用临时密钥启动

```bash
export COS_SECRET_ID="AKID***-临时"
export COS_SECRET_KEY="***-临时"
export COS_SESSION_TOKEN="***-临时token"
export COS_BUCKET_URL="https://bucket.cos.ap-guangzhou.myqcloud.com"

./output/bin/historyserver \
  --runtime-class-name=cos \
  --ray-root-dir=mce-proj-bk0lwhh1 \
  --dashboard-dir=./dashboard \
  --port=8081
```

### 步骤 4: 验证

```bash
# 查看日志，确认 COS 访问正常
tail -f /tmp/historyserver.log | grep -i "cos\|cluster"

# 测试 API
curl http://localhost:8081/clusters/
```

---

## ⚠️ 注意事项

### 1. 临时密钥有效期

- 临时密钥通常有效期为 **2 小时**（可配置 1-2 小时）
- 需要在过期前刷新凭证，否则 COS 请求会失败（403 错误）

### 2. 权限最小化

使用临时密钥时，建议遵循**最小权限原则**：

```json
{
  "version": "2.0",
  "statement": [
    {
      "effect": "allow",
      "action": [
        "cos:GetObject",
        "cos:HeadObject",
        "cos:ListBucket"
      ],
      "resource": [
        "qcs::cos:ap-guangzhou:uid/xxx:bucket-xxx/*",
        "qcs::cos:ap-guangzhou:uid/xxx:bucket-xxx/"
      ]
    }
  ]
}
```

### 3. 错误处理

```go
// 当临时密钥过期时，COS SDK 会返回 403 错误
// 示例错误: "403 RequestTimeTooSkewed" 或 "403 InvalidAccessKeyId"
// 需要捕获此错误并刷新凭证
```

---

## 📝 总结

1. **当前状态**:
   - ✅ Collector 和 HistoryServer **都使用永久密钥**
   - ✅ 从环境变量 `COS_SECRET_ID` 和 `COS_SECRET_KEY` 读取

2. **临时密钥支持**:
   - ✅ **COS Go SDK 原生支持临时密钥**（通过 `SessionToken` 字段）
   - ✅ **完全可以使用临时 SecretID + SecretKey + Token**

3. **推荐方案**:
   - **HistoryServer**: 方案 1（直接传入临时密钥）
   - **Collector**: 方案 1（短生命周期 Job）或方案 2（长时间运行）

4. **实施成本**:
   - **代码修改**: 少量（约 10-20 行）
   - **测试工作**: 中等（需要测试临时密钥刷新逻辑）
   - **安全提升**: 显著（从永久密钥升级到临时密钥）
