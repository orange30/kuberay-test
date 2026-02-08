# Kubernetes YAML 修改总结

## 🎯 核心修改：只需添加 1 个环境变量

### 修改前（使用永久密钥）

```yaml
spec:
  containers:
  - name: historyserver
    env:
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
```

### 修改后（支持临时密钥）

```yaml
spec:
  containers:
  - name: historyserver
    env:
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
    # 🆕 新增：支持临时密钥
    - name: COS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          key: session-token
          name: cos-secret-proj-production-009
          optional: true  # 兼容永久密钥（可选）
```

**关键点：**
- ✅ 只需添加 **1 个环境变量**：`COS_SESSION_TOKEN`
- ✅ 使用 `optional: true` 保持向后兼容
- ✅ 如果 Secret 中没有 `session-token`，不会报错，自动使用永久密钥

---

## 📦 Secret 修改

### 方式 1: 使用永久密钥（无需修改）

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret-proj-production-009
  namespace: mce-proj-production-009
type: Opaque
data:
  secret-id: <Base64 编码的永久 SecretID>
  secret-key: <Base64 编码的永久 SecretKey>
  # 不需要 session-token
```

### 方式 2: 使用临时密钥（推荐）

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cos-secret-proj-production-009
  namespace: mce-proj-production-009
type: Opaque
data:
  secret-id: <Base64 编码的临时 SecretID>
  secret-key: <Base64 编码的临时 SecretKey>
  session-token: <Base64 编码的临时 Token>  # 🆕 新增
```

**创建命令：**
```bash
# 从腾讯云 STS 获取临时密钥后
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="临时SecretID" \
  --from-literal=secret-key="临时SecretKey" \
  --from-literal=session-token="临时Token" \
  -n mce-proj-production-009
```

---

## 🚀 快速部署命令

### 步骤 1: 使用 kubectl patch 添加环境变量

```bash
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

### 步骤 2: 获取并更新临时密钥

```bash
# 使用提供的脚本
export ROLE_ARN="qcs::cam::uin/你的账号ID:roleName/你的角色名"
bash historyserver/scripts/update-temp-credentials.sh
```

### 步骤 3: 更新镜像并重启

```bash
# 更新到支持临时密钥的新镜像
kubectl set image deployment/historyserver-demo \
  historyserver=ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030 \
  -n mce-proj-production-009

# 重启生效
kubectl rollout restart deployment historyserver-demo -n mce-proj-production-009
```

---

## ✅ 验证

### 验证 1: 检查环境变量

```bash
POD_NAME=$(kubectl get pod -n mce-proj-production-009 -l app=historyserver -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n mce-proj-production-009 $POD_NAME -- env | grep COS
```

**预期输出：**
```
COS_BUCKET_URL=https://zhikuodu-1255429800.cos.ap-guangzhou.myqcloud.com
COS_SECRET_ID=AKID***
COS_SECRET_KEY=***
COS_SESSION_TOKEN=***  # 🆕 应该存在
```

### 验证 2: 检查 COS 访问

```bash
# 查看日志
kubectl logs -n mce-proj-production-009 -l app=historyserver | grep -E "Found.*clusters|COS"

# 预期输出：
# Found 6 clusters from storage
# 没有 403 错误
```

### 验证 3: 测试 API

```bash
# 获取集群列表
kubectl run -it --rm debug --image=alpine --restart=Never -n mce-proj-production-009 -- \
  sh -c "apk add curl && curl -s http://historyserver-demo:8080/clusters/ | head -20"

# 预期输出：返回 JSON 格式的集群列表
```

---

## 📊 完整 Deployment 示例

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: historyserver-demo
  namespace: mce-proj-production-009
spec:
  replicas: 1
  selector:
    matchLabels:
      app: historyserver
  template:
    metadata:
      labels:
        app: historyserver
    spec:
      serviceAccountName: historyserver
      containers:
      - name: historyserver
        image: ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030
        command:
        - historyserver
        - --runtime-class-name=cos
        - --ray-root-dir=mce-proj-production-009
        env:
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
        # 🆕 支持临时密钥
        - name: COS_SESSION_TOKEN
          valueFrom:
            secretKeyRef:
              key: session-token
              name: cos-secret-proj-production-009
              optional: true
        - name: RAY_GRAFANA_IFRAME_HOST
          value: http://118.25.169.144:5001
        - name: RAY_GRAFANA_HOST
          value: http://118.25.169.144:5001
        - name: RAY_PROMETHEUS_HOST
          value: http://118.25.169.144:5002
        - name: HISTORYSERVER_REFRESH_INTERVAL
          value: 5m
        - name: HISTORYSERVER_SESSION_MAX_AGE
          value: 72h
        ports:
        - containerPort: 8080
          protocol: TCP
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 500m
            memory: 512Mi
```

---

## ⚠️ 注意事项

1. **镜像版本**:
   - 必须使用支持临时密钥的新镜像（`historyserver-m030` 或更新版本）
   - 旧镜像不支持 `COS_SESSION_TOKEN` 环境变量

2. **兼容性**:
   - ✅ 完全向后兼容：没有 `session-token` 时自动使用永久密钥
   - ✅ 可以先更新 YAML，后续再切换到临时密钥

3. **临时密钥刷新**:
   - 临时密钥有效期通常为 2 小时
   - 需要定期刷新 Secret（建议每小时刷新一次）
   - 使用提供的脚本 `update-temp-credentials.sh` 自动化刷新

4. **故障处理**:
   - 如果临时密钥过期，会出现 403 错误
   - 快速回滚方法：更新 Secret 为永久密钥，重启 Pod

---

## 📝 总结

### 需要修改的地方

1. **Deployment/Pod YAML**: 添加 1 个环境变量（`COS_SESSION_TOKEN`）
2. **Secret**: 添加 1 个字段（`session-token`）
3. **镜像**: 更新到新版本（支持临时密钥）

### 不需要修改的地方

- ✅ 现有的 `COS_SECRET_ID` 和 `COS_SECRET_KEY` 环境变量
- ✅ 现有的 `COS_BUCKET_URL` 配置
- ✅ 其他环境变量和配置

### 推荐部署流程

1. ✅ 先更新 Deployment YAML（添加 `COS_SESSION_TOKEN` 环境变量）
2. ✅ 更新镜像到新版本
3. ✅ 重启 Pod（此时仍使用永久密钥）
4. ✅ 验证功能正常
5. ✅ 获取临时密钥并更新 Secret
6. ✅ 再次重启 Pod（开始使用临时密钥）
7. ✅ 设置定期刷新机制
