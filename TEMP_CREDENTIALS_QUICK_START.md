# 临时密钥快速启动指南 🚀

## 📋 一句话总结

**只需在 Deployment 中添加 1 个环境变量，即可支持临时密钥！**

---

## ✅ 代码修改（已完成）

- ✅ 修改了 2 个文件，新增约 4 行代码
- ✅ 完全向后兼容，支持永久密钥和临时密钥
- ✅ 无需修改其他代码

---

## 🔧 Kubernetes 配置修改

### 只需添加 1 个环境变量：

```yaml
spec:
  containers:
  - name: historyserver
    env:
    # ... 现有配置保持不变 ...
    
    # 🆕 新增这一个环境变量
    - name: COS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          key: session-token
          name: cos-secret-proj-production-009
          optional: true
```

---

## 🚀 快速部署命令

```bash
# 1. 添加环境变量
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

# 2. 更新镜像（假设新镜像已构建）
kubectl set image deployment/historyserver-demo \
  historyserver=ccr.ccs.tencentyun.com/use-test/ray:historyserver-m030 \
  -n mce-proj-production-009

# 3. 获取并更新临时密钥
export ROLE_ARN="qcs::cam::uin/你的账号ID:roleName/你的角色名"
bash historyserver/scripts/update-temp-credentials.sh
```

---

## 📦 Secret 配置

### 使用临时密钥：

```bash
# 从腾讯云 STS 获取临时密钥后
kubectl create secret generic cos-secret-proj-production-009 \
  --from-literal=secret-id="临时SecretID" \
  --from-literal=secret-key="临时SecretKey" \
  --from-literal=session-token="临时Token" \
  -n mce-proj-production-009 \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 或者继续使用永久密钥（兼容）：

```bash
# 不需要修改现有 Secret
# 代码会自动判断：有 session-token 就用临时密钥，没有就用永久密钥
```

---

## ✅ 验证

```bash
# 1. 检查环境变量
POD_NAME=$(kubectl get pod -n mce-proj-production-009 -l app=historyserver -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n mce-proj-production-009 $POD_NAME -- env | grep COS_SESSION_TOKEN

# 2. 查看日志（应该看到集群列表，无 403 错误）
kubectl logs -n mce-proj-production-009 $POD_NAME | grep -E "Found.*clusters|403"

# 3. 测试 API
kubectl run -it --rm debug --image=alpine --restart=Never -n mce-proj-production-009 -- \
  sh -c "apk add curl && curl -s http://historyserver-demo:8080/clusters/"
```

---

## 📚 详细文档

| 文档 | 位置 | 说明 |
|------|------|------|
| **认证分析** | `docs/COS_AUTHENTICATION_ANALYSIS.md` | 技术细节和原理 |
| **部署指南** | `docs/DEPLOY_WITH_TEMP_CREDENTIALS.md` | 完整部署步骤 |
| **YAML 修改** | `docs/YAML_MODIFICATION_SUMMARY.md` | 配置修改说明 |
| **实现总结** | `docs/TEMP_CREDENTIALS_IMPLEMENTATION_SUMMARY.md` | 完整实现总结 |

---

## ⚠️ 重要提醒

1. **临时密钥有效期**: 默认 2 小时，需要定期刷新
2. **自动化脚本**: 使用 `historyserver/scripts/update-temp-credentials.sh`
3. **兼容性**: ✅ 完全兼容永久密钥，随时可以回滚

---

## 🎯 核心优势

- 🔐 **更安全**: 临时密钥有时效性
- 🔐 **最小权限**: 可以限制到只读权限
- ✅ **向后兼容**: 支持永久密钥和临时密钥
- 🚀 **简单实施**: 只需修改 1 个环境变量

---

**有问题？查看详细文档或运行自动化脚本！**
