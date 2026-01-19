# History Server 本地开发指南

## 快速开始

### 方式 1: 使用启动脚本（推荐）

```bash
# 1. 编译二进制文件
make buildhistoryserver

# 2. 构建 Dashboard（如果还没构建）
cd dashboard/v2.51.0/client
npm ci
npm run build
cd ../../..

# 3. 修改 run-local.sh 中的 S3_ENDPOINT 为你的 MinIO 地址
vim run-local.sh

# 4. 运行
chmod +x run-local.sh
./run-local.sh
```

### 方式 2: 手动设置环境变量

```bash
# 设置 S3/MinIO 连接参数
export S3DISABLE_SSL="true"
export AWS_S3ID="minioadmin"
export AWS_S3SECRET="minioadmin"
export AWS_S3TOKEN=""
export S3_BUCKET="ray-historyserver"
export S3_ENDPOINT="192.168.129.137:30900"  # 修改为你的地址
export S3_REGION="test"
export S3FORCE_PATH_STYLE="true"

# 启动服务
./output/bin/historyserver \
    --runtime-class-name=s3 \
    --ray-root-dir=log \
    --dashboard-dir=./dashboard
```

### 方式 3: 使用 JSON 配置文件

```bash
# 1. 编辑配置文件
cat > local-config.json <<EOF
{
  "s3Bucket": "ray-historyserver",
  "s3Endpoint": "192.168.129.137:30900",
  "s3Region": "test",
  "s3ForcePathStyle": "true",
  "s3DisableSSL": "true"
}
EOF

# 2. 设置 AWS 凭证（仍需环境变量）
export AWS_S3ID="minioadmin"
export AWS_S3SECRET="minioadmin"
export AWS_S3TOKEN=""

# 3. 启动服务
./output/bin/historyserver \
    --runtime-class-name=s3 \
    --ray-root-dir=log \
    --dashboard-dir=./dashboard \
    --runtime-class-config-path=./local-config.json
```

## 配置参数说明

### 命令行参数

| 参数 | 说明 | 默认值 | 必需 |
|------|------|--------|------|
| `--runtime-class-name` | 存储后端类型 | - | ✅ 是 |
| `--ray-root-dir` | Ray 日志在存储中的根路径 | - | ✅ 是 |
| `--dashboard-dir` | Dashboard 静态文件目录 | `/dashboard` | ❌ 否 |
| `--runtime-class-config-path` | JSON 配置文件路径 | - | ❌ 否 |
| `--kubeconfigs` | Kubernetes 配置文件路径（如需访问 K8s API） | - | ❌ 否 |

**`runtime-class-name` 支持的值:**
- `s3` - Amazon S3 或兼容 S3 的存储（如 MinIO）
- `aliyun` - 阿里云 OSS
- 其他后端需查看 `pkg/collector/registry.go`

### 环境变量（S3 后端）

| 环境变量 | 说明 | 示例值 | 必需 |
|----------|------|--------|------|
| `AWS_S3ID` | S3 Access Key ID | `minioadmin` | ✅ 是 |
| `AWS_S3SECRET` | S3 Secret Access Key | `minioadmin` | ✅ 是 |
| `AWS_S3TOKEN` | S3 Session Token（临时凭证） | `""` | ❌ 否 |
| `S3_BUCKET` | S3 存储桶名称 | `ray-historyserver` | ❌ 否 |
| `S3_ENDPOINT` | S3 Endpoint（MinIO 必须） | `minio.example.com:9000` | ✅ 是（MinIO） |
| `S3_REGION` | S3 区域 | `us-east-1` | ✅ 是 |
| `S3FORCE_PATH_STYLE` | 使用路径样式 URL（MinIO 必须） | `true` | ✅ 是（MinIO） |
| `S3DISABLE_SSL` | 禁用 SSL（开发环境） | `true` | ❌ 否 |

### JSON 配置文件格式

```json
{
  "s3Bucket": "ray-historyserver",
  "s3Endpoint": "192.168.129.137:30900",
  "s3Region": "test",
  "s3ForcePathStyle": "true",
  "s3DisableSSL": "true"
}
```

**优先级**: JSON 配置文件 > 环境变量 > 默认值

**注意**: AWS 凭证（`AWS_S3ID`, `AWS_S3SECRET`）**只能通过环境变量设置**，不能在 JSON 文件中配置。

## 目录结构要求

```
historyserver/
├── output/
│   └── bin/
│       └── historyserver          # 编译的二进制文件
├── dashboard/                     # Dashboard 静态文件
│   ├── v2.51.0/
│   │   └── client/
│   │       └── build/             # npm run build 生成的前端资源
│   │           ├── index.html
│   │           └── static/
│   └── homepage/
│       └── index.html
├── local-config.json              # 可选：配置文件
└── run-local.sh                   # 启动脚本
```

## 常见问题

### 1. 错误: "Failed to read runtime class config"

**原因**: 指定了 `--runtime-class-config-path` 但文件不存在或格式错误

**解决**:
```bash
# 检查文件是否存在
ls -lh local-config.json

# 验证 JSON 格式
cat local-config.json | jq .
```

### 2. 错误: "Not supported runtime class name"

**原因**: `--runtime-class-name` 参数值不支持

**解决**: 查看支持的后端类型
```bash
grep -r "registry\[" pkg/collector/registry.go
```

当前支持: `s3`, `aliyun`

### 3. 页面显示空白或 404

**原因**: Dashboard 未构建或路径配置错误

**解决**:
```bash
# 构建 Dashboard
cd dashboard/v2.51.0/client
npm ci
npm run build

# 检查构建产物
ls -lh build/

# 确认启动时指定了正确的路径
./output/bin/historyserver --dashboard-dir=./dashboard ...
```

### 4. 无法连接 MinIO

**症状**: 日志显示 connection refused 或 timeout

**解决**:
```bash
# 1. 检查 MinIO 是否可访问
curl http://192.168.129.137:30900

# 2. 如果在 Docker 内访问宿主机 MinIO，使用 host.docker.internal
export S3_ENDPOINT="host.docker.internal:30900"

# 3. 检查防火墙规则
sudo iptables -L -n | grep 30900
```

### 5. S3 认证失败

**症状**: 403 Forbidden 或 InvalidAccessKeyId

**解决**:
```bash
# 验证凭证
echo $AWS_S3ID
echo $AWS_S3SECRET

# 测试 MinIO 连接（使用 mc 客户端）
mc alias set local http://192.168.129.137:30900 minioadmin minioadmin
mc ls local/ray-historyserver
```

## 开发技巧

### 热重载（需要手动）

1. 修改 Go 代码后重新编译:
   ```bash
   make buildhistoryserver
   ```

2. 停止旧进程（Ctrl+C）并重新运行:
   ```bash
   ./run-local.sh
   ```

### Dashboard 前端开发

如需修改 Dashboard 前端:
```bash
cd dashboard/v2.51.0/client

# 开发模式（热重载）
npm start

# 构建生产版本
npm run build
```

### 查看详细日志

修改 `pkg/historyserver/server.go` 中的日志级别:
```go
logrus.SetLevel(logrus.DebugLevel)
```

### 使用 VS Code 调试

创建 `.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Launch History Server",
      "type": "go",
      "request": "launch",
      "mode": "debug",
      "program": "${workspaceFolder}/cmd/historyserver",
      "args": [
        "--runtime-class-name=s3",
        "--ray-root-dir=log",
        "--dashboard-dir=${workspaceFolder}/dashboard"
      ],
      "env": {
        "S3DISABLE_SSL": "true",
        "AWS_S3ID": "minioadmin",
        "AWS_S3SECRET": "minioadmin",
        "S3_BUCKET": "ray-historyserver",
        "S3_ENDPOINT": "192.168.129.137:30900",
        "S3_REGION": "test",
        "S3FORCE_PATH_STYLE": "true"
      }
    }
  ]
}
```

## 访问服务

启动后访问: **http://localhost:8080**

- 首页: `/`
- API: `/api/v0/*`
- 健康检查: `/livez`, `/readz`

## 与 K8s 部署的区别

| 项目 | K8s 部署 | 本地开发 |
|------|----------|----------|
| Dashboard 路径 | `/dashboard` (容器根目录) | `./dashboard` (相对路径) |
| 配置来源 | 环境变量 (K8s Env) | 环境变量 / JSON 文件 |
| 端口 | 8080 (ClusterIP/NodePort) | 8080 (localhost) |
| MinIO 地址 | K8s Service DNS | 外部 IP:Port |
| 热重载 | 需重新构建镜像 + Pod | 重新编译 + 重启进程 |
