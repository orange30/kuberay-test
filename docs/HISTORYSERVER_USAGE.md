# KubeRay HistoryServer 使用说明

## 功能概述

HistoryServer 提供以下功能：
- ✅ 从 COS 读取历史集群数据
- ✅ Worker Log Fallback 机制（当 event_CORE_WORKER_*.log 缺失时自动从 worker-*.out 重建 tasks 和 actors）
- ✅ Web Dashboard 可视化界面
- ✅ Tasks/Actors 数据展示
- ✅ 日志查看功能

## 快速启动

### 1. 设置环境变量

```bash
export COS_SECRET_ID="your_secret_id"
export COS_SECRET_KEY="your_secret_key"
```

### 2. 启动 HistoryServer

```bash
cd /Users/zhikuodu/work/work_ray/kuberay-test
bash START_HISTORYSERVER.sh
```

启动后会显示：
```
✅ HistoryServer is ready!
   PID: xxxxx
   Port: 8081
   URL: http://localhost:8081
```

### 3. 访问 Dashboard

打开浏览器访问：http://localhost:8081

## API 使用

### 获取集群列表

```bash
curl http://localhost:8081/clusters/ | jq
```

### 获取 Tasks 数据

需要先设置 Cookie（从前端进入集群后自动设置）：

```bash
curl --cookie "cluster_name=rayjob-job-xxx; cluster_namespace=mce-proj-bk0lwhh1; session_name=session_xxx" \
  http://localhost:8081/api/v0/tasks?limit=10 | jq
```

### 获取日志

#### Job Driver 日志

```bash
curl --cookie "cluster_name=xxx; cluster_namespace=xxx; session_name=xxx" \
  "http://localhost:8081/api/v0/logs/file?submission_id=rayjob-job-xxx&lines=100"
```

#### Task 日志

```bash
curl --cookie "cluster_name=xxx; cluster_namespace=xxx; session_name=xxx" \
  "http://localhost:8081/api/v0/logs/file?task_id=TASK_ID_HEX&suffix=out&lines=100"
```

#### Actor 日志

```bash
curl --cookie "cluster_name=xxx; cluster_namespace=xxx; session_name=xxx" \
  "http://localhost:8081/api/v0/logs/file?actor_id=ACTOR_ID&suffix=out&lines=100"
```

#### 通过 node_id + filename 获取日志

```bash
curl --cookie "cluster_name=xxx; cluster_namespace=xxx; session_name=xxx" \
  "http://localhost:8081/api/v0/logs/file?node_id=NODE_ID_HEX&filename=job-driver-xxx.log&lines=100"
```

## 测试脚本

运行完整的日志功能测试：

```bash
bash TEST_LOGS_API.sh
```

## 关键配置

### COS 配置文件格式

HistoryServer 自动创建配置文件 `/tmp/historyserver-cos-config.json`：

```json
{
  "cosBucketURL": "https://your-bucket.cos.ap-guangzhou.myqcloud.com"
}
```

### 启动参数

```bash
./output/bin/historyserver \
  --runtime-class-name=cos \
  --runtime-class-config-path=/tmp/historyserver-cos-config.json \
  --ray-root-dir=mce-proj-bk0lwhh1 \
  --dashboard-dir=/path/to/kuberay-test/historyserver/dashboard \
  --port=8081
```

## Worker Log Fallback 机制

当检测到集群的 tasks 和 actors 数据为空时，会自动：

1. 扫描 `logs/` 目录下的所有 node 目录
2. 查找 `worker-*.out` 文件
3. 解析日志文件，提取 task 和 actor 信息
4. 从 `event_JOBS.log` 获取 job_id 映射
5. 重建 tasks 和 actors 数据结构

日志示例：
```
🔍 [FallbackToWorkerLogs] Starting fallback check for cluster: xxx
⚠️  [FallbackToWorkerLogs] Task/actor data incomplete (tasks=0, actors=0), attempting worker log fallback
📄 [FallbackToWorkerLogs] Processing worker log #1: worker-xxx-02000000-1161.out
🎉 [FallbackToWorkerLogs] Successfully reconstructed 10 tasks and 2 actors from 5 worker log files
```

## 常见问题

### 1. "No clusters found"

**原因**：COS 认证失败或路径配置错误

**解决**：
- 检查 `COS_SECRET_ID` 和 `COS_SECRET_KEY` 是否正确
- 检查 `--ray-root-dir` 参数是否匹配 COS 中的实际路径
- 查看日志中的错误信息：`tail -f /tmp/historyserver.log`

### 2. "Cluster Cookie not found"

**原因**：API 请求缺少必需的 Cookie

**解决**：
- 通过浏览器访问 Dashboard，进入集群后 Cookie 会自动设置
- 使用 curl 时手动添加 `--cookie` 参数

### 3. Dashboard HTML 文件找不到

**原因**：启动时的工作目录不正确，或使用了相对路径

**解决**：
- 使用 `START_HISTORYSERVER.sh` 脚本启动（使用绝对路径）
- 确保在项目根目录执行启动命令

### 4. 前端日志显示 "Failed to load"

**原因**：前端在某些页面跳转时未正确传递 Cookie

**解决**：
- 刷新页面或重新进入集群
- 使用 API 直接获取日志（参考上面的 curl 命令）
- 等待前端 Cookie 传递逻辑修复

## 停止服务

```bash
# 方法 1：使用 PID
kill <PID>

# 方法 2：杀死所有 historyserver 进程
pkill -f "historyserver.*--runtime-class-name"
```

## 日志文件位置

- **HistoryServer 日志**: `/tmp/historyserver.log`
- **COS 配置文件**: `/tmp/historyserver-cos-config.json`

查看实时日志：
```bash
tail -f /tmp/historyserver.log
```

## 架构说明

```
┌─────────────────────────────────────────────────────────┐
│                    Browser (Frontend)                   │
│         http://localhost:8081 (Dashboard UI)            │
└──────────────────────────┬──────────────────────────────┘
                           │
                           │ HTTP + Cookies
                           ▼
┌─────────────────────────────────────────────────────────┐
│              HistoryServer (Backend)                    │
│  - Web Server (port 8081)                               │
│  - EventHandler (processes events & logs)               │
│  - Worker Log Fallback (reconstructs missing data)      │
└──────────────────────────┬──────────────────────────────┘
                           │
                           │ COS SDK
                           ▼
┌─────────────────────────────────────────────────────────┐
│         Tencent Cloud Object Storage (COS)              │
│  - mce-proj-bk0lwhh1/                                   │
│    ├── session_xxx/                                     │
│    │   ├── metadir/                                     │
│    │   └── logs/                                        │
│    │       ├── <node_id_hex>/                           │
│    │       │   ├── events/                              │
│    │       │   │   └── event_JOBS.log                   │
│    │       │   ├── worker-*.out                         │
│    │       │   └── job-driver-*.log                     │
└─────────────────────────────────────────────────────────┘
```

## 相关文件

- `START_HISTORYSERVER.sh` - 启动脚本
- `TEST_LOGS_API.sh` - 日志 API 测试脚本
- `historyserver/pkg/eventserver/worker_log_fallback.go` - Worker Log Fallback 实现
- `historyserver/pkg/historyserver/router.go` - API 路由定义

## 维护建议

1. **定期清理日志**：`/tmp/historyserver.log` 会持续增长
2. **监控 COS 访问**：大量集群可能产生较多 API 调用
3. **检查内存使用**：Worker Log Fallback 会在内存中重建数据
4. **备份配置文件**：保存 COS 凭证和配置

## 更新日志

- **2026-01-31**: 
  - ✅ 修复 Dashboard HTML 路径问题（使用绝对路径）
  - ✅ 修复 COS 认证问题（正确配置 SECRET_KEY）
  - ✅ 验证日志 API 功能正常
  - ✅ 创建使用说明文档
