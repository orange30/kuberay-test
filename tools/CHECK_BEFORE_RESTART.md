# ✅ 重启前检查清单

## 🔧 本次修复内容

### 修复了 3 个关键 Bug:

1. ✅ **FallbackToWorkerLogs 未被调用** - 已在 eventserver.go:220 添加调用
2. ✅ **触发条件逻辑错误** - 修复为只有 tasks 和 actors 都有数据时才跳过
3. ✅ **日志不足** - 添加了详细的 emoji 标记日志，易于识别

### 新增的日志标记:
- 🔍 Starting fallback check
- ⚠️  Task/actor data incomplete
- 📁 Scanning logs
- 📊 ListFiles returned X entries
- 🔍 Processing node directory
- 📄 Processing worker log file
- ✅ Success markers
- ❌ Error markers
- 🎉 Successfully reconstructed

---

## 🚀 快速重启步骤

### 方法 1: 使用自动脚本（推荐）
```bash
# 终端 1: 启动 historyserver（会自动重新编译）
./tools/restart_historyserver.sh

# 终端 2: 监控 fallback 日志
./tools/watch_fallback_logs.sh
```

### 方法 2: 手动操作
```bash
# 1. 重新编译
cd /Users/zhikuodu/work/work_ray/kuberay-test
make historyserver

# 2. 杀掉旧进程
pkill -f "historyserver.*--port=8081"

# 3. 启动新进程
./output/bin/historyserver \
  --runtime-class-name=cos \
  --ray-root-dir=mce-proj-bk0lwhh1 \
  --dashboard-dir=./dashboard \
  --port=8081 2>&1 | tee /tmp/historyserver.log
```

---

## 🔍 启动后立即检查

### 1. 检查 fallback 是否被触发
在启动日志中查找:
```
🔍 [FallbackToWorkerLogs] Starting fallback check
```

**如果没有看到**:
- ❌ 说明函数没有被调用（但我们已经添加了调用，应该不会）
- 检查编译是否成功: `ls -lh output/bin/historyserver`

### 2. 检查是否找到了 worker-*.out 文件
查找:
```
📊 [FallbackToWorkerLogs] ListFiles returned X entries
📄 [FallbackToWorkerLogs] Processing worker log #1: worker-xxx.out
```

**如果看到 "0 entries"**:
- ❌ COS 路径问题
- 检查 clusterNameID 和 sessionName 是否正确
- 使用 `cosutil ls` 验证路径

### 3. 检查最终结果
查找:
```
🎉 [FallbackToWorkerLogs] Successfully reconstructed X tasks and Y actors
```

**如果看到 "0 tasks and 0 actors"**:
- ⚠️  找到了 worker-*.out 但没有解析出数据
- 可能文件内容格式不符合预期
- 需要检查文件内容

---

## 📋 关键日志示例

### ✅ 成功的日志流程:
```
INFO[0001] 🔍 [FallbackToWorkerLogs] Starting fallback check for cluster: rayjob-xxx
INFO[0001] ⚠️  [FallbackToWorkerLogs] Task/actor data incomplete (tasks=0, actors=0), attempting fallback
INFO[0001] 📁 [FallbackToWorkerLogs] Scanning logs:
INFO[0001]    - clusterNameID: rayjob-xxx_default
INFO[0001]    - logDirPrefix: session_2026-01-31_22-00-00_123456/logs/
INFO[0001] 📊 [FallbackToWorkerLogs] ListFiles returned 2 entries
INFO[0001]    [1] 9da758bb.../
INFO[0001]    [2] e12103b4.../
INFO[0001] 🔍 [1/2] Processing node directory: 9da758bb.../
INFO[0001]    - Found 5 files in this node
INFO[0001]    - Worker-*.out files: 2
INFO[0001]      [1] worker-xxx-02000000-1106.out
INFO[0001]      [2] worker-xxx-02000000-1107.out
INFO[0001] 📄 [FallbackToWorkerLogs] Processing worker log #1: worker-xxx-02000000-1106.out
INFO[0001]    ✅ File content retrieved successfully
INFO[0001]    - Parsed: 5 tasks, 1 actors
INFO[0002] 🎉 [FallbackToWorkerLogs] Successfully reconstructed 10 tasks and 2 actors from 2 worker log files
```

### ❌ 失败的情况:

#### 情况 1: COS 路径错误
```
INFO[0001] 📊 [FallbackToWorkerLogs] ListFiles returned 0 entries
ERROR[0001] ❌ [FallbackToWorkerLogs] No log directories found at path session_xxx/logs/
ERROR[0001]    💡 Hint: Check if COS path is correct
```
**解决**: 检查 `--ray-root-dir` 参数和 session name

#### 情况 2: 找不到 worker-*.out 文件
```
INFO[0001] 📊 [FallbackToWorkerLogs] ListFiles returned 2 entries
INFO[0001] 🔍 [1/2] Processing node directory: 9da758bb.../
INFO[0001]    - Found 10 files in this node
INFO[0001]    - Worker-*.out files: 0
WARN[0001] ⚠️  [FallbackToWorkerLogs] Scanned 0 worker-*.out files but found no tasks or actors
```
**解决**: 检查 Ray Job 是否配置了 worker log 输出重定向

#### 情况 3: 文件读取失败
```
INFO[0001] 📄 [FallbackToWorkerLogs] Processing worker log #1: worker-xxx.out
ERROR[0001]    ❌ Failed to read file content
```
**解决**: 检查 COS 权限（SecretID/SecretKey）

---

## 🧪 测试 API 是否有数据

### 等待 1-2 秒后测试:
```bash
# 测试 tasks API
curl -s "http://localhost:8081/api/v0/tasks?limit=10" | jq '.data.summary.numTasks'

# 测试 actors API  
curl -s "http://localhost:8081/logical/actors" | jq '.data.actors | length'
```

**预期结果**:
- 如果 fallback 成功: 返回 > 0 的数量
- 如果 fallback 失败: 返回 0 或 null

---

## 🐛 如果还是没有数据

### 调试步骤:

1. **检查完整日志**
   ```bash
   cat /tmp/historyserver.log | grep -A 10 "FallbackToWorkerLogs"
   ```

2. **验证 COS 路径**
   ```bash
   ./tools/debug_cluster_key.sh
   ```

3. **手动检查 worker log 文件**
   ```bash
   # 列出 COS 中的文件
   cosutil ls -r cos://mce-proj-bk0lwhh1/rayjob-xxx/session_xxx/logs/
   
   # 下载一个 worker-*.out 文件查看内容
   cosutil cp cos://mce-proj-bk0lwhh1/.../worker-xxx.out /tmp/test.out
   cat /tmp/test.out | grep -E ":job_id:|:task_name:|:actor_name:"
   ```

4. **检查 clusterInfo 和 sessionName**
   在启动日志中查找:
   ```
   Starting fallback check for cluster: XXX (Name=YYY, Namespace=ZZZ, Session=AAA)
   ```
   验证这些值是否正确

---

## 📞 需要帮助?

提供以下信息:
1. ✅ 完整的 historyserver 启动日志（尤其是包含 FallbackToWorkerLogs 的部分）
2. ✅ `./tools/debug_cluster_key.sh` 的输出
3. ✅ COS 中实际的文件列表（`cosutil ls`）
4. ✅ 一个 worker-*.out 文件的内容示例

---

**最后更新**: 2026-01-31
**修复版本**: v1.2.0-fallback-fixed
