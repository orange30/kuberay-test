# HistoryServer 增量更新逻辑梳理

## 📋 概述

HistoryServer 实现了完整的增量更新机制，可以高效地处理新增的事件文件，同时避免重复处理已经加载过的数据。

---

## 🏗️ 核心数据结构

### 1. EventHandler 结构体

```go
type EventHandler struct {
    reader storage.StorageReader
    
    // 数据存储
    ClusterTaskMap  *types.ClusterTaskMap   // 集群任务映射
    ClusterActorMap *types.ClusterActorMap  // 集群 Actor 映射
    ClusterJobMap   *types.ClusterJobMap    // 集群 Job 映射
    
    // 增量刷新相关
    processedFiles    map[string]bool       // 已处理文件集合 (key: 文件路径)
    filesMutex        sync.RWMutex          // 文件集合锁
    
    // 会话清理相关
    sessionTimestamps map[string]time.Time  // 会话时间戳 (key: clusterKey)
    sessionMutex      sync.RWMutex          // 会话时间戳锁
}
```

**关键字段说明：**
- `processedFiles`: 记录已处理的事件文件路径，避免重复处理
- `sessionTimestamps`: 记录每个会话的创建时间，用于过期清理

---

## 🔄 增量刷新逻辑

### 1. 启动流程 (Run 方法)

```go
func (h *EventHandler) Run(stop chan struct{}, numOfEventProcessors int) error {
    // 1. 创建多个事件处理器通道 (默认5个)
    eventProcessorChannels := make([]chan map[string]any, numOfEventProcessors)
    
    // 2. 启动事件处理 goroutine
    for i, currEventChannel := range eventProcessorChannels {
        go h.ProcessEvents(ctx, currEventChannel)
    }
    
    // 3. 启动文件读取循环
    go func() {
        // 3.1 启动时立即处理一次
        processAllEvents()
        
        // 3.2 获取配置间隔
        refreshInterval := getRefreshInterval()      // 默认 5 分钟
        sessionMaxAge := getSessionMaxAge()          // 默认 24 小时
        
        // 3.3 创建定时器
        refreshTicker := time.NewTicker(refreshInterval)   // 刷新定时器
        cleanupTicker := time.NewTicker(1 * time.Hour)    // 清理定时器
        
        // 3.4 进入事件循环
        for {
            select {
            case <-stop:
                return  // 收到停止信号
            case <-refreshTicker.C:
                processAllEvents()  // 定期刷新
            case <-cleanupTicker.C:
                h.cleanupExpiredSessions(sessionMaxAge)  // 定期清理
            }
        }
    }()
}
```

**执行时机：**
1. **启动时**: 立即执行一次 `processAllEvents()`
2. **定期刷新**: 每隔 `refreshInterval` 执行一次 (默认 5 分钟)
3. **定期清理**: 每隔 1 小时清理过期会话

---

### 2. 文件处理流程 (processAllEvents)

```go
processAllEvents := func() {
    // 1. 获取集群列表
    clusterList := h.reader.List()
    logrus.Infof("🔍 Found %d clusters from storage", len(clusterList))
    
    // 统计变量
    newFilesProcessed := 0    // 新处理的文件数
    skippedFiles := 0         // 跳过的文件数
    skippedOldSessions := 0   // 跳过的旧会话数
    
    for _, clusterInfo := range clusterList {
        // 2. 构建 clusterKey
        clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
        if clusterInfo.SessionName != "" {
            clusterKey = clusterKey + "_" + clusterInfo.SessionName
        }
        
        // 3. 【优化】跳过过期会话（避免加载已过期数据）
        sessionTime := time.Unix(clusterInfo.CreateTimeStamp, 0)
        if now.Sub(sessionTime) > sessionMaxAge {
            skippedOldSessions++
            continue
        }
        
        // 4. 更新会话时间戳
        h.updateSessionTimestamp(clusterKey, sessionTime)
        
        // 5. 获取事件文件列表
        eventFileList := append(
            h.getAllJobEventFiles(clusterInfo),
            h.getAllNodeEventFiles(clusterInfo)...
        )
        
        // 6. 【核心】增量处理文件
        for _, eventFile := range eventFileList {
            fullPath := clusterNameNamespace + "/" + eventFile
            
            // 6.1 检查文件是否已处理
            if h.isFileProcessed(fullPath) {
                skippedFiles++
                logrus.Debugf("Skipping already processed file: %s", eventFile)
                continue
            }
            
            // 6.2 读取新文件
            logrus.Infof("Reading new event file: %s", eventFile)
            eventioReader := h.reader.GetContent(clusterNameNamespace, eventFile)
            eventbytes, _ := io.ReadAll(eventioReader)
            
            // 6.3 解析事件
            var eventList []map[string]any
            json.Unmarshal(eventbytes, &eventList)
            
            // 6.4 分发事件到处理通道
            for i, curr := range eventList {
                curr["clusterName"] = internalParams
                eventProcessorChannels[i%numOfEventProcessors] <- curr
            }
            
            // 6.5 标记文件为已处理
            h.markFileProcessed(fullPath)
            newFilesProcessed++
        }
        
        // 7. 执行 Fallback 逻辑
        h.createJobsFromJobsLog(clusterInfo)     // 从 event_JOBS.log 创建 Jobs
        h.EnrichTasksFromLogs(clusterInfo)       // 从日志补齐 nodeId/workerId
        h.FallbackToWorkerLogs(clusterInfo)      // 从 worker-*.out 重建数据
    }
    
    // 8. 输出统计信息
    logrus.Infof("✅ Refresh complete: %d new files, %d skipped, %d old sessions skipped",
        newFilesProcessed, skippedFiles, skippedOldSessions)
}
```

---

### 3. 文件处理判断逻辑

#### 3.1 文件路径构造

```go
// 文件完整路径 = clusterNameNamespace + "/" + eventFile
// 例如: "rayjob-job-67i5m0mp-nb7vc_mce-proj-bk0lwhh1/session_2026-01-31_05-33-09/node_events/abc123-2026-01-31-05"

clusterNameNamespace := clusterInfo.Name + "_" + clusterInfo.Namespace
fullPath := clusterNameNamespace + "/" + eventFile
```

#### 3.2 文件处理状态检查

```go
// 检查文件是否已处理
func (h *EventHandler) isFileProcessed(filePath string) bool {
    h.filesMutex.RLock()
    defer h.filesMutex.RUnlock()
    return h.processedFiles[filePath]
}

// 标记文件为已处理
func (h *EventHandler) markFileProcessed(filePath string) {
    h.filesMutex.Lock()
    defer h.filesMutex.Unlock()
    h.processedFiles[filePath] = true
}
```

**关键点：**
- `processedFiles` 是一个 `map[string]bool`，key 是文件完整路径
- 使用 `RWMutex` 保证并发安全
- 一旦标记为已处理，后续刷新会直接跳过

---

## 📊 事件去重机制

### 1. Task 事件去重 (TASK_LIFECYCLE_EVENT)

```go
// 使用 (State + Timestamp) 作为唯一 key 进行去重
type eventKey struct {
    State     string
    Timestamp int64  // Unix nano timestamp
}

// 构建已存在的事件集合
existingKeys := make(map[eventKey]bool)
for _, e := range t.Events {
    existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
}

// 只追加新事件
for _, e := range stateEvents {
    key := eventKey{string(e.State), e.Timestamp.UnixNano()}
    if !existingKeys[key] {
        t.Events = append(t.Events, e)
        existingKeys[key] = true
    }
}

// 按时间戳排序
sort.Slice(t.Events, func(i, j int) bool {
    return t.Events[i].Timestamp.Before(t.Events[j].Timestamp)
})
```

### 2. Actor 事件去重 (ACTOR_LIFECYCLE_EVENT)

```go
// 逻辑同 Task，使用 (State + Timestamp) 去重
type eventKey struct {
    State     string
    Timestamp int64
}

existingKeys := make(map[eventKey]bool)
for _, e := range a.Events {
    existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
}

for _, e := range stateEvents {
    key := eventKey{string(e.State), e.Timestamp.UnixNano()}
    if !existingKeys[key] {
        a.Events = append(a.Events, e)
        existingKeys[key] = true
    }
}
```

### 3. Job 事件去重 (DRIVER_JOB_LIFECYCLE_EVENT)

```go
// 逻辑同上，使用 (State + Timestamp) 去重
type eventKey struct {
    State     string
    Timestamp int64
}
```

**去重原理：**
- 使用 `(State, Timestamp)` 作为唯一标识
- 如果同一文件被多次处理（虽然不应该发生），相同的事件会被自动去重
- 保证每个状态变化事件在时间线上唯一

---

## 🧹 会话清理机制

### 1. 清理触发

```go
// 每小时执行一次清理
cleanupTicker := time.NewTicker(1 * time.Hour)

case <-cleanupTicker.C:
    h.cleanupExpiredSessions(sessionMaxAge)  // 默认清理 24 小时前的会话
```

### 2. 清理逻辑

```go
func (h *EventHandler) cleanupExpiredSessions(maxAge time.Duration) {
    now := time.Now()
    expiredKeys := make([]string, 0)
    
    // 1. 找出过期的会话
    h.sessionMutex.RLock()
    for clusterKey, timestamp := range h.sessionTimestamps {
        if now.Sub(timestamp) > maxAge {
            expiredKeys = append(expiredKeys, clusterKey)
        }
    }
    h.sessionMutex.RUnlock()
    
    // 2. 清理过期会话的数据
    for _, clusterKey := range expiredKeys {
        // 清理 tasks
        h.ClusterTaskMap.Lock()
        delete(h.ClusterTaskMap.ClusterTaskMap, clusterKey)
        h.ClusterTaskMap.Unlock()
        
        // 清理 actors
        h.ClusterActorMap.Lock()
        delete(h.ClusterActorMap.ClusterActorMap, clusterKey)
        h.ClusterActorMap.Unlock()
        
        // 清理 jobs
        h.ClusterJobMap.Lock()
        delete(h.ClusterJobMap.ClusterJobMap, clusterKey)
        h.ClusterJobMap.Unlock()
        
        // 清理时间戳
        h.sessionMutex.Lock()
        delete(h.sessionTimestamps, clusterKey)
        h.sessionMutex.Unlock()
        
        logrus.Infof("Cleaned up expired session: %s", clusterKey)
    }
}
```

**清理策略：**
- 基于会话创建时间判断是否过期
- 默认保留 24 小时内的会话数据
- 清理包括：tasks, actors, jobs, sessionTimestamps

---

## ⚙️ 配置参数

### 1. 环境变量

| 环境变量 | 默认值 | 说明 | 示例 |
|---------|--------|------|------|
| `HISTORYSERVER_REFRESH_INTERVAL` | `5m` | 增量刷新间隔 | `1m`, `5m`, `1h` |
| `HISTORYSERVER_SESSION_MAX_AGE` | `24h` | 会话最大保留时间 | `2h`, `24h`, `7d` |

### 2. 配置解析逻辑

```go
func getRefreshInterval() time.Duration {
    defaultInterval := 5 * time.Minute
    envValue := os.Getenv("HISTORYSERVER_REFRESH_INTERVAL")
    
    if envValue == "" {
        return defaultInterval
    }
    
    // 尝试解析为 duration 字符串 (e.g., "5m", "1h")
    if duration, err := time.ParseDuration(envValue); err == nil {
        return duration
    }
    
    // 尝试解析为分钟数 (e.g., "5" 表示 5 分钟)
    if minutes, err := strconv.Atoi(envValue); err == nil && minutes > 0 {
        return time.Duration(minutes) * time.Minute
    }
    
    return defaultInterval
}
```

**支持的格式：**
- Duration 字符串: `"1m"`, `"5m"`, `"1h"`, `"24h"`
- 纯数字: `"5"` (表示 5 分钟/小时)

---

## 🔍 日志输出

### 1. 启动日志

```
[EventHandler] Configuration:
  - Refresh interval: 5m0s (env: HISTORYSERVER_REFRESH_INTERVAL)
  - Session max age: 24h0m0s (env: HISTORYSERVER_SESSION_MAX_AGE)
```

### 2. 刷新日志

```
🔍 [EventHandler] Found 6 clusters from storage
📦 [EventHandler] Processing cluster [1/6]: Name=rayjob-job-xxx, Namespace=mce-proj-xxx, Session=session_2026-01-31_05-33-09
Reading new event file: session_2026-01-31_05-33-09/node_events/abc123-2026-01-31-05
Skipping already processed file: session_2026-01-31_05-33-09/node_events/abc123-2026-01-31-04
✅ [EventHandler] Refresh complete: 3 new files processed, 15 files skipped (already processed), 0 old sessions skipped
```

### 3. 清理日志

```
[EventHandler] Running session cleanup
[EventHandler] Cleaning up 2 expired sessions (older than 24h0m0s)
[EventHandler] Cleaned up expired session: rayjob-job-old_mce-proj-xxx_session_2026-01-30_01-00-00
```

---

## 📈 性能优化

### 1. 文件级别增量

**优化点：**
- 只处理新增的事件文件
- 已处理文件完全跳过，无需读取

**效果：**
- 首次启动：处理所有文件（例如 100 个文件）
- 后续刷新：只处理新增文件（例如 5 个文件）
- 性能提升：95% 的文件被跳过

### 2. 会话级别过滤

**优化点：**
- 在加载前就过滤掉过期会话
- 避免加载和处理即将被清理的数据

**效果：**
- 减少内存占用
- 减少 CPU 处理时间

### 3. 事件级别去重

**优化点：**
- 使用 map[eventKey]bool 快速查找
- 时间复杂度 O(1)

**效果：**
- 避免重复事件导致的数据错误
- 保证数据一致性

---

## 🧪 测试验证

### 1. 增量刷新测试

```bash
# 1. 启动 historyserver
export HISTORYSERVER_REFRESH_INTERVAL="1m"
export HISTORYSERVER_SESSION_MAX_AGE="2h"
bash START_HISTORYSERVER.sh

# 2. 观察首次加载日志
tail -f /tmp/historyserver.log | grep "Refresh complete"
# 预期输出: ✅ Refresh complete: 100 new files processed, 0 files skipped

# 3. 等待 1 分钟后观察增量刷新
# 预期输出: ✅ Refresh complete: 5 new files processed, 100 files skipped

# 4. 手动触发刷新
curl -X POST http://localhost:8081/api/refresh
# 预期输出: ✅ Refresh complete: 0 new files processed, 105 files skipped
```

### 2. 会话清理测试

```bash
# 1. 设置较短的过期时间
export HISTORYSERVER_SESSION_MAX_AGE="10m"

# 2. 等待 1 小时后检查日志
tail -f /tmp/historyserver.log | grep "cleanup"
# 预期输出: [EventHandler] Cleaned up expired session: xxx
```

---

## 🎯 总结

### 核心机制

1. **文件级别增量**：
   - 使用 `processedFiles` map 记录已处理文件
   - 跳过已处理文件，只读取新文件

2. **事件级别去重**：
   - 使用 `(State, Timestamp)` 作为唯一 key
   - 自动过滤重复事件

3. **会话级别清理**：
   - 基于创建时间自动清理过期会话
   - 释放内存和存储空间

4. **定时刷新**：
   - 默认 5 分钟刷新一次
   - 支持手动触发刷新

### 优势

- ✅ **高效**: 只处理新增文件，避免重复解析
- ✅ **准确**: 事件去重保证数据一致性
- ✅ **稳定**: 自动清理过期数据，避免内存泄漏
- ✅ **灵活**: 支持环境变量配置刷新间隔和过期时间

### 适用场景

- 长时间运行的 HistoryServer
- 大量集群和会话的生产环境
- 需要实时查看最新数据的场景
- 资源受限的环境（自动清理节省内存）
