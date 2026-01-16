# History Server API 实现状态清单

## 前端路由（步骤1-3已完成） ✅

| 路由 | 状态 | 说明 |
|-----|------|------|
| `GET /` | ✅ 已实现 | 根据Cookie返回homepage或Ray Dashboard |
| `GET /homepage` | ✅ 已实现 | 集群选择页 |
| `GET /static/{path:*}` | ✅ 已实现 | 静态资源服务（含安全加固） |
| `GET /logout` | ✅ 已实现 | 清除session cookies并重定向 |

---

## 集群管理 API

| 端点 | Live Session | 历史 Session | 说明 |
|-----|-------------|-------------|------|
| `GET /clusters` | ✅ 正常 | ✅ 正常 | 列出所有集群（live + 历史） |
| `GET /enter_cluster/{ns}/{name}/{session}` | ✅ 正常 | ✅ 正常 | 设置Cookie进入指定集群 |

---

## 节点相关 API

| 端点 | Live Session | 历史 Session | 缺失影响 |
|-----|-------------|-------------|----------|
| `GET /nodes?view=summary` | ✅ 代理到Head | ✅ 已实现 | 节点列表 |
| `GET /nodes/{node_id}` | ✅ 代理到Head | ❌ NotImplemented | Dashboard 单节点详情页会失败 |
| `GET /api/v0/logs?node_id=xxx` | ✅ 代理到Head | ✅ 已实现 | 列出节点日志文件 |
| `GET /api/v0/logs/file?node_id=xxx&filename=xxx` | ✅ 代理到Head | ❌ NotImplemented | **无法查看日志内容** |

**优先级**：🔴 **高**（日志查看是核心功能）

---

## 任务（Tasks）API - 已通过 EventHandler 实现 ✅

| 端点 | Live Session | 历史 Session | 说明 |
|-----|-------------|-------------|------|
| `GET /api/v0/tasks` | ✅ 代理到Head | ✅ EventHandler | 所有任务列表 |
| `GET /api/v0/tasks?filter_keys=job_id&filter_values=xxx` | ✅ 代理到Head | ✅ EventHandler | 按job筛选任务 |
| `GET /api/v0/tasks?filter_keys=task_id&filter_values=xxx` | ✅ 代理到Head | ✅ EventHandler | 查询单个任务 |
| `GET /api/v0/tasks/summarize` | ✅ 代理到Head | ✅ EventHandler | 任务统计汇总 |

---

## Actor 相关 API - 已通过 EventHandler 实现 ✅

| 端点 | Live Session | 历史 Session | 说明 |
|-----|-------------|-------------|------|
| `GET /logical/actors` | ✅ 代理到Head | ✅ EventHandler | 所有Actors列表 |
| `GET /logical/actors/{actor_id}` | ✅ 代理到Head | ✅ EventHandler | 单个Actor详情 |

---

## Job 相关 API

| 端点 | Live Session | 历史 Session | 缺失影响 |
|-----|-------------|-------------|----------|
| `GET /api/jobs` | ✅ 代理到Head | ❌ NotImplemented | **Dashboard Jobs页面无法显示** |
| `GET /api/jobs/{job_id}` | ✅ 代理到Head | ❌ NotImplemented | **单个Job详情页失败** |

**优先级**：🔴 **高**（Jobs是核心功能）

---

## 集群状态 API

| 端点 | Live Session | 历史 Session | 缺失影响 |
|-----|-------------|-------------|----------|
| `GET /api/cluster_status` | ✅ 代理到Head | ❌ NotImplemented | Dashboard Overview页autoscaler状态无法显示 |
| `GET /events` | ✅ 代理到Head | ❌ NotImplemented | 事件列表无法查看 |

**优先级**：🟡 **中**（影响部分页面）

---

## 监控相关 API

| 端点 | Live Session | 历史 Session | 缺失影响 |
|-----|-------------|-------------|----------|
| `GET /api/grafana_health` | ✅ 代理到Head | ❌ NotImplemented | Grafana集成失败 |
| `GET /api/prometheus_health` | ✅ 代理到Head | ❌ NotImplemented | Prometheus集成失败 |

**优先级**：🟢 **低**（外部监控集成，非核心）

---

## Data/Serve/Placement 相关 API

| 端点 | Live Session | 历史 Session | 缺失影响 |
|-----|-------------|-------------|----------|
| `GET /api/data/datasets/{job_id}` | ✅ 代理到Head | ❌ NotImplemented | Ray Data相关页面失败 |
| `GET /api/serve/applications/` | ✅ 代理到Head | ❌ NotImplemented | Ray Serve页面无法显示 |
| `GET /api/v0/placement_groups/` | ✅ 代理到Head | ❌ NotImplemented | Placement Groups页面失败 |

**优先级**：🟡 **中**（取决于用户是否使用这些功能）

---

## 📊 总结统计

### 历史 Session API 实现状态
- ✅ **已完成**：11个（前端路由4 + 集群2 + nodes 1 + logs 1 + tasks 2 + actors 2）
- ❌ **缺失**：11个（node详情1 + 日志内容1 + jobs 2 + 集群状态2 + 监控2 + data/serve/placement 3）

### 按优先级分类的待实现 API

#### 🔴 高优先级（影响核心功能）
1. **`GET /api/v0/logs/file`** - 日志内容查看
2. **`GET /api/jobs`** - Jobs列表
3. **`GET /api/jobs/{job_id}`** - 单个Job详情

#### 🟡 中优先级（影响部分页面）
4. `GET /nodes/{node_id}` - 单节点详情
5. `GET /api/cluster_status` - 集群状态
6. `GET /events` - 事件列表
7. `GET /api/data/datasets/{job_id}` - Datasets
8. `GET /api/serve/applications/` - Serve应用
9. `GET /api/v0/placement_groups/` - Placement Groups

#### 🟢 低优先级（外部集成）
10. `GET /api/grafana_health`
11. `GET /api/prometheus_health`

---

## 🎯 建议的实现顺序

### 阶段1：核心功能恢复（立即）
```go
// 1. 实现日志文件读取（最紧急）
func (s *ServerHandler) getNodeLogFile(...)
    // 从 storage reader 读取日志文件内容

// 2. 实现Jobs API（可能需要EventHandler支持或从存储读取）
func (s *ServerHandler) getJobs(...)
func (s *ServerHandler) getJob(...)
```

### 阶段2：完善历史数据查询（短期）
```go
// 3. 单节点详情
func (s *ServerHandler) getNode(...)

// 4. 集群状态和事件
func (s *ServerHandler) getClusterStatus(...)
func (s *ServerHandler) getEvents(...)
```

### 阶段3：高级功能（中期）
```go
// 5. Data/Serve/Placement相关
func (s *ServerHandler) getDatasets(...)
func (s *ServerHandler) getServeApplications(...)
func (s *ServerHandler) getPlacementGroups(...)
```

---

## 🔍 旧版本实现参考

旧版本（kuberay-KunWuLuan）中这些API的实现方式：
- 使用 `MetaKeyInfo()` 从对象存储读取预先收集的JSON文件
- 文件路径格式：`{rootDir}/{clusterID}/meta/{key}.json`
- 例如：
  - Jobs: `OssMetaFile_Jobs` → `meta/jobs.json`
  - Tasks: `OssMetaFile_JOBTASK_DETAIL_Prefix + jobID`
  - Actors: `OssMetaFile_LOGICAL_ACTORS`

**新版本差异**：
- ✅ 已有 EventHandler 替代了部分 meta 文件（tasks/actors）
- ❌ Jobs/ClusterStatus等仍需从存储读取或通过新机制实现

---

## ⚠️ 当前用户体验影响

### Dashboard 可用功能
- ✅ 集群选择和切换
- ✅ Live session 完整功能（通过代理）
- ✅ 历史 session 的 Tasks 页面
- ✅ 历史 session 的 Actors 页面
- ✅ 节点列表查看

### Dashboard 不可用功能
- ❌ 历史 session 无法查看日志内容
- ❌ 历史 session 无法查看 Jobs
- ❌ 部分页面会显示错误/空白

---

## 📝 下一步行动

**建议先实现高优先级API（日志 + Jobs），让历史数据查看的核心功能可用。**

你希望我现在开始实现哪些API？或者先部署当前版本测试前端是否能打开？
