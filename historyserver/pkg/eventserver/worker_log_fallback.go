package eventserver

import (
	"fmt"
	"io"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	"github.com/sirupsen/logrus"
)

// FallbackToWorkerLogs 在 event_CORE_WORKER_*.log 为空时，从 worker-*.out 重建 Task 和 Actor 数据
func (h *EventHandler) FallbackToWorkerLogs(clusterInfo utils.ClusterInfo) {
	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	if clusterInfo.SessionName != "" {
		clusterKey = clusterKey + "_" + clusterInfo.SessionName
	}

	logrus.Infof("🔍 [FallbackToWorkerLogs] Starting fallback check for cluster: %s (Name=%s, Namespace=%s, Session=%s)",
		clusterKey, clusterInfo.Name, clusterInfo.Namespace, clusterInfo.SessionName)

	// 检查是否已经有 Task 数据
	h.ClusterTaskMap.RLock()
	taskMap, taskExists := h.ClusterTaskMap.ClusterTaskMap[clusterKey]
	h.ClusterTaskMap.RUnlock()

	taskCount := 0
	if taskExists {
		taskMap.Lock()
		taskCount = len(taskMap.TaskMap)
		taskMap.Unlock()
	}

	// 检查是否已经有 Actor 数据
	h.ClusterActorMap.RLock()
	actorMap, actorExists := h.ClusterActorMap.ClusterActorMap[clusterKey]
	h.ClusterActorMap.RUnlock()

	actorCount := 0
	if actorExists {
		actorMap.Lock()
		actorCount = len(actorMap.ActorMap)
		actorMap.Unlock()
	}

	// 只有当 tasks 和 actors 都有数据时才跳过 fallback
	// 如果任何一个为空，都应该尝试 fallback
	if taskCount > 0 && actorCount > 0 {
		logrus.Infof("✅ [FallbackToWorkerLogs] Cluster %s already has complete data (tasks=%d, actors=%d), skipping fallback", 
			clusterKey, taskCount, actorCount)
		return
	}

	logrus.Warnf("⚠️  [FallbackToWorkerLogs] Task/actor data incomplete for cluster %s (tasks=%d, actors=%d), attempting worker log fallback", 
		clusterKey, taskCount, actorCount)

	// 构建 session path
	sessionPath := clusterInfo.Name + "_" + clusterInfo.Namespace + "/" + clusterInfo.SessionName
	logrus.Infof("📂 [FallbackToWorkerLogs] Session path: %s", sessionPath)

	// 创建 worker log parser (需要传入 storage handler)
	// 注意：这里需要确保 h.reader 实现了 storage.StorageHandler 接口
	// 如果不是，可能需要调整代码

	// 简化版本：直接调用 reader 的方法来查找 worker-*.out 文件
	h.parseWorkerLogsSimple(clusterInfo, clusterKey, sessionPath)
}

// buildJobIDMapping 构建 hex job_id 到 submission_id 的映射
// 从 event_JOBS.log 中解析的 jobs 构建映射表
func (h *EventHandler) buildJobIDMapping(clusterKey string) map[string]string {
	jobIDMap := make(map[string]string)
	
	h.ClusterJobMap.RLock()
	defer h.ClusterJobMap.RUnlock()
	
	// 获取该 cluster 的所有 jobs
	jobs, exists := h.ClusterJobMap.ClusterJobMap[clusterKey]
	if !exists || len(jobs.JobMap) == 0 {
		return jobIDMap
	}
	
	// 遍历 jobs，提取 submission_id
	// 注意：Ray 的 job_id 有多种形式：
	// 1. submission_id (string, e.g., "rayjob-job-67i5m0mp-52gxs")  
	// 2. Internal hex ID (uint32/uint64, e.g., 0xd36d34d34d34)
	// event_JOBS.log 中的 job_id 字段是 submission_id
	for _, job := range jobs.JobMap {
		if job.JobID == "" {
			continue
		}
		
		// submission_id 通常就是 job_id
		submissionID := job.JobID
		
		// 尝试提取可能的十六进制形式
		// 如果 submission_id 很短且可能是十六进制，跳过
		if len(submissionID) <= 16 && isHexString(submissionID) {
			// 这可能本身就是 hex ID，不需要映射
			continue
		}
		
		// 对于 RayJob，submission_id 通常是人类可读的
		// 我们需要找到对应的十六进制 job_id
		// 但问题是：event_JOBS.log 中没有存储十六进制形式
		
		// 临时解决方案：将 submission_id 作为 key 存储
		// 在解析 worker log 时，如果提取的 hex job_id 匹配不到，
		// 就使用第一个可用的 submission_id
		jobIDMap[submissionID] = submissionID
	}
	
	return jobIDMap
}

// isHexString 检查字符串是否只包含十六进制字符
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// parseWorkerLogsSimple 简化版的 worker log 解析（直接使用 reader）
func (h *EventHandler) parseWorkerLogsSimple(clusterInfo utils.ClusterInfo, clusterKey, sessionPath string) {
	clusterNameID := clusterInfo.Name + "_" + clusterInfo.Namespace
	logDirPrefix := clusterInfo.SessionName + "/logs/"

	logrus.Infof("📁 [FallbackToWorkerLogs] Scanning logs:")
	logrus.Infof("   - clusterNameID: %s", clusterNameID)
	logrus.Infof("   - logDirPrefix: %s", logDirPrefix)
	logrus.Infof("   - Full path will be: %s + %s", clusterNameID, logDirPrefix)

	// 构建 job_id 映射表（从 event_JOBS.log 解析的 jobs）
	jobIDMap := h.buildJobIDMapping(clusterKey)
	if len(jobIDMap) > 0 {
		logrus.Infof("🔗 [FallbackToWorkerLogs] Built job ID mapping with %d entries:", len(jobIDMap))
		for hexID, submissionID := range jobIDMap {
			logrus.Infof("   - %s → %s", hexID, submissionID)
		}
	} else {
		logrus.Warnf("⚠️  [FallbackToWorkerLogs] No job ID mapping found - tasks may have incorrect job_id")
	}

	// 列出所有 node 目录
	nodeEntries := h.reader.ListFiles(clusterNameID, logDirPrefix)
	
	logrus.Infof("📊 [FallbackToWorkerLogs] ListFiles returned %d entries from path: %s", len(nodeEntries), logDirPrefix)
	
	if len(nodeEntries) == 0 {
		logrus.Errorf("❌ [FallbackToWorkerLogs] No log directories found for cluster %s at path %s", clusterKey, logDirPrefix)
		logrus.Errorf("   💡 Hint: Check if COS path is correct: %s/%s", clusterNameID, logDirPrefix)
		return
	}

	logrus.Infof("✅ [FallbackToWorkerLogs] Found %d log entries for cluster %s, listing them:",
		len(nodeEntries), clusterKey)
	for i, entry := range nodeEntries {
		logrus.Infof("   [%d] %s", i+1, entry)
	}

	taskCount := 0
	actorCount := 0
	workerFileCount := 0

	for idx, nodeEntry := range nodeEntries {
		// 只处理目录
		if len(nodeEntry) == 0 || nodeEntry[len(nodeEntry)-1] != '/' {
			logrus.Debugf("   [%d] ⏭️  Skipping non-directory: %s", idx+1, nodeEntry)
			continue
		}

		nodeIDHex := nodeEntry[:len(nodeEntry)-1] // 去掉尾部的 "/"
		nodeDirPath := logDirPrefix + nodeEntry

		logrus.Infof("🔍 [%d/%d] Processing node directory: %s", idx+1, len(nodeEntries), nodeEntry)
		logrus.Infof("   - Node ID (hex): %s", nodeIDHex)
		logrus.Infof("   - Full path: %s", nodeDirPath)

		// 列出该 node 下的所有文件
		files := h.reader.ListFiles(clusterNameID, nodeDirPath)
		logrus.Infof("   - Found %d files in this node", len(files))
		
		if len(files) == 0 {
			logrus.Warnf("   ⚠️  Node %s has no files", nodeIDHex[:12])
			continue
		}

		// 先列出所有文件
		workerFiles := []string{}
		for _, file := range files {
			if len(file) >= 11 && file[:7] == "worker-" && file[len(file)-4:] == ".out" {
				workerFiles = append(workerFiles, file)
			}
		}
		
		logrus.Infof("   - Worker-*.out files: %d", len(workerFiles))
		if len(workerFiles) > 0 {
			for i, wf := range workerFiles {
				logrus.Infof("     [%d] %s", i+1, wf)
			}
		}

		for _, file := range files {
			// 只处理 worker-*.out 文件
			if len(file) < 11 || file[:7] != "worker-" || file[len(file)-4:] != ".out" {
				continue
			}

			workerFileCount++
			logrus.Infof("📄 [FallbackToWorkerLogs] Processing worker log #%d: %s", workerFileCount, file)

			// 读取文件内容
			filePath := nodeDirPath + file
			logrus.Infof("   - Full file path: %s", filePath)
			
			reader := h.reader.GetContent(clusterNameID, filePath)
			if reader == nil {
				logrus.Errorf("   ❌ Failed to read file content")
				continue
			}
			
			logrus.Infof("   ✅ File content retrieved successfully")

			// 包装成 ReadCloser
			var readCloser io.ReadCloser
			if rc, ok := reader.(io.ReadCloser); ok {
				readCloser = rc
			} else {
				readCloser = io.NopCloser(reader)
			}

			// 解析内容并创建 Task/Actor
			parsed := h.parseWorkerLogFile(file, nodeIDHex, readCloser, clusterKey, jobIDMap)
			logrus.Infof("   - Parsed: %d tasks, %d actors", parsed.TaskCount, parsed.ActorCount)
			taskCount += parsed.TaskCount
			actorCount += parsed.ActorCount
		}
	}

	if taskCount > 0 || actorCount > 0 {
		logrus.Infof("🎉 [FallbackToWorkerLogs] Successfully reconstructed %d tasks and %d actors from %d worker log files for cluster %s",
			taskCount, actorCount, workerFileCount, clusterKey)
	} else {
		logrus.Warnf("⚠️  [FallbackToWorkerLogs] Scanned %d worker-*.out files but found no tasks or actors for cluster %s", 
			workerFileCount, clusterKey)
	}
}

// WorkerLogParseResult 解析结果
type WorkerLogParseResult struct {
	TaskCount  int
	ActorCount int
}

// parseWorkerLogFile 解析单个 worker-*.out 文件
func (h *EventHandler) parseWorkerLogFile(fileName, nodeID string, reader io.ReadCloser, clusterKey string, jobIDMap map[string]string) WorkerLogParseResult {
	defer reader.Close()

	result := WorkerLogParseResult{}

	// 读取内容
	content, err := io.ReadAll(reader)
	if err != nil {
		logrus.Warnf("[FallbackToWorkerLogs] Failed to read worker log: %v", err)
		return result
	}

	// 解析文件名：worker-{worker_id}-{job_id}-{pid}.out
	// 示例: worker-14c35918b2f1acd08af35325b5ccd7cbb8657c8ab892f66944d637dd-02000000-1106.out
	parts := fileName[7 : len(fileName)-4] // 去掉 "worker-" 和 ".out"
	fields := strings.Split(parts, "-")
	if len(fields) != 3 {
		logrus.Debugf("[FallbackToWorkerLogs] Invalid worker log filename: %s", fileName)
		return result
	}

	workerID := fields[0]
	jobID := fields[1] // 十六进制 job_id，例如 "02000000"
	// pid := fields[2]

	// 解析内容
	lines := strings.Split(string(content), "\n")
	taskNames := make(map[string]int)  // task_name -> count
	actorNames := make(map[string]bool)

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// 更新 job_id（内容中的更准确）
		if strings.HasPrefix(line, ":job_id:") {
			extractedJobID := strings.TrimPrefix(line, ":job_id:")
			if extractedJobID != "" {
				jobID = extractedJobID
			}
		}

		// 收集 task names
		if strings.HasPrefix(line, ":task_name:") {
			taskName := strings.TrimPrefix(line, ":task_name:")
			if taskName != "" {
				taskNames[taskName]++
			}
		}

		// 收集 actor names
		if strings.HasPrefix(line, ":actor_name:") {
			actorName := strings.TrimPrefix(line, ":actor_name:")
			if actorName != "" {
				actorNames[actorName] = true
			}
		}
	}

	// 尝试映射到真实的 submission_id
	// 如果 jobIDMap 只有一个条目，直接使用它（通常一个 cluster 只有一个 job）
	finalJobID := jobID
	if len(jobIDMap) == 1 {
		for _, submissionID := range jobIDMap {
			finalJobID = submissionID
			logrus.Debugf("   💡 Using submission_id from job mapping: %s (original: %s)", finalJobID, jobID)
			break
		}
	} else if submissionID, found := jobIDMap[jobID]; found {
		// 如果映射表中有匹配，使用映射的 ID
		finalJobID = submissionID
		logrus.Debugf("   💡 Mapped job_id %s → %s", jobID, finalJobID)
	}

	// 创建 Tasks
	for taskName, count := range taskNames {
		for i := 1; i <= count; i++ {
			taskID := fmt.Sprintf("%s-%s-%s-%d", finalJobID, workerID[:8], taskName, i)

			task := types.Task{
				TaskID:          taskID,
				Name:            taskName,
				JobID:           finalJobID,
				WorkerID:        workerID,
				NodeID:          nodeID,
				State:           types.FINISHED,
				Type:            types.NORMAL_TASK,
				FuncOrClassName: taskName,
				ErrorMessage:    "[Reconstructed from worker-*.out - event_CORE_WORKER_*.log unavailable]",
			}

			h.addTaskToCluster(clusterKey, &task)
			result.TaskCount++
		}
	}

	// 创建 Actors
	for actorName := range actorNames {
		actorID := fmt.Sprintf("%s-%s-%s", finalJobID, actorName, workerID[:8])

		actor := types.Actor{
			ActorID:    actorID,
			Name:       actorName,
			JobID:      finalJobID,
			State:      types.ALIVE,
			ActorClass: actorName,
			Address: types.Address{
				WorkerID: workerID,
				NodeID:   nodeID,
			},
			ExitDetails: "[Reconstructed from worker-*.out - event_CORE_WORKER_*.log unavailable]",
		}

		h.addActorToCluster(clusterKey, &actor)
		result.ActorCount++
	}

	return result
}

// addTaskToCluster 添加 Task 到 ClusterTaskMap
func (h *EventHandler) addTaskToCluster(clusterKey string, task *types.Task) {
	h.ClusterTaskMap.Lock()
	defer h.ClusterTaskMap.Unlock()

	if h.ClusterTaskMap.ClusterTaskMap == nil {
		h.ClusterTaskMap.ClusterTaskMap = make(map[string]*types.TaskMap)
	}

	taskMap, exists := h.ClusterTaskMap.ClusterTaskMap[clusterKey]
	if !exists {
		taskMap = types.NewTaskMap()
		h.ClusterTaskMap.ClusterTaskMap[clusterKey] = taskMap
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	// 添加 task（作为第一个 attempt）
	taskMap.TaskMap[task.TaskID] = []types.Task{*task}
}

// addActorToCluster 添加 Actor 到 ClusterActorMap
func (h *EventHandler) addActorToCluster(clusterKey string, actor *types.Actor) {
	h.ClusterActorMap.Lock()
	defer h.ClusterActorMap.Unlock()

	if h.ClusterActorMap.ClusterActorMap == nil {
		h.ClusterActorMap.ClusterActorMap = make(map[string]*types.ActorMap)
	}

	actorMap, exists := h.ClusterActorMap.ClusterActorMap[clusterKey]
	if !exists {
		actorMap = &types.ActorMap{
			ActorMap: make(map[string]types.Actor),
		}
		h.ClusterActorMap.ClusterActorMap[clusterKey] = actorMap
	}

	actorMap.Lock()
	defer actorMap.Unlock()

	// 添加 actor
	actorMap.ActorMap[actor.ActorID] = *actor
}
