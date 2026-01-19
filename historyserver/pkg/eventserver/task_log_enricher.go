package eventserver

import (
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// workerLogPattern matches: python-core-worker-<workerID_hex>_<pid>.log
// Example: python-core-worker-99fb9107e3a872fe769fb61d87e3781eb226f3241f269185b3154399_3944.log
var workerLogPattern = regexp.MustCompile(`^python-core-worker-([0-9a-f]+)_(\d+)\.log$`)

// EnrichTasksFromLogs scans log files to infer missing task metadata (nodeId, workerId)
// This is a fallback when TASK_LIFECYCLE_EVENT has empty nodeId/workerId fields.
func (h *EventHandler) EnrichTasksFromLogs(clusterInfo utils.ClusterInfo) {
	clusterNameID := clusterInfo.Name + "_" + clusterInfo.Namespace
	sessionKey := clusterNameID + "_" + clusterInfo.SessionName

	logDirPrefix := clusterInfo.SessionName + "/logs/"

	// List all node directories under logs/
	nodeEntries := h.reader.ListFiles(clusterNameID, logDirPrefix)

	logrus.Debugf("[EnrichTasksFromLogs] cluster=%s session=%s, found %d log entries",
		clusterNameID, clusterInfo.SessionName, len(nodeEntries))

	// Track how many tasks we enriched
	enrichedCount := 0

	for _, nodeEntry := range nodeEntries {
		// Skip files (we only want directories like "e3e96dcda00dc1f3.../")
		if !strings.HasSuffix(nodeEntry, "/") {
			continue
		}

		// Extract nodeID from directory name (remove trailing /)
		nodeIDHex := strings.TrimSuffix(nodeEntry, "/")

		// List worker log files in this node directory
		nodeDirPath := logDirPrefix + nodeEntry
		logFiles := h.reader.ListFiles(clusterNameID, nodeDirPath)

		for _, logFile := range logFiles {
			// Skip directories
			if strings.HasSuffix(logFile, "/") {
				continue
			}

			// Parse worker log filename
			matches := workerLogPattern.FindStringSubmatch(logFile)
			if len(matches) != 3 {
				continue // Not a worker log file
			}

			workerIDHex := matches[1]
			// pid := matches[2] // Could be useful for future enhancements

			// Convert hex IDs to base64 (internal storage format)
			nodeIDBase64 := hexToBase64ForEnrich(nodeIDHex)
			workerIDBase64 := hexToBase64ForEnrich(workerIDHex)

			if nodeIDBase64 == "" || workerIDBase64 == "" {
				continue // Conversion failed
			}

			// Enrich all tasks on this node+worker that are missing nodeId/workerId
			// Note: We can't infer jobID from filename anymore, so we iterate all tasks in session
			enrichedCount += h.enrichTasksByNodeWorker(sessionKey, nodeIDBase64, workerIDBase64)
		}
	}

	if enrichedCount > 0 {
		logrus.Infof("[EnrichTasksFromLogs] Enriched %d tasks with nodeId/workerId from log files", enrichedCount)
	}
}

// enrichTasksByNodeWorker finds tasks with missing nodeId/workerId and fills them
func (h *EventHandler) enrichTasksByNodeWorker(clusterKey, nodeID, workerID string) int {
	h.ClusterTaskMap.RLock()
	taskMap, ok := h.ClusterTaskMap.ClusterTaskMap[clusterKey]
	h.ClusterTaskMap.RUnlock()

	if !ok {
		return 0
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	enrichedCount := 0

	for taskID, attempts := range taskMap.TaskMap {
		for i := range attempts {
			task := &attempts[i]

			// Only enrich if WorkerID is missing (fill both node and worker together)
			if task.WorkerID == "" {
				task.NodeID = nodeID
				task.WorkerID = workerID
				enrichedCount++
				logrus.Debugf("[EnrichTasksFromLogs] Enriched task %s (attempt %d): nodeId=%s workerId=%s",
					taskID, task.AttemptNumber, nodeID[:min(6, len(nodeID))], workerID[:min(6, len(workerID))])
			}
		}
	}

	return enrichedCount
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// hexToBase64ForEnrich converts hex ID to base64 (used by enricher)
func hexToBase64ForEnrich(hexID string) string {
	if hexID == "" {
		return ""
	}

	decoded, err := hex.DecodeString(hexID)
	if err != nil {
		return ""
	}

	// Use standard base64 encoding (with padding)
	// This should match the encoding used in Ray events
	return base64.StdEncoding.EncodeToString(decoded)
}
