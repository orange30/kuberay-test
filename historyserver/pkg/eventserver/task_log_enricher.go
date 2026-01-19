package eventserver

import (
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// workerLogPattern matches: worker-<workerID>-<jobID>-<pid>.<timestamp>.out
// Example: worker-abc123-def456-1234.1234567890.out
var workerLogPattern = regexp.MustCompile(`^worker-([0-9a-f]+)-([0-9a-f]+)-(\d+)\.(\d+)\.(out|err)$`)

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
			if len(matches) != 6 {
				continue // Not a worker log file
			}

			workerIDHex := matches[1]
			jobIDHex := matches[2]
			// pid := matches[3] // Could be useful for future enhancements

			// Convert hex IDs to base64 (internal storage format)
			nodeIDBase64 := hexToBase64ForEnrich(nodeIDHex)
			workerIDBase64 := hexToBase64ForEnrich(workerIDHex)
			jobIDBase64 := hexToBase64ForEnrich(jobIDHex)

			if nodeIDBase64 == "" || workerIDBase64 == "" || jobIDBase64 == "" {
				continue // Conversion failed
			}

			// Find tasks matching this jobID that are missing nodeId/workerId
			enrichedCount += h.enrichTasksByJobID(sessionKey, jobIDBase64, nodeIDBase64, workerIDBase64)
		}
	}

	if enrichedCount > 0 {
		logrus.Infof("[EnrichTasksFromLogs] Enriched %d tasks with nodeId/workerId from log files", enrichedCount)
	}
}

// enrichTasksByJobID updates tasks matching jobID with nodeId/workerId from log files
func (h *EventHandler) enrichTasksByJobID(clusterKey, jobID, nodeID, workerID string) int {
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

			// Only enrich if:
			// 1. JobID matches
			// 2. NodeID or WorkerID is missing
			if task.JobID == jobID && (task.NodeID == "" || task.WorkerID == "") {
				if task.NodeID == "" {
					task.NodeID = nodeID
				}
				if task.WorkerID == "" {
					task.WorkerID = workerID
				}
				enrichedCount++
				logrus.Debugf("[EnrichTasksFromLogs] Enriched task %s (attempt %d): nodeId=%s workerId=%s",
					taskID, task.AttemptNumber, nodeID, workerID)
			}
		}
	}

	return enrichedCount
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
