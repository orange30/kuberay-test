package eventserver

import (
	"encoding/base64"
	"encoding/hex"
	"io"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// mockStorageForEnrich provides test data for log enrichment
type mockStorageForEnrich struct {
	files map[string][]string // prefix -> filenames
}

func (m *mockStorageForEnrich) ListFiles(clusterNameID, prefix string) []string {
	return m.files[prefix]
}

func (m *mockStorageForEnrich) GetContent(clusterNameID, filePath string) io.Reader {
	return nil // Not needed for enrichment tests
}

func (m *mockStorageForEnrich) List() []utils.ClusterInfo {
	return nil
}

func TestEnrichTasksFromLogs(t *testing.T) {
	// Setup mock storage with log files
	mock := &mockStorageForEnrich{
		files: make(map[string][]string),
	}

	// Simulate log directory structure:
	// session/logs/
	//   -> e12103b473d520863b8098c6995121fde78ce0ef6ba47ad555c9865e/
	//      -> python-core-worker-99fb9107e3a872fe769fb61d87e3781eb226f3241f269185b3154399_3944.log
	nodeIDHex := "e12103b473d520863b8098c6995121fde78ce0ef6ba47ad555c9865e"
	workerIDHex := "99fb9107e3a872fe769fb61d87e3781eb226f3241f269185b3154399"

	mock.files["session_test/logs/"] = []string{nodeIDHex + "/"}
	mock.files["session_test/logs/"+nodeIDHex+"/"] = []string{
		"python-core-worker-" + workerIDHex + "_3944.log",
	}

	// Create EventHandler with test tasks
	h := NewEventHandler(mock)
	clusterKey := "test-cluster_default_session_test"

	// Add a task with missing nodeId/workerId
	taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(clusterKey)
	taskMap.CreateOrMergeAttempt("task1", 0, func(t *types.Task) {
		t.TaskID = "task1"
		t.JobID = "job123"
		t.NodeID = ""   // Missing - should be enriched
		t.WorkerID = "" // Missing - should be enriched
	})

	// Run enrichment
	clusterInfo := utils.ClusterInfo{
		Name:        "test-cluster",
		Namespace:   "default",
		SessionName: "session_test",
	}
	h.EnrichTasksFromLogs(clusterInfo)

	// Verify task was enriched
	tasks := h.GetTasks(clusterKey)
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}

	task := tasks[0]
	if task.NodeID == "" {
		t.Errorf("NodeID was not enriched")
	}
	if task.WorkerID == "" {
		t.Errorf("WorkerID was not enriched")
	}

	// Verify the values match expected base64-encoded IDs
	expectedNodeID := base64.StdEncoding.EncodeToString(hexDecode(t, nodeIDHex))
	expectedWorkerID := base64.StdEncoding.EncodeToString(hexDecode(t, workerIDHex))

	if task.NodeID != expectedNodeID {
		t.Errorf("NodeID mismatch: got %s, want %s", task.NodeID, expectedNodeID)
	}
	if task.WorkerID != expectedWorkerID {
		t.Errorf("WorkerID mismatch: got %s, want %s", task.WorkerID, expectedWorkerID)
	}

	t.Logf("✅ Task enriched: nodeId=%s... workerId=%s...", task.NodeID[:6], task.WorkerID[:6])
}

func hexDecode(t *testing.T, hexStr string) []byte {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("Failed to decode hex: %v", err)
	}
	return b
}
