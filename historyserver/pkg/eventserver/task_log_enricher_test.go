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
	//   -> abc123/
	//      -> worker-def456-aabbccdd-1000.1234567.out
	mock.files["session_test/logs/"] = []string{"abc123/"}
	mock.files["session_test/logs/abc123/"] = []string{
		"worker-def456-aabbccdd-1000.1234567.out",
		"worker-def456-aabbccdd-1000.1234567.err",
	}
	
	// Create EventHandler with test tasks
	h := NewEventHandler(mock)
	clusterKey := "test-cluster_default_session_test"
	
	// Add a task with matching jobID but missing nodeId/workerId
	jobIDBase64 := base64.StdEncoding.EncodeToString(hexDecode(t, "aabbccdd"))
	
	taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(clusterKey)
	taskMap.CreateOrMergeAttempt("task1", 0, func(t *types.Task) {
		t.TaskID = "task1"
		t.JobID = jobIDBase64
		t.NodeID = "" // Missing - should be enriched
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
	
	t.Logf("✅ Task enriched: nodeId=%s workerId=%s", task.NodeID, task.WorkerID)
}

func hexDecode(t *testing.T, hexStr string) []byte {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("Failed to decode hex: %v", err)
	}
	return b
}
