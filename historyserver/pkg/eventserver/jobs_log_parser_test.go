package eventserver

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type mockStorageForJobsLog struct {
	files    map[string][]string
	contents map[string]string
}

func (m *mockStorageForJobsLog) ListFiles(clusterNameID, prefix string) []string {
	if v, ok := m.files[prefix]; ok {
		return v
	}
	// Be forgiving about trailing slash differences.
	if strings.HasSuffix(prefix, "/") {
		if v, ok := m.files[strings.TrimSuffix(prefix, "/")]; ok {
			return v
		}
	}
	if v, ok := m.files[prefix+"/"]; ok {
		return v
	}
	return nil
}

func (m *mockStorageForJobsLog) GetContent(clusterNameID, filePath string) io.Reader {
	if s, ok := m.contents[filePath]; ok {
		return bytes.NewBufferString(s)
	}
	return nil
}

func (m *mockStorageForJobsLog) List() []utils.ClusterInfo { return nil }

func TestParseEntrypointFromDriverLog(t *testing.T) {
	log := strings.Join([]string{
		"2026-01-19 00:00:00,000 INFO something: boot",
		"2026-01-19 00:00:00,001 INFO job: Running entrypoint for job raysubmit_5Am9Defib2FPCaCv: python 1.py --epochs 10",
	}, "\n")

	ep := parseEntrypointFromDriverLog(strings.NewReader(log))
	if ep != "python 1.py --epochs 10" {
		t.Fatalf("entrypoint mismatch: got %q", ep)
	}
}

func TestCreateJobsFromJobsLog_PopulatesEntrypointFromMetadata(t *testing.T) {
	mock := &mockStorageForJobsLog{files: map[string][]string{}, contents: map[string]string{}}

	clusterInfo := utils.ClusterInfo{Name: "test-cluster", Namespace: "default", SessionName: "session_test"}
	physicalClusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	_ = physicalClusterKey

	nodeHex := "e12103b473d520863b8098c6995121fde78ce0ef6ba47ad555c9865e"

	// logs/ directories
	mock.files["session_test/logs/"] = []string{nodeHex + "/"}
	mock.files["session_test/logs/"+nodeHex] = []string{
		"job-driver-raysubmit_12345.log.metadata",
		"job-driver-raysubmit_12345.log",
		"events/",
	}

	// event_JOBS.log
	startTs := time.Date(2026, 1, 19, 0, 44, 9, 0, time.UTC).Unix()
	jobsLogPath := "session_test/logs/" + nodeHex + "/events/event_JOBS.log"
	mock.contents[jobsLogPath] = fmt.Sprintf(`{"event_id":"1","message":"Started a ray job","timestamp":"%s","custom_fields":{"submission_id":"raysubmit_12345"}}`+"\n", strconv.FormatInt(startTs, 10))

	// driver log metadata provides entrypoint/runtime_env/metadata
	metaPath := "session_test/logs/" + nodeHex + "/job-driver-raysubmit_12345.log.metadata"
	mock.contents[metaPath] = "{\"entrypoint\":\"python -c \\\"print(123)\\\"\",\"metadata\":{\"k\":\"v\"},\"runtime_env\":{\"pip\":[\"requests\"]}}"

	// driver log (should not be needed due to metadata)
	logPath := "session_test/logs/" + nodeHex + "/job-driver-raysubmit_12345.log"
	mock.contents[logPath] = "INFO job: Running entrypoint: python -c \"print(999)\"\n"

	h := NewEventHandler(mock)
	h.createJobsFromJobsLog(clusterInfo)

	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace + "_" + clusterInfo.SessionName
	jobs := h.GetJobs(clusterKey)
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Entrypoint != "python -c \"print(123)\"" {
		t.Fatalf("expected entrypoint from metadata, got %q", jobs[0].Entrypoint)
	}
	if jobs[0].Metadata["k"] != "v" {
		t.Fatalf("expected metadata populated, got %+v", jobs[0].Metadata)
	}
	if len(jobs[0].RuntimeEnv) == 0 {
		t.Fatalf("expected runtime_env populated")
	}
}