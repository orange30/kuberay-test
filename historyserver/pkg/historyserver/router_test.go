package historyserver

import (
	"encoding/base64"
	"testing"
	"time"

	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
)

func TestFormatActorForResponse_CamelCaseKeys(t *testing.T) {
	actorID := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	jobID := base64.StdEncoding.EncodeToString([]byte("job0123456789abcd"))
	nodeID := base64.StdEncoding.EncodeToString([]byte("node0123456789abc"))
	workerID := base64.StdEncoding.EncodeToString([]byte("work0123456789abc"))

	actor := eventtypes.Actor{
		ActorID:     actorID,
		JobID:       jobID,
		State:       eventtypes.ALIVE,
		Name:        "n",
		PID:         123,
		NumRestarts: 2,
		StartTime:   time.UnixMilli(1700000000000),
		ActorClass:  "c",
		Address: eventtypes.Address{
			NodeID:    nodeID,
			WorkerID:  workerID,
			IPAddress: "1.2.3.4",
			Port:      "1234",
		},
	}

	got := formatActorForResponse(actor)
	requireKey(t, got, "actorId")
	requireKey(t, got, "jobId")
	requireKey(t, got, "address")
	if _, ok := got["actor_id"]; ok {
		t.Fatalf("did not expect snake_case actor_id key")
	}

	addr, ok := got["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected address to be object, got %T", got["address"])
	}
	requireKey(t, addr, "nodeId")
	requireKey(t, addr, "ipAddress")
	requireKey(t, addr, "port")
	requireKey(t, addr, "workerId")
	if _, ok := addr["node_id"]; ok {
		t.Fatalf("did not expect snake_case address.node_id key")
	}

	if v, ok := got["numRestarts"]; !ok {
		t.Fatalf("expected numRestarts key")
	} else if _, isString := v.(string); !isString {
		t.Fatalf("expected numRestarts to be string, got %T", v)
	}

	if p, ok := addr["port"].(int); !ok || p != 1234 {
		t.Fatalf("expected address.port to be int(1234), got %#v (%T)", addr["port"], addr["port"])
	}
}

func requireKey(t *testing.T, m map[string]interface{}, key string) {
	t.Helper()
	if _, ok := m[key]; !ok {
		t.Fatalf("expected key %q", key)
	}
}
