package eventserver

import (
	"strings"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/stretchr/testify/assert"
)

func TestParseWorkerLogFile(t *testing.T) {
	handler := &EventHandler{
		ClusterTaskMap:  &types.ClusterTaskMap{ClusterTaskMap: make(map[string]*types.TaskMap)},
		ClusterActorMap: &types.ClusterActorMap{ClusterActorMap: make(map[string]*types.ActorMap)},
	}

	// 模拟 worker log 内容
	content := `:job_id:02000000
:task_name:compute_task
:task_name:compute_task
:task_name:compute_task
:actor_name:Counter
`

	reader := strings.NewReader(content)
	nopCloser := &nopCloser{reader}

	fileName := "worker-14c35918b2f1acd08af35325b5ccd7cbb8657c8ab892f66944d637dd-01000000-1106.out"
	nodeID := "9da758bbb610dd76c280cc24981575544356164c321bec114ab6c6db"
	clusterKey := "test-cluster_test-ns_test-session"

	result := handler.parseWorkerLogFile(fileName, nodeID, nopCloser, clusterKey, nil)

	// 验证结果
	assert.Equal(t, 3, result.TaskCount, "应该创建 3 个 Tasks")
	assert.Equal(t, 1, result.ActorCount, "应该创建 1 个 Actor")

	// 验证 Task 数据
	handler.ClusterTaskMap.Lock()
	taskMap := handler.ClusterTaskMap.ClusterTaskMap[clusterKey]
	handler.ClusterTaskMap.Unlock()

	assert.NotNil(t, taskMap)
	taskMap.Lock()
	assert.Equal(t, 3, len(taskMap.TaskMap), "TaskMap 应该包含 3 个 Task")
	taskMap.Unlock()

	// 验证 Actor 数据
	handler.ClusterActorMap.Lock()
	actorMap := handler.ClusterActorMap.ClusterActorMap[clusterKey]
	handler.ClusterActorMap.Unlock()

	assert.NotNil(t, actorMap)
	actorMap.Lock()
	assert.Equal(t, 1, len(actorMap.ActorMap), "ActorMap 应该包含 1 个 Actor")
	
	// 验证 Actor 的字段
	for _, actor := range actorMap.ActorMap {
		assert.Equal(t, "Counter", actor.Name)
		assert.Equal(t, "02000000", actor.JobID)
		assert.Equal(t, types.ALIVE, actor.State)
		assert.Contains(t, actor.ExitDetails, "Reconstructed from worker")
	}
	actorMap.Unlock()
}

// nopCloser 包装 io.Reader 为 io.ReadCloser
type nopCloser struct {
	*strings.Reader
}

func (n *nopCloser) Close() error {
	return nil
}

func TestParseWorkerLogFileName(t *testing.T) {
	fileName := "worker-14c35918b2f1acd08af35325b5ccd7cbb8657c8ab892f66944d637dd-02000000-1106.out"
	
	// 解析文件名
	parts := fileName[7 : len(fileName)-4] // 去掉 "worker-" 和 ".out"
	fields := strings.Split(parts, "-")
	
	assert.Equal(t, 3, len(fields))
	assert.Equal(t, "14c35918b2f1acd08af35325b5ccd7cbb8657c8ab892f66944d637dd", fields[0]) // workerID
	assert.Equal(t, "02000000", fields[1]) // jobID
	assert.Equal(t, "1106", fields[2]) // pid
}
