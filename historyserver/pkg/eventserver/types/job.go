package types

import (
	"sync"
	"time"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	JOB_PENDING   JobStatus = "PENDING"
	JOB_RUNNING   JobStatus = "RUNNING"
	JOB_STOPPED   JobStatus = "STOPPED"
	JOB_SUCCEEDED JobStatus = "SUCCEEDED"
	JOB_FAILED    JobStatus = "FAILED"
	// Ray actually uses FINISHED instead of SUCCEEDED in some cases
	JOB_FINISHED  JobStatus = "FINISHED"
)

// DriverInfo contains driver information for a job
// This matches the format expected by Ray Dashboard
type DriverInfo struct {
	ID             string `json:"id"`
	NodeIPAddress  string `json:"node_ip_address"`
	NodeID         string `json:"node_id"`
	PID            string `json:"pid"`
}

// JobStateEvent represents a single state transition event with its timestamp
type JobStateEvent struct {
	State     JobStatus `json:"state"`
	Timestamp time.Time `json:"timestamp"`
}

// Job represents a Ray job with its definition and lifecycle information
type Job struct {
	// --- DEFINITION FIELDS (from DRIVER_JOB_DEFINITION_EVENT) ---
	JobID        string `json:"job_id"`
	SubmissionID string `json:"submission_id,omitempty"`
	Type         string `json:"type"` // e.g., "DRIVER", "SUBMISSION"
	Entrypoint   string `json:"entrypoint"`

	// Metadata contains user-provided job metadata
	Metadata map[string]string `json:"metadata,omitempty"`

	// RuntimeEnv contains the runtime environment configuration
	RuntimeEnv map[string]interface{} `json:"runtime_env,omitempty"`

	// DriverInfo contains driver-specific information
	DriverInfo *DriverInfo `json:"driver_info,omitempty"`

	// DriverAgentHTTPAddress is the HTTP address of the driver agent
	DriverAgentHTTPAddress string `json:"driver_agent_http_address,omitempty"`

	// DriverNodeID is the node ID where the driver runs
	DriverNodeID string `json:"driver_node_id,omitempty"`

	// --- LIFECYCLE FIELDS (from DRIVER_JOB_LIFECYCLE_EVENT) ---
	Status JobStatus `json:"status"`

	// Message contains status message (e.g., error details)
	Message string `json:"message,omitempty"`

	// ErrorType contains the type of error if job failed
	ErrorType string `json:"error_type,omitempty"`

	// Events stores the complete state transition history
	Events []JobStateEvent `json:"events,omitempty"`

	// StartTime is the timestamp when job started (first RUNNING state)
	StartTime time.Time `json:"start_time,omitempty"`

	// EndTime is the timestamp when job ended (SUCCEEDED/FAILED/STOPPED)
	EndTime time.Time `json:"end_time,omitempty"`
}

// DeepCopy creates a deep copy of the Job
func (j Job) DeepCopy() Job {
	result := j

	// Deep copy Metadata map
	if j.Metadata != nil {
		result.Metadata = make(map[string]string, len(j.Metadata))
		for k, v := range j.Metadata {
			result.Metadata[k] = v
		}
	}

	// Deep copy RuntimeEnv map
	if j.RuntimeEnv != nil {
		result.RuntimeEnv = make(map[string]interface{}, len(j.RuntimeEnv))
		for k, v := range j.RuntimeEnv {
			result.RuntimeEnv[k] = v
		}
	}

	// Deep copy DriverInfo struct
	if j.DriverInfo != nil {
		driverInfoCopy := *j.DriverInfo
		result.DriverInfo = &driverInfoCopy
	}

	// Deep copy Events slice
	if j.Events != nil {
		result.Events = make([]JobStateEvent, len(j.Events))
		for i, event := range j.Events {
			result.Events[i] = event
		}
	}

	return result
}

// JobMap is a struct that uses JobID as key and stores Job information
type JobMap struct {
	JobMap map[string]Job
	Mu     sync.Mutex
}

func (j *JobMap) Lock() {
	j.Mu.Lock()
}

func (j *JobMap) Unlock() {
	j.Mu.Unlock()
}

// CreateOrMergeJob creates a new job or updates an existing one using the provided update function
func (j *JobMap) CreateOrMergeJob(jobID string, updateFunc func(*Job)) {
	j.Lock()
	defer j.Unlock()

	job, exists := j.JobMap[jobID]
	if !exists {
		job = Job{JobID: jobID}
	}

	updateFunc(&job)
	j.JobMap[jobID] = job
}

// ClusterJobMap is a struct that uses cluster name as key and stores JobMap for each cluster
type ClusterJobMap struct {
	ClusterJobMap map[string]*JobMap
	sync.RWMutex
}

// GetOrCreateJobMap gets or creates a JobMap for the given cluster
func (c *ClusterJobMap) GetOrCreateJobMap(clusterName string) *JobMap {
	c.Lock()
	defer c.Unlock()

	jobMap, ok := c.ClusterJobMap[clusterName]
	if !ok {
		jobMap = &JobMap{
			JobMap: make(map[string]Job),
		}
		c.ClusterJobMap[clusterName] = jobMap
	}
	return jobMap
}
