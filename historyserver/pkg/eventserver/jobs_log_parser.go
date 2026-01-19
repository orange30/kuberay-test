package eventserver

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// JobsLogEvent represents an event from event_JOBS.log
type JobsLogEvent struct {
	EventID      string            `json:"event_id"`
	SourceType   string            `json:"source_type"`
	SourceHost   string            `json:"source_hostname"`
	SourcePID    int               `json:"source_pid"`
	Message      string            `json:"message"`
	Timestamp    string            `json:"timestamp"`
	CustomFields map[string]string `json:"custom_fields"`
	Severity     string            `json:"severity"`
	Label        string            `json:"label"`
}

// JobSubmissionInfo contains information about a job extracted from event_JOBS.log
type JobSubmissionInfo struct {
	SubmissionID string
	StartTime    time.Time
	EndTime      time.Time
	Status       types.JobStatus
	Message      string
}

func hexToBase64(hexID string) string {
	hexID = strings.TrimSpace(hexID)
	if hexID == "" {
		return ""
	}
	decoded, err := hex.DecodeString(hexID)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(decoded)
}

func (h *EventHandler) inferDriverNodeIDFromLogs(clusterInfo utils.ClusterInfo, submissionID string) string {
	if h.reader == nil {
		return ""
	}
	if submissionID == "" || clusterInfo.SessionName == "" {
		return ""
	}

	physicalClusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	logsPath := clusterInfo.SessionName + "/logs/"

	nodeDirList := h.reader.ListFiles(physicalClusterKey, logsPath)
	if len(nodeDirList) == 0 {
		return ""
	}

	logCandidates := []string{
		"job-driver-" + submissionID + ".log",
		".job-driver-" + submissionID + ".log",
		"job-driver-" + submissionID + ".log.metadata",
		".job-driver-" + submissionID + ".log.metadata",
	}

	for _, nodeDir := range nodeDirList {
		nodeHex := strings.TrimSuffix(strings.TrimSpace(nodeDir), "/")
		if nodeHex == "" || nodeHex == "." {
			continue
		}

		files := h.reader.ListFiles(physicalClusterKey, clusterInfo.SessionName+"/logs/"+nodeHex)
		if len(files) == 0 {
			continue
		}

		for _, f := range files {
			for _, cand := range logCandidates {
				if f == cand {
					return hexToBase64(nodeHex)
				}
			}
		}
	}

	return ""
}

// parseJobsLog reads event_JOBS.log and extracts job information
func (h *EventHandler) parseJobsLog(clusterInfo utils.ClusterInfo) map[string]*JobSubmissionInfo {
	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	if clusterInfo.SessionName != "" {
		clusterKey = clusterKey + "_" + clusterInfo.SessionName
	}

	// Find all event_JOBS.log files in logs/{node_id}/events/
	jobsLogFiles := h.getJobsLogFiles(clusterInfo)
	if len(jobsLogFiles) == 0 {
		logrus.Debugf("[parseJobsLog] No event_JOBS.log files found for cluster %s", clusterKey)
		return nil
	}

	logrus.Infof("[parseJobsLog] Found %d event_JOBS.log files for cluster %s", len(jobsLogFiles), clusterKey)

	jobsMap := make(map[string]*JobSubmissionInfo)

	for _, jobsLogFile := range jobsLogFiles {
		logrus.Infof("[parseJobsLog] Reading %s", jobsLogFile)

		reader := h.reader.GetContent(clusterInfo.Name+"_"+clusterInfo.Namespace, jobsLogFile)
		if reader == nil {
			logrus.Errorf("[parseJobsLog] Failed to get content for %s", jobsLogFile)
			continue
		}

		// Parse JSONL format (one JSON object per line)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}

			var event JobsLogEvent
			if err := json.Unmarshal([]byte(line), &event); err != nil {
				logrus.Errorf("[parseJobsLog] Failed to parse line: %v, line: %s", err, line)
				continue
			}

			// Extract submission_id from custom_fields
			submissionID, ok := event.CustomFields["submission_id"]
			if !ok || submissionID == "" {
				logrus.Warnf("[parseJobsLog] No submission_id in event: %s", event.Message)
				continue
			}

			// Parse timestamp (unix timestamp as string)
			timestamp, err := strconv.ParseInt(event.Timestamp, 10, 64)
			if err != nil {
				logrus.Errorf("[parseJobsLog] Failed to parse timestamp: %v", err)
				continue
			}
			eventTime := time.Unix(timestamp, 0)

			// Initialize job info if not exists
			if _, exists := jobsMap[submissionID]; !exists {
				jobsMap[submissionID] = &JobSubmissionInfo{
					SubmissionID: submissionID,
					Status:       types.JOB_RUNNING, // Default status
				}
			}

			jobInfo := jobsMap[submissionID]

			// Parse message to determine event type
			if strings.HasPrefix(event.Message, "Started a ray job") {
				// Job started event
				jobInfo.StartTime = eventTime
				logrus.Debugf("[parseJobsLog] Job %s started at %s", submissionID, eventTime)
			} else if strings.HasPrefix(event.Message, "Completed a ray job") {
				// Job completed event - extract status from message
				// Format: "Completed a ray job {id} with a status {STATUS}."
				jobInfo.EndTime = eventTime
				jobInfo.Message = event.Message

				// Extract status from message using regex
				statusPattern := regexp.MustCompile(`with a status (\w+)`)
				matches := statusPattern.FindStringSubmatch(event.Message)
				if len(matches) >= 2 {
					statusStr := matches[1]
					switch statusStr {
					case "SUCCEEDED":
						jobInfo.Status = types.JOB_SUCCEEDED
					case "FAILED":
						jobInfo.Status = types.JOB_FAILED
					case "STOPPED":
						jobInfo.Status = types.JOB_STOPPED
					default:
						logrus.Warnf("[parseJobsLog] Unknown job status: %s", statusStr)
						jobInfo.Status = types.JobStatus(statusStr)
					}
				}
				logrus.Debugf("[parseJobsLog] Job %s completed with status %s at %s", submissionID, jobInfo.Status, eventTime)
			}
		}

		if err := scanner.Err(); err != nil {
			logrus.Errorf("[parseJobsLog] Error reading file: %v", err)
		}
	}

	logrus.Infof("[parseJobsLog] Parsed %d jobs from event_JOBS.log for cluster %s", len(jobsMap), clusterKey)
	return jobsMap
}

// getJobsLogFiles returns all event_JOBS.log files in logs/{node_id}/events/
func (h *EventHandler) getJobsLogFiles(clusterInfo utils.ClusterInfo) []string {
	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	sessionName := clusterInfo.SessionName
	if sessionName == "" {
		sessionName = "default"
	}

	// List all node_id directories in logs/
	logsPath := sessionName + "/logs/"
	nodeDirList := h.reader.ListFiles(clusterKey, logsPath)

	logrus.Infof("[getJobsLogFiles] cluster=%s, session=%s, logsPath=%s",
		clusterKey, sessionName, logsPath)
	logrus.Infof("[getJobsLogFiles] nodeDirList count=%d, dirs=%v", len(nodeDirList), nodeDirList)

	var jobsLogFiles []string
	for _, nodeDir := range nodeDirList {
		// Skip non-directory entries
		if !strings.HasSuffix(nodeDir, "/") {
			logrus.Debugf("[getJobsLogFiles] Skip non-directory: %s", nodeDir)
			continue
		}

		// Construct path to event_JOBS.log: logs/{node_id}/events/event_JOBS.log
		eventJobsLogPath := logsPath + nodeDir + "events/event_JOBS.log"

		// Check if the file exists by trying to get its content
		// We'll just add it to the list and let parseJobsLog handle missing files
		jobsLogFiles = append(jobsLogFiles, eventJobsLogPath)
		logrus.Infof("[getJobsLogFiles] Added event_JOBS.log path: %s", eventJobsLogPath)
	}

	logrus.Infof("[getJobsLogFiles] Total event_JOBS.log files to check: %d", len(jobsLogFiles))
	return jobsLogFiles
}

// mapSubmissionIDToJobID maps submission_id to job_id
// For now, we directly use submission_id as job_id because:
// 1. There's no reliable way to map submission_id to Base64 job_id without reading event content
// 2. Not all jobs in event_JOBS.log have corresponding job_events (e.g., early failures)
// 3. This ensures all jobs are visible in the Dashboard
//
// Note: This means Tasks/Actors may not be correctly associated if they use Base64 job_id
// A future improvement would be to read DRIVER_JOB_DEFINITION_EVENT to get the mapping
func (h *EventHandler) mapSubmissionIDToJobID(clusterInfo utils.ClusterInfo, submissionID string) string {
	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	if clusterInfo.SessionName != "" {
		clusterKey = clusterKey + "_" + clusterInfo.SessionName
	}

	// Best-effort mapping: if we already have jobs with SubmissionID populated, reuse that JobID.
	for _, j := range h.GetJobs(clusterKey) {
		if j.SubmissionID == submissionID && j.JobID != "" {
			logrus.Debugf("[mapSubmissionIDToJobID] Mapped submission_id=%s -> job_id=%s", submissionID, j.JobID)
			return j.JobID
		}
	}

	logrus.Debugf("[mapSubmissionIDToJobID] No mapping found for submission_id=%s, fallback to submission_id as job_id", submissionID)
	return submissionID
}

// createJobsFromJobsLog creates Job objects from event_JOBS.log data and stores them
func (h *EventHandler) createJobsFromJobsLog(clusterInfo utils.ClusterInfo) {
	clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	if clusterInfo.SessionName != "" {
		clusterKey = clusterKey + "_" + clusterInfo.SessionName
	}

	// Parse event_JOBS.log
	jobsMap := h.parseJobsLog(clusterInfo)
	if len(jobsMap) == 0 {
		logrus.Debugf("[createJobsFromJobsLog] No jobs found in event_JOBS.log for cluster %s", clusterKey)
		return
	}

	// Try to infer the real Ray JobID (base64) from tasks.
	// TASK_LIFECYCLE_EVENT includes jobId; after parsing events, tasks should have Task.JobID populated.
	tasks := h.GetTasks(clusterKey)
	type jobCandidate struct {
		JobID       string
		EarliestRun time.Time
		LatestEnd   time.Time
	}
	byJob := make(map[string]*jobCandidate)
	for _, t := range tasks {
		if t.JobID == "" {
			continue
		}
		c, ok := byJob[t.JobID]
		if !ok {
			c = &jobCandidate{JobID: t.JobID}
			byJob[t.JobID] = c
		}
		if !t.StartTime.IsZero() {
			if c.EarliestRun.IsZero() || t.StartTime.Before(c.EarliestRun) {
				c.EarliestRun = t.StartTime
			}
		}
		if !t.EndTime.IsZero() {
			if c.LatestEnd.IsZero() || t.EndTime.After(c.LatestEnd) {
				c.LatestEnd = t.EndTime
			}
		}
	}
	jobCandidates := make([]jobCandidate, 0, len(byJob))
	for _, c := range byJob {
		jobCandidates = append(jobCandidates, *c)
	}
	sort.Slice(jobCandidates, func(i, j int) bool {
		// Put zero times at the end.
		a := jobCandidates[i].EarliestRun
		b := jobCandidates[j].EarliestRun
		if a.IsZero() && b.IsZero() {
			return jobCandidates[i].JobID < jobCandidates[j].JobID
		}
		if a.IsZero() {
			return false
		}
		if b.IsZero() {
			return true
		}
		return a.Before(b)
	})

	// Sort submission jobs by start time to align with task-derived job IDs.
	type submissionJob struct {
		SubmissionID string
		Info         *JobSubmissionInfo
	}
	orderedSubs := make([]submissionJob, 0, len(jobsMap))
	for submissionID, info := range jobsMap {
		orderedSubs = append(orderedSubs, submissionJob{SubmissionID: submissionID, Info: info})
	}
	sort.Slice(orderedSubs, func(i, j int) bool {
		a := orderedSubs[i].Info.StartTime
		b := orderedSubs[j].Info.StartTime
		if a.IsZero() && b.IsZero() {
			return orderedSubs[i].SubmissionID < orderedSubs[j].SubmissionID
		}
		if a.IsZero() {
			return false
		}
		if b.IsZero() {
			return true
		}
		return a.Before(b)
	})

	// Create/merge Job objects.
	for idx, sub := range orderedSubs {
		submissionID := sub.SubmissionID
		jobInfo := sub.Info

		jobID := ""
		if idx < len(jobCandidates) {
			jobID = jobCandidates[idx].JobID
		}
		if jobID == "" {
			// Fallback to any existing mapping, otherwise use submission_id.
			jobID = h.mapSubmissionIDToJobID(clusterInfo, submissionID)
		}

		job := types.Job{
			JobID:        jobID,
			SubmissionID: submissionID,
			Status:       jobInfo.Status,
			StartTime:    jobInfo.StartTime,
			EndTime:      jobInfo.EndTime,
			Message:      jobInfo.Message,
			Type:         "SUBMISSION",
		}

		if job.DriverNodeID == "" {
			job.DriverNodeID = h.inferDriverNodeIDFromLogs(clusterInfo, submissionID)
		}

		// Add state transition events
		if !jobInfo.StartTime.IsZero() {
			job.Events = append(job.Events, types.JobStateEvent{
				State:     types.JOB_RUNNING,
				Timestamp: jobInfo.StartTime,
			})
		}
		if !jobInfo.EndTime.IsZero() {
			job.Events = append(job.Events, types.JobStateEvent{
				State:     jobInfo.Status,
				Timestamp: jobInfo.EndTime,
			})
		}

		// Store the job using JobMap's CreateOrMergeJob method
		jobMap := h.ClusterJobMap.GetOrCreateJobMap(clusterKey)
		jobMap.CreateOrMergeJob(jobID, func(j *types.Job) {
			// Overwrite with data from event_JOBS.log
			j.JobID = job.JobID
			j.SubmissionID = job.SubmissionID
			j.Status = job.Status
			j.StartTime = job.StartTime
			j.EndTime = job.EndTime
			j.Message = job.Message
			j.Type = job.Type
			j.Events = job.Events
			j.DriverNodeID = job.DriverNodeID
		})
		logrus.Infof("[createJobsFromJobsLog] Created/merged job %s (submission_id=%s, status=%s) for cluster %s",
			jobID, submissionID, jobInfo.Status, clusterKey)
	}
}
