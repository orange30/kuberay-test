package eventserver

import (
	"bufio"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
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

type jobDriverLogMetadata struct {
	Entrypoint   string                 `json:"entrypoint"`
	Metadata     map[string]string      `json:"metadata"`
	RuntimeEnv   map[string]interface{} `json:"runtime_env"`
	SubmissionID string                 `json:"submission_id"`
	JobID        string                 `json:"job_id"`
}

func normalizeEntrypoint(entrypoint string) string {
	ep := strings.TrimSpace(entrypoint)
	if ep == "" {
		return ""
	}
	// Some Ray logs include: "Running entrypoint for job <submission_id>: <cmd>".
	// We want to display just <cmd>.
	// Also handle the case where the prefix is already extracted into entrypoint.
	if m := regexp.MustCompile(`(?i)^for\s+job\s+\S+\s*:\s*(.+)$`).FindStringSubmatch(ep); len(m) >= 2 {
		return strings.TrimSpace(m[1])
	}
	return ep
}

func parseEntrypointFromDriverLog(r io.Reader) string {
	if r == nil {
		return ""
	}
	// Best-effort heuristics: scan first N lines for any "entrypoint" hint.
	scanner := bufio.NewScanner(r)
	// Allow moderately long lines.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	entrypointPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\brunning\s+entrypoint\b(?:\s+for\s+job\s+\S+)?\s*[:=]\s*(.+)$`),
		regexp.MustCompile(`(?i)\bentrypoint\b\s*[:=]\s*(.+)$`),
		regexp.MustCompile(`(?i)\bentrypoint\b\s+(\S.+)$`),
	}

	for i := 0; i < 200 && scanner.Scan(); i++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Try JSON line first.
		if strings.HasPrefix(line, "{") && strings.Contains(strings.ToLower(line), "entrypoint") {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				if v, ok := m["entrypoint"].(string); ok {
					v = normalizeEntrypoint(v)
					if v != "" {
						return v
					}
				}
			}
		}
		for _, re := range entrypointPatterns {
			matches := re.FindStringSubmatch(line)
			if len(matches) >= 2 {
				v := normalizeEntrypoint(matches[1])
				if len(v) >= 2 {
					if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
						v = strings.TrimSpace(v[1 : len(v)-1])
					}
				}
				v = normalizeEntrypoint(v)
				if v != "" {
					return v
				}
			}
		}
	}
	return ""
}

func (h *EventHandler) findJobDriverFile(clusterInfo utils.ClusterInfo, submissionID string) (nodeHex string, filename string, fullPath string) {
	if h.reader == nil {
		return "", "", ""
	}
	if submissionID == "" || clusterInfo.SessionName == "" {
		return "", "", ""
	}
	physicalClusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
	logsPath := clusterInfo.SessionName + "/logs/"
	for _, nodeDir := range h.reader.ListFiles(physicalClusterKey, logsPath) {
		nodeHexCandidate := strings.TrimSuffix(strings.TrimSpace(nodeDir), "/")
		if nodeHexCandidate == "" || nodeHexCandidate == "." {
			continue
		}
		files := h.reader.ListFiles(physicalClusterKey, clusterInfo.SessionName+"/logs/"+nodeHexCandidate)
		if len(files) == 0 {
			files = h.reader.ListFiles(physicalClusterKey, clusterInfo.SessionName+"/logs/"+nodeHexCandidate+"/")
		}
		if len(files) == 0 {
			continue
		}
		for _, f := range files {
			cand := strings.TrimSpace(f)
			if cand == "" {
				continue
			}
			if cand == "job-driver-"+submissionID+".log" || cand == ".job-driver-"+submissionID+".log" ||
				cand == "job-driver-"+submissionID+".log.metadata" || cand == ".job-driver-"+submissionID+".log.metadata" {
				return nodeHexCandidate, cand, clusterInfo.SessionName + "/logs/" + nodeHexCandidate + "/" + cand
			}
		}
	}
	return "", "", ""
}

func (h *EventHandler) populateJobDetailsFromDriverLogs(clusterInfo utils.ClusterInfo, submissionID string, job *types.Job) {
	if job == nil || h.reader == nil {
		return
	}
	physicalClusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace

	// Prefer structured metadata if available.
	nodeHex, filename, fullPath := h.findJobDriverFile(clusterInfo, submissionID)
	_ = nodeHex
	if fullPath != "" && strings.HasSuffix(filename, ".log.metadata") {
		if r := h.reader.GetContent(physicalClusterKey, fullPath); r != nil {
			b, err := io.ReadAll(r)
			if err == nil {
				var meta jobDriverLogMetadata
				if err := json.Unmarshal(b, &meta); err == nil {
					if job.Entrypoint == "" {
						job.Entrypoint = normalizeEntrypoint(meta.Entrypoint)
					}
					if len(job.Metadata) == 0 && len(meta.Metadata) > 0 {
						job.Metadata = meta.Metadata
					}
					if len(job.RuntimeEnv) == 0 && len(meta.RuntimeEnv) > 0 {
						job.RuntimeEnv = meta.RuntimeEnv
					}
				}
			}
		}
	}

	// Fallback: read driver log content and infer entrypoint.
	if job.Entrypoint != "" {
		return
	}
	// If we located metadata file, try to locate the corresponding .log in same node dir.
	if nodeHex != "" {
		logCandidates := []string{
			"job-driver-" + submissionID + ".log",
			".job-driver-" + submissionID + ".log",
		}
		for _, cand := range logCandidates {
			p := clusterInfo.SessionName + "/logs/" + nodeHex + "/" + cand
			if r := h.reader.GetContent(physicalClusterKey, p); r != nil {
				if ep := parseEntrypointFromDriverLog(r); ep != "" {
					job.Entrypoint = ep
					return
				}
			}
		}
	}

	// Last resort: search again specifically for a .log file.
	if h.reader == nil {
		return
	}
	logsPath := clusterInfo.SessionName + "/logs/"
	for _, nodeDir := range h.reader.ListFiles(physicalClusterKey, logsPath) {
		nodeHexCandidate := strings.TrimSuffix(strings.TrimSpace(nodeDir), "/")
		if nodeHexCandidate == "" || nodeHexCandidate == "." {
			continue
		}
		files := h.reader.ListFiles(physicalClusterKey, clusterInfo.SessionName+"/logs/"+nodeHexCandidate)
		if len(files) == 0 {
			files = h.reader.ListFiles(physicalClusterKey, clusterInfo.SessionName+"/logs/"+nodeHexCandidate+"/")
		}
		for _, f := range files {
			cand := strings.TrimSpace(f)
			if cand == "job-driver-"+submissionID+".log" || cand == ".job-driver-"+submissionID+".log" {
				p := clusterInfo.SessionName + "/logs/" + nodeHexCandidate + "/" + cand
				if r := h.reader.GetContent(physicalClusterKey, p); r != nil {
					if ep := parseEntrypointFromDriverLog(r); ep != "" {
						job.Entrypoint = ep
						return
					}
				}
			}
		}
	}
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
		h.populateJobDetailsFromDriverLogs(clusterInfo, submissionID, &job)

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
			// Jobs from event_JOBS.log are a fallback source; avoid overwriting richer structured data.
			j.JobID = job.JobID
			if j.SubmissionID == "" {
				j.SubmissionID = job.SubmissionID
			}
			if j.Type == "" {
				j.Type = job.Type
			}
			// Status/timestamps are considered authoritative from jobs log when present.
			j.Status = job.Status
			if !job.StartTime.IsZero() {
				j.StartTime = job.StartTime
			}
			if !job.EndTime.IsZero() {
				j.EndTime = job.EndTime
			}
			if job.Message != "" {
				j.Message = job.Message
			}
			if len(j.Events) == 0 && len(job.Events) > 0 {
				j.Events = job.Events
			}
			if j.DriverNodeID == "" {
				j.DriverNodeID = job.DriverNodeID
			}
			if j.Entrypoint == "" {
				j.Entrypoint = job.Entrypoint
			}
			if len(j.Metadata) == 0 && len(job.Metadata) > 0 {
				j.Metadata = job.Metadata
			}
			if len(j.RuntimeEnv) == 0 && len(job.RuntimeEnv) > 0 {
				j.RuntimeEnv = job.RuntimeEnv
			}
		})
		logrus.Infof("[createJobsFromJobsLog] Created/merged job %s (submission_id=%s, status=%s) for cluster %s",
			jobID, submissionID, jobInfo.Status, clusterKey)
	}
}
