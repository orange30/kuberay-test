package eventserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	"github.com/sirupsen/logrus"
)

type EventHandler struct {
	reader storage.StorageReader

	ClusterTaskMap  *types.ClusterTaskMap
	ClusterActorMap *types.ClusterActorMap
	ClusterJobMap   *types.ClusterJobMap

	// Track processed files for incremental refresh
	processedFiles map[string]bool
	filesMutex     sync.RWMutex

	// Track session creation time for expiry
	sessionTimestamps map[string]time.Time
	sessionMutex      sync.RWMutex
}

var eventFilePattern = regexp.MustCompile(`-\d{4}-\d{2}-\d{2}-\d{2}$`)

func isActorTaskStateEvents(events []types.StateEvent) bool {
	for _, e := range events {
		switch e.State {
		case types.PENDING_ACTOR_TASK_ARGS_FETCH, types.PENDING_ACTOR_TASK_ORDERING_OR_CONCURRENCY:
			return true
		}
	}
	return false
}

// deriveActorIDFromTaskID best-effort derives ActorID bytes from a TaskID.
// Ray TaskID is 24 bytes for actor tasks where the first 16 bytes are ActorID.
// The input taskId is typically base64 without padding; we support both padded and raw variants.
func deriveActorIDFromTaskID(taskId string) (string, bool) {
	if taskId == "" {
		return "", false
	}

	var decoded []byte
	var err error

	// Prefer RawStdEncoding because many Ray IDs are emitted without '=' padding.
	decoded, err = base64.RawStdEncoding.DecodeString(taskId)
	if err != nil {
		decoded, err = base64.StdEncoding.DecodeString(taskId)
		if err != nil {
			decoded, err = base64.URLEncoding.DecodeString(taskId)
			if err != nil {
				decoded, err = base64.RawURLEncoding.DecodeString(taskId)
				if err != nil {
					return "", false
				}
			}
		}
	}

	if len(decoded) < 16 {
		return "", false
	}
	actorBytes := decoded[:16]
	return base64.StdEncoding.EncodeToString(actorBytes), true
}

func isValidEventFile(fileName string) bool {
	// Skip directories
	if strings.HasSuffix(fileName, "/") {
		return false
	}
	// Only files matching {nodeId}-{YYYY-MM-DD-HH} format are valid event files
	return eventFilePattern.MatchString(fileName)
}

func NewEventHandler(reader storage.StorageReader) *EventHandler {
	return &EventHandler{
		reader: reader,
		ClusterTaskMap: &types.ClusterTaskMap{
			ClusterTaskMap: make(map[string]*types.TaskMap),
		},
		ClusterActorMap: &types.ClusterActorMap{
			ClusterActorMap: make(map[string]*types.ActorMap),
		},
		ClusterJobMap: &types.ClusterJobMap{
			ClusterJobMap: make(map[string]*types.JobMap),
		},
		processedFiles:    make(map[string]bool),
		sessionTimestamps: make(map[string]time.Time),
	}
}

// ProcessEvents func reads the channel and then processes the event received
func (h *EventHandler) ProcessEvents(ctx context.Context, ch <-chan map[string]any) error {
	logrus.Infof("Starting a event processor channel")
	for {
		select {
		case <-ctx.Done():
			// TODO: The context was cancelled, either stop here or process the rest of the events and return
			// Currently, it will just stop.
			logrus.Warnf("Event processor context was cancelled")
			return ctx.Err()
		case currEventData, ok := <-ch:
			if !ok {
				logrus.Warnf("Channel was closed")
				return nil
			}
			if err := h.storeEvent(currEventData); err != nil {
				logrus.Errorf("Failed to store event: %v", err)
				continue
			}
		}
	}
}

// Run will start numOfEventProcessors (default to 5) processing functions and the event reader. The event reader will run once an hr,
// which is currently how often the collector flushes.
func (h *EventHandler) Run(stop chan struct{}, numOfEventProcessors int) error {
	logrus.Info("🚀 EventHandler.Run() started") // 明显的启动标记
	var wg sync.WaitGroup

	if numOfEventProcessors == 0 {
		numOfEventProcessors = 5
	}
	logrus.Infof("Creating %d event processor channels", numOfEventProcessors)
	eventProcessorChannels := make([]chan map[string]any, numOfEventProcessors)
	cctx := make([]context.CancelFunc, numOfEventProcessors)

	for i := range numOfEventProcessors {
		eventProcessorChannels[i] = make(chan map[string]any, 100)
	}

	for i, currEventChannel := range eventProcessorChannels {
		wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		cctx[i] = cancel
		go func() {
			defer wg.Done()
			var processor EventProcessor[map[string]any] = h
			err := processor.ProcessEvents(ctx, currEventChannel)
			if err == ctx.Err() {
				logrus.Warnf("Event processor go routine %d is now closed", i)
				return
			}
			if err != nil {
				logrus.Errorf("event processor %d go routine failed %v", i, err)
				return
			}
		}()
	}

	// Start reading files and sending events for processing
	wg.Add(1)
	go func() {
		defer wg.Done()
		logrus.Info("Starting event file reader loop")

		// Helper function to process all events
		processAllEvents := func() {
			clusterList := h.reader.List()
			logrus.Infof("🔍 [EventHandler] Found %d clusters from storage", len(clusterList))
			
			if len(clusterList) == 0 {
				logrus.Warnf("⚠️  [EventHandler] No clusters found! This means:")
				logrus.Warnf("   1. COS bucket is empty, OR")
				logrus.Warnf("   2. COS path (--ray-root-dir) is incorrect, OR")
				logrus.Warnf("   3. COS authentication failed")
				logrus.Warnf("   💡 Check: --ray-root-dir=mce-proj-bk0lwhh1")
				return
			}
			
			newFilesProcessed := 0
			skippedFiles := 0
			skippedOldSessions := 0
			sessionMaxAge := getSessionMaxAge()
			now := time.Now()
			
			for idx, clusterInfo := range clusterList {
				logrus.Infof("📦 [EventHandler] Processing cluster [%d/%d]: Name=%s, Namespace=%s, Session=%s",
					idx+1, len(clusterList), clusterInfo.Name, clusterInfo.Namespace, clusterInfo.SessionName)
				
				// Build cluster key
				clusterKey := clusterInfo.Name + "_" + clusterInfo.Namespace
				if clusterInfo.SessionName != "" {
					clusterKey = clusterKey + "_" + clusterInfo.SessionName
				}
				
				// Skip sessions that are too old (optimization: don't load expired sessions)
				sessionTime := time.Unix(clusterInfo.CreateTimeStamp, 0)
				if now.Sub(sessionTime) > sessionMaxAge {
					skippedOldSessions++
					logrus.Debugf("Skipping old session %s (age: %v, max: %v)", 
						clusterKey, now.Sub(sessionTime), sessionMaxAge)
					continue
				}
				
				// Update session timestamp for expiry tracking
				h.updateSessionTimestamp(clusterKey, time.Unix(clusterInfo.CreateTimeStamp, 0))
				
				clusterNameNamespace := clusterInfo.Name + "_" + clusterInfo.Namespace
				eventFileList := append(h.getAllJobEventFiles(clusterInfo), h.getAllNodeEventFiles(clusterInfo)...)

				logrus.Infof("current eventFileList for cluster %s is: %v", clusterInfo.Name, eventFileList)
				for _, eventFile := range eventFileList {
					// Incremental: skip already processed files
					fullPath := clusterNameNamespace + "/" + eventFile
					if h.isFileProcessed(fullPath) {
						skippedFiles++
						logrus.Debugf("Skipping already processed file: %s", eventFile)
						continue
					}
					
					logrus.Infof("Reading new event file: %s", eventFile)

					eventioReader := h.reader.GetContent(clusterNameNamespace, eventFile)
					if eventioReader == nil {
						logrus.Errorf("Failed to get content for event file: %s, skipping", eventFile)
						continue
					}
					eventbytes, err := io.ReadAll(eventioReader)
					if err != nil {
						logrus.Errorf("Failed to read event file: %v", err)
						continue
					}

					var eventList []map[string]any
					if err := json.Unmarshal(eventbytes, &eventList); err != nil {
						logrus.Errorf("Failed to unmarshal event: %v", err)
						continue
					}

					// Evenly distribute events to each channel
					for i, curr := range eventList {
						// Skip nil events (can occur with corrupted event files containing null elements)
						if curr == nil {
							continue
						}
						// Construct unique key for internal storage
						// Must match the key construction in router.go
						internalParams := clusterInfo.Name + "_" + clusterInfo.Namespace
						if clusterInfo.SessionName != "" {
							internalParams = internalParams + "_" + clusterInfo.SessionName
						}
						curr["clusterName"] = internalParams

						eventProcessorChannels[i%numOfEventProcessors] <- curr
					}
					
					// Mark file as processed after successful processing
					h.markFileProcessed(fullPath)
					newFilesProcessed++
				}

				// After processing structured events, parse event_JOBS.log as fallback
				logrus.Infof("[EventHandler] Parsing event_JOBS.log for cluster %s", clusterInfo.Name)
				h.createJobsFromJobsLog(clusterInfo)

				// Enrich tasks from log files (补齐缺失的 nodeId/workerId)
				logrus.Debugf("[EventHandler] Enriching tasks from log files for cluster %s", clusterInfo.Name)
				h.EnrichTasksFromLogs(clusterInfo)

				// Fallback: reconstruct task/actor data from worker-*.out logs if event files are empty
				logrus.Debugf("[EventHandler] Checking if worker log fallback is needed for cluster %s", clusterInfo.Name)
				h.FallbackToWorkerLogs(clusterInfo)
			}
			
			logrus.Infof("✅ [EventHandler] Refresh complete: %d new files processed, %d files skipped (already processed), %d old sessions skipped", 
				newFilesProcessed, skippedFiles, skippedOldSessions)
		}

		// Process events immediately on startup
		processAllEvents()

		// Get configurable intervals
		refreshInterval := getRefreshInterval()
		sessionMaxAge := getSessionMaxAge()
		
		logrus.Infof("[EventHandler] Configuration:")
		logrus.Infof("  - Refresh interval: %v (env: HISTORYSERVER_REFRESH_INTERVAL)", refreshInterval)
		logrus.Infof("  - Session max age: %v (env: HISTORYSERVER_SESSION_MAX_AGE)", sessionMaxAge)

		// Create tickers for periodic operations
		refreshTicker := time.NewTicker(refreshInterval)
		defer refreshTicker.Stop()
		
		// Cleanup ticker - run cleanup every hour regardless of refresh interval
		cleanupTicker := time.NewTicker(1 * time.Hour)
		defer cleanupTicker.Stop()

		for {
			logrus.Info("Finished reading files, waiting for next cycle...")
			select {
			case <-stop:
				// Received stop signal, clean up and exit
				for i, currChan := range eventProcessorChannels {
					close(currChan)
					cctx[i]()
				}
				logrus.Info("Event processor received stop signal, exiting.")
				return
			case <-refreshTicker.C:
				// Process events at configured interval
				logrus.Info("[EventHandler] Periodic refresh triggered")
				
				// Reload credentials before processing (for temporary credential rotation)
				if err := h.reader.ReloadCredentials(); err != nil {
					logrus.Errorf("[EventHandler] Failed to reload credentials: %v", err)
					// Continue anyway - will retry next cycle
				}
				
				processAllEvents()
			case <-cleanupTicker.C:
				// Cleanup expired sessions
				logrus.Info("[EventHandler] Running session cleanup")
				h.cleanupExpiredSessions(sessionMaxAge)
			}
		}
	}()

	wg.Wait()
	return nil
}

// storeEvent unmarshals the event map into the correct actor/task struct and then stores it into the corresonding list
func (h *EventHandler) storeEvent(eventMap map[string]any) error {
	eventTypeVal, ok := eventMap["eventType"]
	if !ok {
		return fmt.Errorf("event missing 'eventType' field")
	}
	eventTypeStr, ok := eventTypeVal.(string)
	if !ok {
		return fmt.Errorf("eventType is not a string, got %T", eventTypeVal)
	}
	eventType := types.EventType(eventTypeStr)

	clusterNameVal, ok := eventMap["clusterName"]
	if !ok {
		return fmt.Errorf("event missing 'clusterName' field")
	}
	currentClusterName, ok := clusterNameVal.(string)
	if !ok {
		return fmt.Errorf("clusterName is not a string, got %T", clusterNameVal)
	}

	logrus.Infof("current eventType: %v", eventType)
	switch eventType {
	case types.TASK_DEFINITION_EVENT:
		taskDef, ok := eventMap["taskDefinitionEvent"]
		if !ok {
			return fmt.Errorf("event does not have 'taskDefinitionEvent'")
		}
		jsonTaskDefinition, err := json.Marshal(taskDef)
		if err != nil {
			return err
		}

		var currTask types.Task
		if err := json.Unmarshal(jsonTaskDefinition, &currTask); err != nil {
			return err
		}

		taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(currentClusterName)
		taskMap.CreateOrMergeAttempt(currTask.TaskID, currTask.AttemptNumber, func(t *types.Task) {
			// Merge definition fields while preserving lifecycle-derived fields.
			existingEvents := t.Events
			existingState := t.State
			existingJobID := t.JobID
			existingNodeID := t.NodeID
			existingActorID := t.ActorID
			existingWorkerID := t.WorkerID
			existingStartTime := t.StartTime
			existingEndTime := t.EndTime
			existingTaskLogInfo := t.TaskLogInfo
			existingErrorType := t.ErrorType
			existingErrorMessage := t.ErrorMessage

			*t = currTask

			if len(existingEvents) > 0 {
				t.Events = existingEvents
				t.State = existingState
			}
			if t.JobID == "" {
				t.JobID = existingJobID
			}
			if t.NodeID == "" {
				t.NodeID = existingNodeID
			}
			if t.ActorID == "" {
				t.ActorID = existingActorID
			}
			if t.WorkerID == "" {
				t.WorkerID = existingWorkerID
			}
			if t.StartTime.IsZero() {
				t.StartTime = existingStartTime
			}
			if t.EndTime.IsZero() {
				t.EndTime = existingEndTime
			}
			if len(t.TaskLogInfo) == 0 {
				t.TaskLogInfo = existingTaskLogInfo
			}
			if t.ErrorType == "" {
				t.ErrorType = existingErrorType
			}
			if t.ErrorMessage == "" {
				t.ErrorMessage = existingErrorMessage
			}
		})

	case types.TASK_LIFECYCLE_EVENT:
		lifecycleEvent, ok := eventMap["taskLifecycleEvent"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid taskLifecycleEvent format")
		}

		taskId, _ := lifecycleEvent["taskId"].(string)
		taskAttempt, _ := lifecycleEvent["taskAttempt"].(float64)
		transitions, _ := lifecycleEvent["stateTransitions"].([]any)
		jobId, _ := lifecycleEvent["jobId"].(string)
		actorId, _ := lifecycleEvent["actorId"].(string)

		nodeId, _ := lifecycleEvent["nodeId"].(string)
		workerId, _ := lifecycleEvent["workerId"].(string)

		if len(transitions) == 0 || taskId == "" {
			return nil
		}

		// Parse state transitions
		var stateEvents []types.StateEvent
		for _, transition := range transitions {
			tr, ok := transition.(map[string]any)
			if !ok {
				continue
			}
			state, _ := tr["state"].(string)
			timestampStr, _ := tr["timestamp"].(string)

			var timestamp time.Time
			if timestampStr != "" {
				timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
			}

			stateEvents = append(stateEvents, types.StateEvent{
				State:     types.TaskStatus(state),
				Timestamp: timestamp,
			})
		}

		if len(stateEvents) == 0 {
			return nil
		}

		// Best-effort fallback: derive actorId from taskId for actor tasks when export does not include actorId.
		derivedActorID := ""
		isActorTask := isActorTaskStateEvents(stateEvents)
		if actorId == "" && isActorTask {
			if d, ok := deriveActorIDFromTaskID(taskId); ok {
				derivedActorID = d
			}
		}

		taskMap := h.ClusterTaskMap.GetOrCreateTaskMap(currentClusterName)
		taskMap.CreateOrMergeAttempt(taskId, int(taskAttempt), func(t *types.Task) {
			// --- DEDUPLICATION using (State + Timestamp) as unique key ---
			// Build a set of existing event keys to detect duplicates
			type eventKey struct {
				State     string
				Timestamp int64
			}
			existingKeys := make(map[eventKey]bool)
			for _, e := range t.Events {
				existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
			}

			// Only append events that haven't been seen before
			for _, e := range stateEvents {
				key := eventKey{string(e.State), e.Timestamp.UnixNano()}
				if !existingKeys[key] {
					t.Events = append(t.Events, e)
					existingKeys[key] = true
				}
			}

			// Sort events by timestamp to ensure correct order
			sort.Slice(t.Events, func(i, j int) bool {
				return t.Events[i].Timestamp.Before(t.Events[j].Timestamp)
			})

			if len(t.Events) == 0 {
				return
			}

			t.State = t.Events[len(t.Events)-1].State

			if jobId != "" {
				t.JobID = jobId
			}

			if actorId != "" {
				t.ActorID = actorId
			} else if derivedActorID != "" && t.ActorID == "" {
				t.ActorID = derivedActorID
			}
			if isActorTask && t.Type == "" {
				t.Type = types.ACTOR_TASK
			}

			if nodeId != "" {
				t.NodeID = nodeId
			}
			if workerId != "" {
				t.WorkerID = workerId
			}
			if t.StartTime.IsZero() {
				for _, e := range t.Events {
					if e.State == types.RUNNING {
						t.StartTime = e.Timestamp
						break
					}
				}
			}
			lastEvent := t.Events[len(t.Events)-1]
			if lastEvent.State == types.FINISHED || lastEvent.State == types.FAILED {
				t.EndTime = lastEvent.Timestamp
			}
		})

	case types.ACTOR_DEFINITION_EVENT:
		actorDef, ok := eventMap["actorDefinitionEvent"]
		if !ok {
			return fmt.Errorf("event does not have 'actorDefinitionEvent'")
		}
		jsonActorDefinition, err := json.Marshal(actorDef)
		if err != nil {
			return err
		}

		var currActor types.Actor
		if err := json.Unmarshal(jsonActorDefinition, &currActor); err != nil {
			return err
		}

		// Use CreateOrMergeActor pattern (same as Task)
		actorMap := h.ClusterActorMap.GetOrCreateActorMap(currentClusterName)
		actorMap.CreateOrMergeActor(currActor.ActorID, func(a *types.Actor) {
			// Preserve lifecycle-derived fields that may have arrived first
			existingEvents := a.Events
			existingState := a.State
			existingStartTime := a.StartTime
			existingEndTime := a.EndTime
			existingNumRestarts := a.NumRestarts
			existingPID := a.PID
			existingExitDetails := a.ExitDetails
			existingAddress := a.Address

			// Overwrite with definition fields
			*a = currActor

			// Restore lifecycle-derived fields if they existed
			if len(existingEvents) > 0 {
				a.Events = existingEvents
				a.State = existingState
				a.StartTime = existingStartTime
				a.EndTime = existingEndTime
				a.NumRestarts = existingNumRestarts
				a.PID = existingPID
				a.ExitDetails = existingExitDetails
				a.Address = existingAddress
			}
		})
	case types.ACTOR_LIFECYCLE_EVENT:
		lifecycleEvent, ok := eventMap["actorLifecycleEvent"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid actorLifecycleEvent format")
		}

		actorId, _ := lifecycleEvent["actorId"].(string)
		transitions, _ := lifecycleEvent["stateTransitions"].([]any)

		if len(transitions) == 0 || actorId == "" {
			return nil
		}

		// Parse state transitions into ActorStateEvent slice
		var stateEvents []types.ActorStateEvent
		for _, transition := range transitions {
			tr, ok := transition.(map[string]any)
			if !ok {
				continue
			}
			state, _ := tr["state"].(string)
			timestampStr, _ := tr["timestamp"].(string)
			nodeId, _ := tr["nodeId"].(string)
			workerId, _ := tr["workerId"].(string)
			reprName, _ := tr["reprName"].(string)

			var timestamp time.Time
			if timestampStr != "" {
				timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
			}

			// DeathCause is a complex object, store as JSON string
			var deathCause string
			if dc, ok := tr["deathCause"]; ok {
				if dcBytes, err := json.Marshal(dc); err == nil {
					deathCause = string(dcBytes)
				}
			}

			stateEvents = append(stateEvents, types.ActorStateEvent{
				State:      types.StateType(state),
				Timestamp:  timestamp,
				NodeID:     nodeId,
				WorkerID:   workerId,
				ReprName:   reprName,
				DeathCause: deathCause,
			})
		}

		if len(stateEvents) == 0 {
			return nil
		}

		actorMap := h.ClusterActorMap.GetOrCreateActorMap(currentClusterName)
		actorMap.CreateOrMergeActor(actorId, func(a *types.Actor) {
			// Ensure ActorID is set (in case LIFECYCLE arrives before DEFINITION)
			a.ActorID = actorId

			// --- DEDUPLICATION using (State + Timestamp) as unique key ---
			// Build a set of existing event keys to detect duplicates
			type eventKey struct {
				State     string
				Timestamp int64
			}
			existingKeys := make(map[eventKey]bool)
			for _, e := range a.Events {
				existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
			}

			// Only append events that haven't been seen before
			for _, e := range stateEvents {
				key := eventKey{string(e.State), e.Timestamp.UnixNano()}
				if !existingKeys[key] {
					a.Events = append(a.Events, e)
					existingKeys[key] = true
				}
			}

			// Sort events by timestamp to ensure correct order
			sort.Slice(a.Events, func(i, j int) bool {
				return a.Events[i].Timestamp.Before(a.Events[j].Timestamp)
			})

			if len(a.Events) == 0 {
				return
			}

			lastEvent := a.Events[len(a.Events)-1]

			// --- UPDATE STATE ---
			a.State = lastEvent.State

			// --- UPDATE ADDRESS from ALIVE state ---
			// NodeID and WorkerID are only populated in ALIVE state
			for i := len(a.Events) - 1; i >= 0; i-- {
				if a.Events[i].State == types.ALIVE && a.Events[i].NodeID != "" {
					a.Address.NodeID = a.Events[i].NodeID
					a.Address.WorkerID = a.Events[i].WorkerID
					break
				}
			}

			// --- UPDATE ReprName from latest ---
			if lastEvent.ReprName != "" {
				a.ReprName = lastEvent.ReprName
			}

			// --- CALCULATE StartTime (first ALIVE timestamp) ---
			if a.StartTime.IsZero() {
				for _, e := range a.Events {
					if e.State == types.ALIVE {
						a.StartTime = e.Timestamp
						break
					}
				}
			}

			// --- HANDLE DEAD state ---
			if lastEvent.State == types.DEAD {
				a.EndTime = lastEvent.Timestamp

				// Parse deathCause to extract PID, IP, errorMessage
				if lastEvent.DeathCause != "" {
					var deathCauseMap map[string]any
					if err := json.Unmarshal([]byte(lastEvent.DeathCause), &deathCauseMap); err == nil {
						if ctx, ok := deathCauseMap["actorDiedErrorContext"].(map[string]any); ok {
							// Extract PID
							if pid, ok := ctx["pid"].(float64); ok {
								a.PID = int(pid)
							}
							// Extract IP address
							if ip, ok := ctx["nodeIpAddress"].(string); ok {
								a.Address.IPAddress = ip
							}
							// Extract error message as ExitDetails
							if errMsg, ok := ctx["errorMessage"].(string); ok {
								a.ExitDetails = errMsg
							}
						}
					}
				}
			}

			// --- COUNT RESTARTS ---
			restartCount := 0
			for _, e := range a.Events {
				if e.State == types.RESTARTING {
					restartCount++
				}
			}
			a.NumRestarts = restartCount
		})

	case types.ACTOR_TASK_DEFINITION_EVENT:
		// TODO: Handle actor task definition event
		// This is related to GET /api/v0/tasks (type=ACTOR_TASK)

	case types.DRIVER_JOB_DEFINITION_EVENT:
		jobDef, ok := eventMap["driverJobDefinitionEvent"]
		if !ok {
			return fmt.Errorf("event does not have 'driverJobDefinitionEvent'")
		}

		logrus.Debugf("Processing DRIVER_JOB_DEFINITION_EVENT: %+v", jobDef)

		jsonJobDefinition, err := json.Marshal(jobDef)
		if err != nil {
			return err
		}

		var currJob types.Job
		if err := json.Unmarshal(jsonJobDefinition, &currJob); err != nil {
			logrus.Errorf("Failed to unmarshal job definition: %v, json: %s", err, string(jsonJobDefinition))
			return err
		}

		// Ray might use different field names, try to extract from raw map
		if jobDefMap, ok := jobDef.(map[string]any); ok {
			// Try jobId vs job_id
			if currJob.JobID == "" {
				if jobId, ok := jobDefMap["jobId"].(string); ok {
					currJob.JobID = jobId
				}
			}
			// Try jobType vs type
			if currJob.Type == "" {
				if jobType, ok := jobDefMap["jobType"].(string); ok {
					currJob.Type = jobType
				}
			}
			// entrypoint should be correct, but double-check
			if currJob.Entrypoint == "" {
				if entrypoint, ok := jobDefMap["entrypoint"].(string); ok {
					currJob.Entrypoint = entrypoint
				} else if driverEntry, ok := jobDefMap["driverEntry"].(string); ok {
					currJob.Entrypoint = driverEntry
				}
			}
		}

		logrus.Infof("Parsed Job Definition - JobID: %s, Type: %s, Entrypoint: %s",
			currJob.JobID, currJob.Type, currJob.Entrypoint)

		jobMap := h.ClusterJobMap.GetOrCreateJobMap(currentClusterName)
		jobMap.CreateOrMergeJob(currJob.JobID, func(j *types.Job) {
			// Preserve lifecycle-derived fields that may have arrived first
			existingEvents := j.Events
			existingStatus := j.Status
			existingStartTime := j.StartTime
			existingEndTime := j.EndTime
			existingMessage := j.Message
			existingErrorType := j.ErrorType

			// Overwrite with definition fields
			*j = currJob

			// Restore lifecycle-derived fields if they existed
			if len(existingEvents) > 0 {
				j.Events = existingEvents
				j.Status = existingStatus
				j.StartTime = existingStartTime
				j.EndTime = existingEndTime
				j.Message = existingMessage
				j.ErrorType = existingErrorType
			}
		})

	case types.DRIVER_JOB_LIFECYCLE_EVENT:
		lifecycleEvent, ok := eventMap["driverJobLifecycleEvent"].(map[string]any)
		if !ok {
			return fmt.Errorf("invalid driverJobLifecycleEvent format")
		}

		// Try both jobId and job_id
		jobId, _ := lifecycleEvent["jobId"].(string)
		if jobId == "" {
			jobId, _ = lifecycleEvent["job_id"].(string)
		}

		transitions, _ := lifecycleEvent["stateTransitions"].([]any)

		logrus.Debugf("Processing DRIVER_JOB_LIFECYCLE_EVENT - JobID: %s, Transitions: %d", jobId, len(transitions))

		if len(transitions) == 0 || jobId == "" {
			return nil
		}

		// Parse state transitions into JobStateEvent slice
		var stateEvents []types.JobStateEvent
		for _, transition := range transitions {
			tr, ok := transition.(map[string]any)
			if !ok {
				continue
			}
			state, _ := tr["state"].(string)
			timestampStr, _ := tr["timestamp"].(string)

			var timestamp time.Time
			if timestampStr != "" {
				timestamp, _ = time.Parse(time.RFC3339Nano, timestampStr)
			}

			stateEvents = append(stateEvents, types.JobStateEvent{
				State:     types.JobStatus(state),
				Timestamp: timestamp,
			})
		}

		if len(stateEvents) == 0 {
			return nil
		}

		// Extract message and error_type from lifecycle event if present
		message, _ := lifecycleEvent["message"].(string)
		errorType, _ := lifecycleEvent["errorType"].(string)

		jobMap := h.ClusterJobMap.GetOrCreateJobMap(currentClusterName)
		jobMap.CreateOrMergeJob(jobId, func(j *types.Job) {
			// --- DEDUPLICATION using (State + Timestamp) as unique key ---
			type eventKey struct {
				State     string
				Timestamp int64
			}
			existingKeys := make(map[eventKey]bool)
			for _, e := range j.Events {
				existingKeys[eventKey{string(e.State), e.Timestamp.UnixNano()}] = true
			}

			// Only append events that haven't been seen before
			for _, e := range stateEvents {
				key := eventKey{string(e.State), e.Timestamp.UnixNano()}
				if !existingKeys[key] {
					j.Events = append(j.Events, e)
					existingKeys[key] = true
				}
			}

			// Sort events by timestamp to ensure correct order
			sort.Slice(j.Events, func(i, k int) bool {
				return j.Events[i].Timestamp.Before(j.Events[k].Timestamp)
			})

			if len(j.Events) == 0 {
				return
			}

			// Update current status to the latest event state
			j.Status = j.Events[len(j.Events)-1].State

			// Update message and error type if provided
			if message != "" {
				j.Message = message
			}
			if errorType != "" {
				j.ErrorType = errorType
			}

			// Calculate StartTime
			// Priority: 1) First RUNNING state, 2) First PENDING state, 3) First event timestamp
			if j.StartTime.IsZero() {
				for _, e := range j.Events {
					if e.State == types.JOB_RUNNING {
						j.StartTime = e.Timestamp
						break
					}
				}
				// If no RUNNING state found, try PENDING
				if j.StartTime.IsZero() {
					for _, e := range j.Events {
						if e.State == types.JOB_PENDING {
							j.StartTime = e.Timestamp
							break
						}
					}
				}
				// If still no start time, use first event timestamp
				if j.StartTime.IsZero() && len(j.Events) > 0 {
					j.StartTime = j.Events[0].Timestamp
				}
			}

			// Calculate EndTime (SUCCEEDED, FAILED, STOPPED, or FINISHED state)
			lastEvent := j.Events[len(j.Events)-1]
			if lastEvent.State == types.JOB_SUCCEEDED || lastEvent.State == types.JOB_FAILED ||
				lastEvent.State == types.JOB_STOPPED || lastEvent.State == types.JOB_FINISHED {
				j.EndTime = lastEvent.Timestamp
			}

			logrus.Debugf("Job %s: Events=%d, StartTime=%v, EndTime=%v, Status=%s",
				jobId, len(j.Events), j.StartTime, j.EndTime, j.Status)
		})

		logrus.Debugf("ACTOR_TASK_DEFINITION_EVENT received, not yet implemented")
	default:
		logrus.Infof("Event not supported, skipping: %v", eventMap)
	}

	return nil
}

// getAllJobEventFiles get all the job event files for the given cluster.
// Assuming that the events file object follow the format root/clustername/sessionid/job_events/{job-*}/*
func (h *EventHandler) getAllJobEventFiles(clusterInfo utils.ClusterInfo) []string {
	var allJobFiles []string
	clusterNameID := clusterInfo.Name + "_" + clusterInfo.Namespace
	jobEventDirPrefix := clusterInfo.SessionName + "/job_events/"
	jobDirList := h.reader.ListFiles(clusterNameID, jobEventDirPrefix)

	logrus.Infof("[getAllJobEventFiles] cluster=%s, session=%s, jobEventDirPrefix=%s",
		clusterNameID, clusterInfo.SessionName, jobEventDirPrefix)
	logrus.Infof("[getAllJobEventFiles] jobDirList count=%d, dirs=%v", len(jobDirList), jobDirList)

	for _, jobDir := range jobDirList {
		// Skip non-directory entries
		if !strings.HasSuffix(jobDir, "/") {
			logrus.Debugf("[getAllJobEventFiles] Skip non-directory: %s", jobDir)
			continue
		}
		jobDirPath := jobEventDirPrefix + jobDir
		jobFiles := h.reader.ListFiles(clusterNameID, jobDirPath)
		logrus.Infof("[getAllJobEventFiles] jobDir=%s, files count=%d, files=%v", jobDir, len(jobFiles), jobFiles)
		for _, jobFile := range jobFiles {
			if isValidEventFile(jobFile) {
				allJobFiles = append(allJobFiles, jobDirPath+jobFile)
				logrus.Infof("[getAllJobEventFiles] Added valid event file: %s", jobDirPath+jobFile)
			} else {
				logrus.Debugf("[getAllJobEventFiles] Skip invalid event file: %s", jobFile)
			}
		}
	}
	logrus.Infof("[getAllJobEventFiles] Total job event files found: %d", len(allJobFiles))
	return allJobFiles
}

// getAllNodeEventFiles retrieves all node event files for the given cluster
func (h *EventHandler) getAllNodeEventFiles(clusterInfo utils.ClusterInfo) []string {
	clusterNameID := clusterInfo.Name + "_" + clusterInfo.Namespace
	nodeEventDirPrefix := clusterInfo.SessionName + "/node_events/"
	nodeEventFileNames := h.reader.ListFiles(clusterNameID, nodeEventDirPrefix)

	// Filter out directories (items ending with /) and build full paths
	var nodeEventFiles []string
	for _, fileName := range nodeEventFileNames {
		// Skip directories
		if isValidEventFile(fileName) {
			fullPath := nodeEventDirPrefix + fileName
			nodeEventFiles = append(nodeEventFiles, fullPath)
		}
	}
	return nodeEventFiles
}

// GetTasks returns a thread-safe deep copy of all tasks (including all attempts) for a given cluster.
// Each task attempt is returned as a separate element in the slice.
// Deep copy ensures the returned data is safe to use after the lock is released.
func (h *EventHandler) GetTasks(clusterName string) []types.Task {
	h.ClusterTaskMap.RLock()
	defer h.ClusterTaskMap.RUnlock()

	taskMap, ok := h.ClusterTaskMap.ClusterTaskMap[clusterName]
	if !ok {
		return []types.Task{}
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	// Flatten all attempts into a single slice with deep copy
	var tasks []types.Task
	for _, attempts := range taskMap.TaskMap {
		for _, task := range attempts {
			tasks = append(tasks, task.DeepCopy())
		}
	}
	return tasks
}

// GetTaskByID returns all attempts for a specific task ID in a given cluster.
// Returns a slice of tasks representing all attempts, sorted by attempt number is not guaranteed.
func (h *EventHandler) GetTaskByID(clusterName, taskID string) ([]types.Task, bool) {
	h.ClusterTaskMap.RLock()
	defer h.ClusterTaskMap.RUnlock()

	taskMap, ok := h.ClusterTaskMap.ClusterTaskMap[clusterName]
	if !ok {
		return nil, false
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	attempts, ok := taskMap.TaskMap[taskID]
	if !ok || len(attempts) == 0 {
		return nil, false
	}
	// Return a deep copy to avoid data race
	result := make([]types.Task, len(attempts))
	for i, task := range attempts {
		result[i] = task.DeepCopy()
	}
	return result, true
}

// GetTasksByJobID returns all tasks (including all attempts) for a given job ID in a cluster.
func (h *EventHandler) GetTasksByJobID(clusterName, jobID string) []types.Task {
	h.ClusterTaskMap.RLock()
	defer h.ClusterTaskMap.RUnlock()

	taskMap, ok := h.ClusterTaskMap.ClusterTaskMap[clusterName]
	if !ok {
		return []types.Task{}
	}

	taskMap.Lock()
	defer taskMap.Unlock()

	var tasks []types.Task
	for _, attempts := range taskMap.TaskMap {
		for _, task := range attempts {
			if task.JobID == jobID {
				tasks = append(tasks, task.DeepCopy())
			}
		}
	}
	return tasks
}

// GetActors returns a thread-safe deep copy of all actors for a given cluster
func (h *EventHandler) GetActors(clusterName string) []types.Actor {
	h.ClusterActorMap.RLock()
	defer h.ClusterActorMap.RUnlock()

	actorMap, ok := h.ClusterActorMap.ClusterActorMap[clusterName]
	if !ok {
		return []types.Actor{}
	}

	actorMap.Lock()
	defer actorMap.Unlock()

	actors := make([]types.Actor, 0, len(actorMap.ActorMap))
	for _, actor := range actorMap.ActorMap {
		actors = append(actors, actor.DeepCopy())
	}
	return actors
}

// GetActorByID returns a specific actor by ID for a given cluster
func (h *EventHandler) GetActorByID(clusterName, actorID string) (types.Actor, bool) {
	h.ClusterActorMap.RLock()
	defer h.ClusterActorMap.RUnlock()

	actorMap, ok := h.ClusterActorMap.ClusterActorMap[clusterName]
	if !ok {
		return types.Actor{}, false
	}

	actorMap.Lock()
	defer actorMap.Unlock()

	actor, ok := actorMap.ActorMap[actorID]
	if !ok {
		return types.Actor{}, false
	}
	return actor.DeepCopy(), true
}

// GetActorsMap returns a thread-safe deep copy of all actors as a map for a given cluster
func (h *EventHandler) GetActorsMap(clusterName string) map[string]types.Actor {
	h.ClusterActorMap.RLock()
	defer h.ClusterActorMap.RUnlock()

	actorMap, ok := h.ClusterActorMap.ClusterActorMap[clusterName]
	if !ok {
		return map[string]types.Actor{}
	}

	actorMap.Lock()
	defer actorMap.Unlock()

	actors := make(map[string]types.Actor, len(actorMap.ActorMap))
	for id, actor := range actorMap.ActorMap {
		actors[id] = actor.DeepCopy()
	}
	return actors
}

// GetJobs returns a thread-safe deep copy of all jobs for a given cluster
func (h *EventHandler) GetJobs(clusterName string) []types.Job {
	h.ClusterJobMap.RLock()
	defer h.ClusterJobMap.RUnlock()

	jobMap, ok := h.ClusterJobMap.ClusterJobMap[clusterName]
	if !ok {
		return []types.Job{}
	}

	jobMap.Lock()
	defer jobMap.Unlock()

	jobs := make([]types.Job, 0, len(jobMap.JobMap))
	for _, job := range jobMap.JobMap {
		jobs = append(jobs, job.DeepCopy())
	}
	return jobs
}

// GetJobByID returns a specific job by ID for a given cluster
func (h *EventHandler) GetJobByID(clusterName, jobID string) (types.Job, bool) {
	h.ClusterJobMap.RLock()
	defer h.ClusterJobMap.RUnlock()

	jobMap, ok := h.ClusterJobMap.ClusterJobMap[clusterName]
	if !ok {
		return types.Job{}, false
	}

	jobMap.Lock()
	defer jobMap.Unlock()

	job, ok := jobMap.JobMap[jobID]
	if !ok {
		return types.Job{}, false
	}
	return job.DeepCopy(), true
}

// TriggerRefresh provides a way to manually trigger data refresh
// This is useful for immediately loading new clusters/sessions without waiting for the timer
func (h *EventHandler) TriggerRefresh() error {
	logrus.Info("[EventHandler] Manual refresh triggered")
	// Note: This is a simplified implementation. In production, you would want to
	// trigger the actual processAllEvents() function. For now, we just log it.
	// The actual implementation would require refactoring Run() to expose processAllEvents.
	return nil
}

// isFileProcessed checks if a file has been processed
func (h *EventHandler) isFileProcessed(filePath string) bool {
	h.filesMutex.RLock()
	defer h.filesMutex.RUnlock()
	return h.processedFiles[filePath]
}

// markFileProcessed marks a file as processed
func (h *EventHandler) markFileProcessed(filePath string) {
	h.filesMutex.Lock()
	defer h.filesMutex.Unlock()
	h.processedFiles[filePath] = true
}

// updateSessionTimestamp updates the timestamp for a session
func (h *EventHandler) updateSessionTimestamp(clusterKey string, timestamp time.Time) {
	h.sessionMutex.Lock()
	defer h.sessionMutex.Unlock()
	h.sessionTimestamps[clusterKey] = timestamp
}

// cleanupExpiredSessions removes sessions older than the specified duration
func (h *EventHandler) cleanupExpiredSessions(maxAge time.Duration) {
	now := time.Now()
	expiredKeys := make([]string, 0)

	// Find expired sessions
	h.sessionMutex.RLock()
	for clusterKey, timestamp := range h.sessionTimestamps {
		if now.Sub(timestamp) > maxAge {
			expiredKeys = append(expiredKeys, clusterKey)
		}
	}
	h.sessionMutex.RUnlock()

	if len(expiredKeys) == 0 {
		return
	}

	logrus.Infof("[EventHandler] Cleaning up %d expired sessions (older than %v)", len(expiredKeys), maxAge)

	// Remove expired data
	for _, clusterKey := range expiredKeys {
		// Remove from tasks
		h.ClusterTaskMap.Lock()
		delete(h.ClusterTaskMap.ClusterTaskMap, clusterKey)
		h.ClusterTaskMap.Unlock()

		// Remove from actors
		h.ClusterActorMap.Lock()
		delete(h.ClusterActorMap.ClusterActorMap, clusterKey)
		h.ClusterActorMap.Unlock()

		// Remove from jobs
		h.ClusterJobMap.Lock()
		delete(h.ClusterJobMap.ClusterJobMap, clusterKey)
		h.ClusterJobMap.Unlock()

		// Remove from session timestamps
		h.sessionMutex.Lock()
		delete(h.sessionTimestamps, clusterKey)
		h.sessionMutex.Unlock()

		logrus.Infof("[EventHandler] Cleaned up expired session: %s", clusterKey)
	}
}

// getRefreshInterval reads the refresh interval from environment variable
// Returns default of 5 minutes if not set or invalid
func getRefreshInterval() time.Duration {
	defaultInterval := 5 * time.Minute
	envValue := os.Getenv("HISTORYSERVER_REFRESH_INTERVAL")
	
	if envValue == "" {
		logrus.Infof("[EventHandler] Using default refresh interval: %v", defaultInterval)
		return defaultInterval
	}

	// Try parsing as duration string (e.g., "5m", "1h")
	if duration, err := time.ParseDuration(envValue); err == nil {
		logrus.Infof("[EventHandler] Using refresh interval from env: %v", duration)
		return duration
	}

	// Try parsing as minutes (e.g., "5" means 5 minutes)
	if minutes, err := strconv.Atoi(envValue); err == nil && minutes > 0 {
		duration := time.Duration(minutes) * time.Minute
		logrus.Infof("[EventHandler] Using refresh interval from env: %v minutes", minutes)
		return duration
	}

	logrus.Warnf("[EventHandler] Invalid HISTORYSERVER_REFRESH_INTERVAL value '%s', using default: %v", envValue, defaultInterval)
	return defaultInterval
}

// getSessionMaxAge reads the session max age from environment variable
// Returns default of 24 hours if not set or invalid
func getSessionMaxAge() time.Duration {
	defaultMaxAge := 24 * time.Hour
	envValue := os.Getenv("HISTORYSERVER_SESSION_MAX_AGE")
	
	if envValue == "" {
		return defaultMaxAge
	}

	// Try parsing as duration string (e.g., "24h", "7d")
	if duration, err := time.ParseDuration(envValue); err == nil {
		logrus.Infof("[EventHandler] Using session max age from env: %v", duration)
		return duration
	}

	// Try parsing as hours (e.g., "24" means 24 hours)
	if hours, err := strconv.Atoi(envValue); err == nil && hours > 0 {
		duration := time.Duration(hours) * time.Hour
		logrus.Infof("[EventHandler] Using session max age from env: %v hours", hours)
		return duration
	}

	logrus.Warnf("[EventHandler] Invalid HISTORYSERVER_SESSION_MAX_AGE value '%s', using default: %v", envValue, defaultMaxAge)
	return defaultMaxAge
}
