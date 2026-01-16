package historyserver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	COOKIE_CLUSTER_NAME_KEY      = "cluster_name"
	COOKIE_CLUSTER_NAMESPACE_KEY = "cluster_namespace"
	COOKIE_SESSION_NAME_KEY      = "session_name"
	COOKIE_DASHBOARD_VERSION_KEY = "dashboard_version"

	ATTRIBUTE_SERVICE_NAME = "cluster_service_name"
)

func RequestLogFilter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	logrus.Infof("Received request: %s %s", req.Request.Method, req.Request.URL.String())
	chain.ProcessFilter(req, resp)
}

func routerClusters(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)

	ws.Path("/clusters").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/").To(s.getClusters).
		Doc("get all clusters").
		Writes([]string{}))
}

func routerNodes(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/nodes").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/").To(s.getNodes).Filter(s.CookieHandle).
		Doc("get nodes for a given clusters").Param(ws.QueryParameter("view", "such as summary")).
		Writes(""))
	ws.Route(ws.GET("/{node_id}").To(s.getNode).Filter(s.CookieHandle).
		Doc("get specifical nodes  ").
		Param(ws.PathParameter("node_id", "node_id")).
		Writes(""))
}

func routerEvents(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/events").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/").To(s.getEvents).Filter(s.CookieHandle).
		Doc("get events").
		Writes(""))
}

func routerAPI(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/api").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/cluster_status").To(s.getClusterStatus).Filter(s.CookieHandle).
		Doc("get clusters status").Param(ws.QueryParameter("format", "such as 1")).
		Writes("")) // Placeholder for specific return type
	ws.Route(ws.GET("/grafana_health").To(s.getGrafanaHealth).Filter(s.CookieHandle).
		Doc("get grafana_health").
		Writes("")) // Placeholder for specific return type
	ws.Route(ws.GET("/prometheus_health").To(s.getPrometheusHealth).Filter(s.CookieHandle).
		Doc("get prometheus_health").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/jobs").To(s.getJobs).Filter(s.CookieHandle).
		Doc("get jobs").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/jobs/{job_id}").To(s.getJob).Filter(s.CookieHandle).
		Doc("get single job").
		Param(ws.PathParameter("job_id", "job_id")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/data/datasets/{job_id}").To(s.getDatasets).Filter(s.CookieHandle).
		Doc("get datasets").
		Param(ws.PathParameter("job_id", "job_id")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/serve/applications/").To(s.getServeApplications).Filter(s.CookieHandle).
		Doc("get appliations").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/placement_groups/").To(s.getPlacementGroups).Filter(s.CookieHandle).
		Doc("get placement_groups").
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/logs").To(s.getNodeLogs).Filter(s.CookieHandle).
		Doc("get appliations").Param(ws.QueryParameter("node_id", "node_id")).
		Writes("")) // Placeholder for specific return type
	ws.Route(ws.GET("/v0/logs/file").To(s.getNodeLogFile).Filter(s.CookieHandle).
		Doc("get logfile").Param(ws.QueryParameter("node_id", "node_id")).
		Param(ws.QueryParameter("task_id", "task_id")).
		Param(ws.QueryParameter("filename", "filename")).
		Param(ws.QueryParameter("suffix", "suffix")).
		Param(ws.QueryParameter("lines", "lines")).
		Param(ws.QueryParameter("format", "format")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/tasks").To(s.getTaskDetail).Filter(s.CookieHandle).
		Doc("get task detail ").
		Param(ws.QueryParameter("limit", "maximum number of results to return")).
		Param(ws.QueryParameter("detail", "return detailed information (1=true, 0=false)")).
		Param(ws.QueryParameter("filter_keys", "filter_keys")).
		Param(ws.QueryParameter("filter_predicates", "filter_predicates")).
		Param(ws.QueryParameter("filter_values", "filter_values")).
		Writes("")) // Placeholder for specific return type

	ws.Route(ws.GET("/v0/tasks/summarize").To(s.getTaskSummarize).Filter(s.CookieHandle).
		Doc("get summarize").
		Param(ws.QueryParameter("filter_keys", "filter_keys")).
		Param(ws.QueryParameter("filter_predicates", "filter_predicates")).
		Param(ws.QueryParameter("filter_values", "filter_values")).
		Param(ws.QueryParameter("summary_by", "summary_by")).
		Writes("")) // Placeholder for specific return type
}

func routerRoot(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Filter(RequestLogFilter)
	ws.Route(ws.GET("/").To(func(req *restful.Request, w *restful.Response) {
		isHomePage := true
		_, err := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
		isHomePage = err != nil
		prefix := ""
		if isHomePage {
			prefix = "homepage"
		} else {
			version := "v2.51.0"
			if versionCookie, err := req.Request.Cookie(COOKIE_DASHBOARD_VERSION_KEY); err == nil {
				version = versionCookie.Value
			}
			prefix = version + "/client/build"
		}
		// Check if homepage file exists; if so use it, otherwise use default index.html
		homepagePath := path.Join(s.dashboardDir, prefix, "index.html")

		var data []byte

		if _, statErr := os.Stat(homepagePath); !os.IsNotExist(statErr) {
			data, err = os.ReadFile(homepagePath)
		} else {
			http.Error(w, "could not read HTML file", http.StatusInternalServerError)
			logrus.Errorf("could not read HTML file: %v", statErr)
			return
		}

		if err != nil {
			http.Error(w, "could not read HTML file", http.StatusInternalServerError)
			logrus.Errorf("could not read HTML file: %v", err)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(data)
	}).Writes(""))
}

// TODO: this is the frontend's entry.
func routerHomepage(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/homepage").Consumes("*/*").Produces("*/*").Filter(RequestLogFilter)
	ws.Route(ws.GET("/").To(func(_ *restful.Request, w *restful.Response) {
		data, err := os.ReadFile(path.Join(s.dashboardDir, "homepage/index.html"))
		if err != nil {
			// Fallback to root path
			routerRoot(s)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(data)
	}).Writes(""))
}

func routerStatic(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/static").Consumes("*/*").Produces("*/*").Filter(RequestLogFilter)
	ws.Route(ws.GET("/{path:*}").To(s.staticFileHandler).
		Doc("Get static file or directory").
		Param(ws.PathParameter("path", "path of the static file").DataType("string")))
}

func routerLogout(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/logout").Consumes("*/*").Produces("*/*").Filter(RequestLogFilter)
	ws.Route(ws.GET("/").To(func(req *restful.Request, w *restful.Response) {
		// Clear all cluster-related cookies
		http.SetCookie(w, &http.Cookie{MaxAge: -1, Path: "/", Name: COOKIE_CLUSTER_NAME_KEY})
		http.SetCookie(w, &http.Cookie{MaxAge: -1, Path: "/", Name: COOKIE_CLUSTER_NAMESPACE_KEY})
		http.SetCookie(w, &http.Cookie{MaxAge: -1, Path: "/", Name: COOKIE_SESSION_NAME_KEY})
		http.SetCookie(w, &http.Cookie{MaxAge: -1, Path: "/", Name: COOKIE_DASHBOARD_VERSION_KEY})
		// Redirect to homepage
		http.Redirect(w, req.Request, "/", http.StatusFound)
	}).Doc("Logout and clear session cookies").Writes(""))
}

func routerHealthz(s *ServerHandler) {

	http.HandleFunc("/readz", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Received request: %s %s", r.Method, r.URL.String())
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
		logrus.Debugf("request /readz")
	})
	http.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		logrus.Infof("Received request: %s %s", r.Method, r.URL.String())
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
		logrus.Debugf("request /livez")
	})

}

func routerLogical(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/logical").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter) //.Filter(s.loginWrapper)
	ws.Route(ws.GET("/actors").To(s.getLogicalActors).Filter(s.CookieHandle).
		Doc("get logical actors").
		Param(ws.QueryParameter("filter_keys", "filter_keys")).
		Param(ws.QueryParameter("filter_predicates", "filter_predicates")).
		Param(ws.QueryParameter("filter_values", "filter_values")).
		Writes("")) // Placeholder for specific return type

	// TODO: discuss with Ray Core team about this
	// I noticed that IDs (`actor_id`, `job_id`, `node_id`, etc.) in Ray Base Events
	// are encoded as Base64, while the Dashboard/State APIs use Hex.
	// Problem: Base64 can contain `/` characters, which breaks URL routing:
	ws.Route(ws.GET("/actors/{single_actor:*}").To(s.getLogicalActor).Filter(s.CookieHandle).
		Doc("get logical single actor").
		Param(ws.PathParameter("single_actor", "single_actor")).
		Writes("")) // Placeholder for specific return type

}

func routerRayClusterSet(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)

	ws.Path("/enter_cluster").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter)
	ws.Route(ws.GET("/{namespace}/{name}/{session}").To(func(r1 *restful.Request, r2 *restful.Response) {
		name := r1.PathParameter("name")
		namespace := r1.PathParameter("namespace")
		session := r1.PathParameter("session")
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAME_KEY, Value: name})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAMESPACE_KEY, Value: namespace})
		http.SetCookie(r2, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_SESSION_NAME_KEY, Value: session})
		r2.WriteJson(map[string]interface{}{
			"result":    "success",
			"name":      name,
			"namespace": namespace,
			"session":   session,
		}, "application/json")
	}).
		Doc("set cookie for cluster").
		Param(ws.PathParameter("namespace", "namespace")).
		Param(ws.PathParameter("name", "name")).
		Param(ws.PathParameter("session", "session")).
		Writes("")) // Placeholder for specific return type
}

func (s *ServerHandler) RegisterRouter() {
	routerRayClusterSet(s)
	routerClusters(s)
	routerNodes(s)
	routerEvents(s)
	routerAPI(s)
	routerRoot(s)
	routerHomepage(s)
	routerStatic(s)
	routerLogout(s)
	routerHealthz(s)
	routerLogical(s)
}

func (s *ServerHandler) redirectRequest(req *restful.Request, resp *restful.Response) {
	svcName := req.Attribute(ATTRIBUTE_SERVICE_NAME).(string)
	remoteResp, err := s.httpClient.Get("http://" + svcName + req.Request.URL.String())
	if err != nil {
		logrus.Errorf("Error: %v", err)
		resp.WriteError(http.StatusBadGateway, err)
		return
	}
	defer remoteResp.Body.Close()

	// Copy headers from remote response
	for key, values := range remoteResp.Header {
		for _, value := range values {
			resp.Header().Add(key, value)
		}
	}

	// Set status code
	resp.WriteHeader(remoteResp.StatusCode)

	// Copy response body
	_, err = io.Copy(resp, remoteResp.Body)
	if err != nil {
		logrus.Errorf("Failed to copy response body: %v", err)
	}
}

func (s *ServerHandler) getClusters(req *restful.Request, resp *restful.Response) {
	clusters := s.listClusters(s.maxClusters)
	resp.WriteAsJson(clusters)
}

// getNodes returns nodes for the specified cluster
func (s *ServerHandler) getNodes(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}
	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	data, err := s.GetNodes(clusterNameID+"_"+clusterNamespace, sessionName)
	if data == nil {
		logrus.Errorf("Failed to get nodes for cluster %s", clusterNameID+"_"+clusterNamespace)
		resp.WriteError(http.StatusInternalServerError, errors.New("failed to get nodes"))
		return
	}
	if err != nil {
		logrus.Errorf("Error: %v", err)
		resp.WriteError(400, err)
		return
	}
	resp.Write(data)
}

func (s *ServerHandler) getEvents(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}
	// Return "not yet supported" for historical data
	resp.WriteErrorString(http.StatusNotImplemented, "Historical events not yet supported")
}

func (s *ServerHandler) getPrometheusHealth(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}
	// Return "not yet supported" for prometheus health
	resp.WriteErrorString(http.StatusNotImplemented, "Prometheus health not yet supported")
}

func (s *ServerHandler) getJobs(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterKey := clusterNameID + "_" + clusterNamespace

	// Get all jobs from EventHandler
	jobs := s.eventHandler.GetJobs(clusterKey)

	// Convert to UnifiedJob format
	unifiedJobs := make([]map[string]interface{}, 0, len(jobs))
	for _, job := range jobs {
		// Enhance timing with task data
		enrichJobTimingFromTasks(s.eventHandler, clusterKey, &job)
		
		unifiedJob := formatJobForResponse(job)
		unifiedJobs = append(unifiedJobs, unifiedJob)
	}

	resp.WriteAsJson(unifiedJobs)
}

func (s *ServerHandler) getJob(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	jobID := req.PathParameter("job_id")

	if jobID == "" {
		resp.WriteErrorString(http.StatusBadRequest, "job_id is required")
		return
	}

	clusterKey := clusterNameID + "_" + clusterNamespace

	// Get specific job from EventHandler
	job, found := s.eventHandler.GetJobByID(clusterKey, jobID)
	if !found {
		resp.WriteErrorString(http.StatusNotFound, "Job not found")
		return
	}

	// Enhance job timing with task data if available
	enrichJobTimingFromTasks(s.eventHandler, clusterKey, &job)

	unifiedJob := formatJobForResponse(job)
	resp.WriteAsJson(unifiedJob)
}

// normalizeJobStatus converts Ray job status to Dashboard-expected format
// Ray uses "FINISHED" but Dashboard expects "SUCCEEDED"
func normalizeJobStatus(status eventtypes.JobStatus) string {
	if status == eventtypes.JOB_FINISHED {
		return "SUCCEEDED"
	}
	return string(status)
}

// enrichJobTimingFromTasks enhances job start/end times using associated task data
func enrichJobTimingFromTasks(eventHandler *eventserver.EventHandler, clusterKey string, job *eventtypes.Job) {
	// Get all tasks for this job
	tasks := eventHandler.GetTasksByJobID(clusterKey, job.JobID)
	if len(tasks) == 0 {
		return
	}

	// Find earliest start time and latest end time from tasks
	var earliestStart, latestEnd time.Time
	for _, task := range tasks {
		if !task.StartTime.IsZero() {
			if earliestStart.IsZero() || task.StartTime.Before(earliestStart) {
				earliestStart = task.StartTime
			}
		}
		if !task.EndTime.IsZero() {
			if latestEnd.IsZero() || task.EndTime.After(latestEnd) {
				latestEnd = task.EndTime
			}
		}
	}

	// Use task timing if job timing is missing or suspicious (same start/end)
	if job.StartTime.IsZero() && !earliestStart.IsZero() {
		job.StartTime = earliestStart
	}
	if job.EndTime.IsZero() && !latestEnd.IsZero() {
		job.EndTime = latestEnd
	}
	
	// If job has same start and end time but tasks show real duration, use task timing
	if !job.StartTime.IsZero() && !job.EndTime.IsZero() && 
		job.StartTime.Equal(job.EndTime) && 
		!earliestStart.IsZero() && !latestEnd.IsZero() {
		job.StartTime = earliestStart
		job.EndTime = latestEnd
	}
}

// formatJobForResponse converts a Job struct to the UnifiedJob format expected by Dashboard
func formatJobForResponse(job eventtypes.Job) map[string]interface{} {
	result := map[string]interface{}{
		"job_id":      job.JobID,
		"type":        job.Type,
		"status":      normalizeJobStatus(job.Status),
		"entrypoint":  job.Entrypoint,
		"message":     nil,
		"error_type":  nil,
		"start_time":  nil,
		"end_time":    nil,
		"metadata":    nil,
		"runtime_env": nil,
		"driver_info": nil,
		"driver_agent_http_address": nil,
		"driver_node_id":            nil,
		"submission_id":             nil,
	}

	// Set submission_id if available
	if job.SubmissionID != "" {
		result["submission_id"] = job.SubmissionID
	}

	// Set message if available
	if job.Message != "" {
		result["message"] = job.Message
	}

	// Set error_type if available
	if job.ErrorType != "" {
		result["error_type"] = job.ErrorType
	}

	// Convert times to milliseconds timestamp
	if !job.StartTime.IsZero() {
		result["start_time"] = job.StartTime.UnixMilli()
	} else if len(job.Events) > 0 {
		// Fallback: use first event timestamp if StartTime not calculated
		result["start_time"] = job.Events[0].Timestamp.UnixMilli()
	}
	
	if !job.EndTime.IsZero() {
		result["end_time"] = job.EndTime.UnixMilli()
	} else if len(job.Events) > 0 && 
		(job.Status == eventtypes.JOB_SUCCEEDED || job.Status == eventtypes.JOB_FAILED || 
		 job.Status == eventtypes.JOB_STOPPED || job.Status == eventtypes.JOB_FINISHED) {
		// Fallback: use last event timestamp for terminal states
		result["end_time"] = job.Events[len(job.Events)-1].Timestamp.UnixMilli()
	}

	// Set metadata if available (return empty object instead of null)
	if job.Metadata != nil && len(job.Metadata) > 0 {
		result["metadata"] = job.Metadata
	} else {
		result["metadata"] = map[string]string{}
	}

	// Set runtime_env if available (return empty object instead of null for better display)
	if job.RuntimeEnv != nil && len(job.RuntimeEnv) > 0 {
		result["runtime_env"] = job.RuntimeEnv
	} else {
		result["runtime_env"] = map[string]interface{}{}
	}

	// Set driver_info if available
	// Must match the DriverInfo structure expected by Dashboard
	if job.DriverInfo != nil {
		result["driver_info"] = map[string]interface{}{
			"id":              job.DriverInfo.ID,
			"node_ip_address": job.DriverInfo.NodeIPAddress,
			"node_id":         job.DriverInfo.NodeID,
			"pid":             job.DriverInfo.PID,
		}
	}

	// Set driver_agent_http_address if available
	if job.DriverAgentHTTPAddress != "" {
		result["driver_agent_http_address"] = job.DriverAgentHTTPAddress
	}

	// Set driver_node_id if available
	if job.DriverNodeID != "" {
		result["driver_node_id"] = job.DriverNodeID
	}

	return result
}

func (s *ServerHandler) getNode(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	nodeID := req.PathParameter("node_id")

	if nodeID == "" {
		resp.WriteErrorString(http.StatusBadRequest, "node_id is required")
		return
	}

	clusterKey := clusterNameID + "_" + clusterNamespace

	// Convert nodeID from Hex to Base64 format
	// Ray Base Events use Base64 encoding, but URLs/logs use Hex encoding
	nodeIDBase64 := nodeID
	if hexBytes, err := hex.DecodeString(nodeID); err == nil {
		nodeIDBase64 = base64.StdEncoding.EncodeToString(hexBytes)
	}

	// Get all actors from EventHandler and filter by nodeID
	actorsMap := s.eventHandler.GetActorsMap(clusterKey)
	
	nodeActors := make(map[string]interface{})
	var nodeIP string
	for actorID, actor := range actorsMap {
		if actor.Address.NodeID == nodeIDBase64 {
			nodeActors[actorID] = formatActorForResponse(actor)
			// Try to extract IP from the first actor on this node
			if nodeIP == "" && actor.Address.IPAddress != "" {
				nodeIP = actor.Address.IPAddress
			}
		}
	}

	// Get tasks running on this node
	allTasks := s.eventHandler.GetTasks(clusterKey)
	
	nodeTasks := []eventtypes.Task{}
	for _, task := range allTasks {
		if task.NodeID == nodeIDBase64 {
			nodeTasks = append(nodeTasks, task)
		}
	}

	// Use IP from actors if available, otherwise mark as UNKNOWN
	if nodeIP == "" {
		nodeIP = "UNKNOWN"
	}

	// Construct basic node detail response with limited historical data
	// Must match TypeScript NodeDetail interface expectations:
	// - loadAvg: [[system1m,5m,15m], [perCpu1m,5m,15m]]
	// - networkSpeed: [sendBps, receiveBps]
	// - mem: [totalBytes, freeBytes, usedPercent]
	// - cpus: [logicalCount, physicalCount]
	nodeDetail := map[string]interface{}{
		"now":       0,
		"hostname":  "UNKNOWN", // Historical data doesn't have hostname
		"ip":        nodeIP,
		"cpu":       0,
		"bootTime":  0,
		"loadAvg": [][]float64{
			{0, 0, 0}, // System load averages (1min, 5min, 15min)
			{0, 0, 0}, // Per-CPU load averages
		},
		"networkSpeed": []float64{0, 0},                                   // [sendBps, receiveBps]
		"mem":          []float64{0, 0, 0},                                // [total, free, percent]
		"cpus":         []int{0, 0},                                       // [logical, physical]
		"disk":         map[string]interface{}{},                          // Disk usage by mount point
		"cmdline":      []string{},                                        // Command line
		"state":        "DEAD",                                            // Node state
		"logCounts":    0,                                                 // Log count
		"errorCounts":  0,                                                 // Error count
		"raylet": map[string]interface{}{
			"nodeId":                     nodeID,
			"state":                      "DEAD",
			"isHeadNode":                 false,
			"numWorkers":                 len(nodeTasks), // Use task count as approximation
			"pid":                        0,
			"startTime":                  0,
			"terminateTime":              -1,
			"objectStoreAvailableMemory": 0,
			"objectStoreUsedMemory":      0,
			"nodeManagerPort":            0,
			"brpcPort":                   0,
			"labels":                     map[string]string{},
		},
		"workers": []interface{}{},
		"actors":  nodeActors,
	}

	response := map[string]interface{}{
		"result": true,
		"msg":    "Node detail fetched from historical data (limited)",
		"data": map[string]interface{}{
			"detail": nodeDetail,
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal node detail response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

func (s *ServerHandler) getDatasets(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Return "not yet supported" for datasets
	resp.WriteErrorString(http.StatusNotImplemented, "Datasets not yet supported")
}

func (s *ServerHandler) getServeApplications(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Return "not yet supported" for serve applications
	resp.WriteErrorString(http.StatusNotImplemented, "Serve applications not yet supported")
}

func (s *ServerHandler) getPlacementGroups(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Return "not yet supported" for placement groups
	resp.WriteErrorString(http.StatusNotImplemented, "Placement groups not yet supported")
}

func (s *ServerHandler) getClusterStatus(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Return "not yet supported" for cluster status
	resp.WriteErrorString(http.StatusNotImplemented, "Cluster status not yet supported")
}

func (s *ServerHandler) getNodeLogs(req *restful.Request, resp *restful.Response) {
	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}
	folder := ""
	if req.QueryParameter("folder") != "" {
		folder = req.QueryParameter("folder")
	}
	if req.QueryParameter("glob") != "" {
		folder = req.QueryParameter("glob")
		folder = strings.TrimSuffix(folder, "*")
	}
	data, err := s._getNodeLogs(clusterNameID+"_"+clusterNamespace, sessionName, req.QueryParameter("node_id"), folder)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		resp.WriteError(400, err)
		return
	}
	resp.Write(data)
}

func (s *ServerHandler) getLogicalActors(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterNameID := clusterName + "_" + clusterNamespace
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	filterKey := req.QueryParameter("filter_keys")
	filterValue := req.QueryParameter("filter_values")
	filterPredicate := req.QueryParameter("filter_predicates")

	// Get actors from EventHandler's in-memory map
	actorsMap := s.eventHandler.GetActorsMap(clusterNameID)

	// Convert map to slice for filtering
	actors := make([]eventtypes.Actor, 0, len(actorsMap))
	for _, actor := range actorsMap {
		actors = append(actors, actor)
	}

	// Apply generic filtering
	actors = utils.ApplyFilter(actors, filterKey, filterPredicate, filterValue,
		func(a eventtypes.Actor, key string) string {
			return eventtypes.GetActorFieldValue(a, key)
		})

	// Format response to match Ray Dashboard API format
	formattedActors := make(map[string]interface{})
	for _, actor := range actors {
		formattedActors[actor.ActorID] = formatActorForResponse(actor)
	}

	response := map[string]interface{}{
		"result": true,
		"msg":    "All actors fetched.",
		"data": map[string]interface{}{
			"actors": formattedActors,
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal actors response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// formatActorForResponse converts an eventtypes.Actor to the format expected by Ray Dashboard
func formatActorForResponse(actor eventtypes.Actor) map[string]interface{} {
	// Convert Base64 IDs to Hex format for Dashboard display
	actorIDHex := base64ToHex(actor.ActorID)
	nodeIDHex := base64ToHex(actor.Address.NodeID)
	workerIDHex := base64ToHex(actor.Address.WorkerID)
	
	result := map[string]interface{}{
		"actor_id":           actorIDHex,
		"job_id":             actor.JobID,
		"placement_group_id": actor.PlacementGroupID,
		"state":              string(actor.State),
		"pid":                actor.PID,
		"address": map[string]interface{}{
			"node_id":    nodeIDHex,
			"ip_address": actor.Address.IPAddress,
			"port":       actor.Address.Port,
			"worker_id":  workerIDHex,
		},
		"name":               actor.Name,
		"num_restarts":       actor.NumRestarts,
		"actor_class":        actor.ActorClass,
		"required_resources": actor.RequiredResources,
		"exit_details":       actor.ExitDetails,
		"repr_name":          actor.ReprName,
		"call_site":          actor.CallSite,
		"is_detached":        actor.IsDetached,
		"ray_namespace":      actor.RayNamespace,
	}

	// Only include start_time if it's set (non-zero)
	if !actor.StartTime.IsZero() {
		result["start_time"] = actor.StartTime.UnixMilli()
	}

	// Only include end_time if it's set (non-zero)
	if !actor.EndTime.IsZero() {
		result["end_time"] = actor.EndTime.UnixMilli()
	}

	return result
}
func (s *ServerHandler) getLogicalActor(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterNameID := clusterName + "_" + clusterNamespace
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	actorID := req.PathParameter("single_actor")

	// Get actor from EventHandler's in-memory map
	actor, found := s.eventHandler.GetActorByID(clusterNameID, actorID)

	replyActorInfo := ReplyActorInfo{
		Data: ActorInfoData{},
	}

	if found {
		replyActorInfo.Result = true
		replyActorInfo.Msg = "Actor fetched."
		replyActorInfo.Data.Detail = formatActorForResponse(actor)
	} else {
		replyActorInfo.Result = false
		replyActorInfo.Msg = "Actor not found."
	}

	actData, err := json.MarshalIndent(&replyActorInfo, "", "  ")
	if err != nil {
		logrus.Errorf("Failed to marshal actor response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}

	resp.Write(actData)
}

func (s *ServerHandler) getNodeLogFile(req *restful.Request, resp *restful.Response) {
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	nodeID := req.QueryParameter("node_id")
	taskID := req.QueryParameter("task_id")
	filename := req.QueryParameter("filename")
	suffix := req.QueryParameter("suffix") // "out" or "err"
	
	// Support both node_id and task_id based log retrieval
	var filePath string
	
	if taskID != "" {
		// Task log: need to convert hex task_id to Base64 and find associated log file
		taskIDBase64 := hexToBase64(taskID)
		
		// Get task to find its node
		clusterKey := clusterNameID + "_" + clusterNamespace
		tasks := s.eventHandler.GetTasks(clusterKey)
		
		var foundTask *eventtypes.Task
		for _, task := range tasks {
			if task.TaskID == taskIDBase64 {
				foundTask = &task
				break
			}
		}
		
		if foundTask == nil {
			resp.WriteErrorString(http.StatusNotFound, "Task not found")
			return
		}
		
		// Build log path based on actual MinIO structure
		// Path: logs/<node_id_hex>/worker-<worker_id_hex>-<job_id_hex>-*.out
		// Note: Ray uses full hex node IDs (without dashes), worker IDs, and job IDs in filenames
		
		if suffix == "" {
			suffix = "out" // default to stdout
		}
		
		// Convert IDs to hex format (all stored as Base64 in events)
		nodeIDHex := base64ToHex(foundTask.NodeID)
		workerIDHex := base64ToHex(foundTask.WorkerID)
		jobIDHex := base64ToHex(foundTask.JobID)
		
		if nodeIDHex == "" {
			logrus.Warnf("Task %s has no NodeID, cannot locate logs", taskID)
			resp.WriteErrorString(http.StatusNotFound, "Task has no associated node, cannot locate logs")
			return
		}
		
		// Build the log directory path
		logDir := path.Join(sessionName, "logs", nodeIDHex)
		
		// List all files in the node's log directory to find matching worker log
		// We need to search because the filename includes additional suffix after job_id
		allFiles := s.reader.ListFiles(clusterNameID+"_"+clusterNamespace, logDir)
		
		// Search for files matching: worker-<worker_id>-<job_id>-*.<suffix>
		var matchedFile string
		searchPrefix := fmt.Sprintf("worker-%s-%s-", workerIDHex, jobIDHex)
		searchSuffix := "." + suffix
		
		for _, filename := range allFiles {
			if strings.HasPrefix(filename, searchPrefix) && strings.HasSuffix(filename, searchSuffix) {
				matchedFile = filename
				logrus.Infof("Found matching log file: %s", filename)
				break
			}
		}
		
		if matchedFile == "" {
			logrus.Warnf("No worker log file found in %s with pattern %s*%s", logDir, searchPrefix, searchSuffix)
			logrus.Infof("Available files in directory: %v", allFiles)
			
			errorMsg := fmt.Sprintf(
				"Task log file not found.\n\n"+
					"Searched in: %s\n"+
					"Pattern: %s*%s\n"+
					"Task info:\n"+
					"  - Task ID: %s\n"+
					"  - Node ID: %s\n"+
					"  - Worker ID: %s\n"+
					"  - Job ID: %s\n\n"+
					"Found %d files in log directory.",
				logDir, searchPrefix, searchSuffix, taskID, nodeIDHex, workerIDHex, jobIDHex, len(allFiles))
			resp.WriteErrorString(http.StatusNotFound, errorMsg)
			return
		}
		
		// Get the log file content
		filePath = path.Join(logDir, matchedFile)
		reader := s.reader.GetContent(clusterNameID+"_"+clusterNamespace, filePath)
		if reader != nil {
			logrus.Infof("Successfully retrieved task log from: %s", filePath)
			
			// Set appropriate content type
			resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
			
			// Copy the content from reader to response
			_, err := io.Copy(resp, reader)
			if err != nil {
				logrus.Errorf("Failed to write log file content: %v", err)
				resp.WriteErrorString(http.StatusInternalServerError, "Failed to read log file")
			}
			return
		}
		
		// If we still can't get the file (shouldn't happen)
		resp.WriteErrorString(http.StatusInternalServerError, "Failed to retrieve log file content")
		return
		
	} else if nodeID != "" && filename != "" {
		// Node log: original behavior
		filePath = path.Join(sessionName, "logs", nodeID, filename)
		
		// Get file content from object storage using the reader
		reader := s.reader.GetContent(clusterNameID+"_"+clusterNamespace, filePath)
		if reader == nil {
			logrus.Warnf("Log file not found in storage: %s", filePath)
			resp.WriteErrorString(http.StatusNotFound, fmt.Sprintf("Log file not found: %s", filePath))
			return
		}

		// Set appropriate content type
		resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
		
		// Copy the content from reader to response
		_, err := io.Copy(resp, reader)
		if err != nil {
			logrus.Errorf("Failed to write log file content: %v", err)
			resp.WriteErrorString(http.StatusInternalServerError, "Failed to read log file")
			return
		}
	} else {
		resp.WriteErrorString(http.StatusBadRequest, "Either node_id+filename or task_id is required")
		return
	}
}

func (s *ServerHandler) getTaskSummarize(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	clusterNameID := clusterName + "_" + clusterNamespace
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	// Parse filter parameters
	filterKey := req.QueryParameter("filter_keys")
	filterValue := req.QueryParameter("filter_values")
	filterPredicate := req.QueryParameter("filter_predicates")
	summaryBy := req.QueryParameter("summary_by")

	// Get all tasks
	tasks := s.eventHandler.GetTasks(clusterNameID)

	// Apply generic filtering using utils.ApplyFilter
	tasks = utils.ApplyFilter(tasks, filterKey, filterPredicate, filterValue,
		func(t eventtypes.Task, key string) string {
			return eventtypes.GetTaskFieldValue(t, key)
		})

	// Summarize tasks based on summary_by parameter
	var summary map[string]interface{}
	if summaryBy == "lineage" {
		summary = summarizeTasksByLineage(tasks)
	} else {
		// Default to func_name
		summary = summarizeTasksByFuncName(tasks)
	}

	response := map[string]interface{}{
		"result": true,
		"msg":    "Tasks summarized.",
		"data": map[string]interface{}{
			"result": summary,
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal task summarize response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// summarizeTasksByFuncName groups tasks by function name and counts by state
func summarizeTasksByFuncName(tasks []eventtypes.Task) map[string]interface{} {
	summary := make(map[string]map[string]int)

	for _, task := range tasks {
		funcName := task.FuncOrClassName
		if funcName == "" {
			funcName = "unknown"
		}
		if _, ok := summary[funcName]; !ok {
			summary[funcName] = make(map[string]int)
		}
		state := string(task.State)
		if state == "" {
			state = "UNKNOWN"
		}
		summary[funcName][state]++
	}

	return map[string]interface{}{
		"summary": summary,
		"total":   len(tasks),
	}
}

// TODO(Han-Ju Chen): This function has a bug - using JobID instead of actual lineage.
// Real lineage requires:
// 1. Add ParentTaskID field to Task struct (types/task.go)
// 2. Parse parent_task_id from Ray events (eventserver.go)
// 3. Build task tree structure based on ParentTaskID
// 4. Update rayjob example to generate nested tasks for testing
func summarizeTasksByLineage(tasks []eventtypes.Task) map[string]interface{} {
	summary := make(map[string]map[string]int)

	for _, task := range tasks {
		// Use JobID as a simple lineage grouping for now
		lineageKey := task.JobID
		if lineageKey == "" {
			lineageKey = "unknown"
		}
		if _, ok := summary[lineageKey]; !ok {
			summary[lineageKey] = make(map[string]int)
		}
		state := string(task.State)
		if state == "" {
			state = "UNKNOWN"
		}
		summary[lineageKey][state]++
	}

	return map[string]interface{}{
		"summary": summary,
		"total":   len(tasks),
	}
}

func (s *ServerHandler) getTaskDetail(req *restful.Request, resp *restful.Response) {
	clusterName := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)

	// Combine into internal key format
	clusterNameID := clusterName + "_" + clusterNamespace

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	filterKey := req.QueryParameter("filter_keys")
	filterValue := req.QueryParameter("filter_values")
	filterPredicate := req.QueryParameter("filter_predicates")
	limitStr := req.QueryParameter("limit")
	detailStr := req.QueryParameter("detail")

	// Parse limit parameter (default: no limit)
	var limit int
	if limitStr != "" {
		if parsedLimit, err := fmt.Sscanf(limitStr, "%d", &limit); err == nil && parsedLimit == 1 && limit > 0 {
			// Valid limit
		} else {
			limit = 0 // No limit if parse fails
		}
	}

	// Parse detail parameter (default: false)
	// detail=1 means return detailed information
	detailed := detailStr == "1" || detailStr == "true"

	// If filtering by task_id, convert hex ID to Base64 for comparison
	if filterKey == "task_id" && filterValue != "" {
		filterValue = hexToBase64(filterValue)
	}

	tasks := s.eventHandler.GetTasks(clusterNameID)
	tasks = utils.ApplyFilter(tasks, filterKey, filterPredicate, filterValue,
		func(t eventtypes.Task, key string) string {
			return eventtypes.GetTaskFieldValue(t, key)
		})

	// Apply limit if specified
	if limit > 0 && len(tasks) > limit {
		tasks = tasks[:limit]
	}

	taskResults := make([]interface{}, 0, len(tasks))
	for _, task := range tasks {
		formatted := formatTaskForResponse(task)
		// If detail=1, include additional fields (for now, we already return all fields)
		if detailed {
			// Future: add more detailed fields here if needed
		}
		taskResults = append(taskResults, formatted)
	}

	response := ReplyTaskInfo{
		Result: true,
		Msg:    "Tasks fetched.",
		Data: TaskInfoData{
			Result: TaskInfoDataResult{
				Result:             taskResults,
				Total:              len(taskResults),
				NumFiltered:        len(taskResults),
				NumAfterTruncation: len(taskResults),
			},
		},
	}

	respData, err := json.Marshal(response)
	if err != nil {
		logrus.Errorf("Failed to marshal task response: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, err.Error())
		return
	}
	resp.Write(respData)
}

// formatTaskForResponse converts an eventtypes.Task to the format expected by Ray Dashboard
func formatTaskForResponse(task eventtypes.Task) map[string]interface{} {
	// Convert Base64 IDs to Hex format for Dashboard display
	taskIDHex := base64ToHex(task.TaskID)
	nodeIDHex := base64ToHex(task.NodeID)
	actorIDHex := base64ToHex(task.ActorID)
	workerIDHex := base64ToHex(task.WorkerID)
	
	result := map[string]interface{}{
		"task_id":            taskIDHex,
		"name":               task.Name,
		"attempt_number":     task.AttemptNumber,
		"state":              string(task.State),
		"job_id":             task.JobID,
		"node_id":            nodeIDHex,
		"actor_id":           actorIDHex,
		"placement_group_id": task.PlacementGroupID,
		"type":               string(task.Type),
		"func_or_class_name": task.FuncOrClassName,
		"language":           task.Language,
		"required_resources": task.RequiredResources,
		"worker_id":          workerIDHex,
		"error_type":         task.ErrorType,
		"error_message":      task.ErrorMessage,
		"call_site":          task.CallSite,
	}

	// Dashboard expects start_time_ms and end_time_ms (note the _ms suffix)
	// Use -1 for unset times to indicate "not available"
	if !task.StartTime.IsZero() {
		result["start_time_ms"] = task.StartTime.UnixMilli()
	} else {
		result["start_time_ms"] = -1
	}

	if !task.EndTime.IsZero() {
		result["end_time_ms"] = task.EndTime.UnixMilli()
	} else {
		result["end_time_ms"] = -1
	}

	return result
}

// base64ToHex converts a Base64-encoded ID to hexadecimal format
// Ray stores IDs as Base64 but Dashboard displays them as hex
func base64ToHex(base64ID string) string {
	if base64ID == "" {
		return ""
	}
	
	// Try standard encoding first
	decoded, err := base64.StdEncoding.DecodeString(base64ID)
	if err != nil {
		// Try URL encoding if standard fails
		decoded, err = base64.URLEncoding.DecodeString(base64ID)
		if err != nil {
			// Try RawStdEncoding (without padding)
			decoded, err = base64.RawStdEncoding.DecodeString(base64ID)
			if err != nil {
				// If all fail, return original
				return base64ID
			}
		}
	}
	
	// Convert to hex
	return hex.EncodeToString(decoded)
}

// hexToBase64 converts a hexadecimal ID back to Base64 format
// Used for internal lookups when Dashboard sends hex IDs
func hexToBase64(hexID string) string {
	if hexID == "" {
		return ""
	}
	
	// Decode hex
	decoded, err := hex.DecodeString(hexID)
	if err != nil {
		// If decode fails, return original
		return hexID
	}
	
	// Convert to Base64 (standard encoding with padding)
	return base64.StdEncoding.EncodeToString(decoded)
}

// CookieHandle is a preprocessing filter function
func (s *ServerHandler) CookieHandle(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// Get cookie from request
	clusterName, err := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "Cluster Cookie not found")
		return
	}
	sessionName, err := req.Request.Cookie(COOKIE_SESSION_NAME_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "RayCluster Session Name Cookie not found")
		return
	}
	clusterNamespace, err := req.Request.Cookie(COOKIE_CLUSTER_NAMESPACE_KEY)
	if err != nil {
		resp.WriteHeaderAndEntity(http.StatusBadRequest, "Cluster Namespace Cookie not found")
		return
	}
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAME_KEY, Value: clusterName.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_CLUSTER_NAMESPACE_KEY, Value: clusterNamespace.Value})
	http.SetCookie(resp, &http.Cookie{MaxAge: 600, Path: "/", Name: COOKIE_SESSION_NAME_KEY, Value: sessionName.Value})

	if sessionName.Value == "live" {
		// Always query K8s to get the service name to prevent SSRF attacks.
		// Do not trust user-provided cookies for service name.
		// TODO: here might be a bottleneck if there are many requests in the future.
		svcName, err := getClusterSvcName(s.clientManager.clients, clusterName.Value, clusterNamespace.Value)
		if err != nil {
			resp.WriteHeaderAndEntity(http.StatusBadRequest, err.Error())
			return
		}
		req.SetAttribute(ATTRIBUTE_SERVICE_NAME, svcName)
	}
	req.SetAttribute(COOKIE_CLUSTER_NAME_KEY, clusterName.Value)
	req.SetAttribute(COOKIE_SESSION_NAME_KEY, sessionName.Value)
	req.SetAttribute(COOKIE_CLUSTER_NAMESPACE_KEY, clusterNamespace.Value)
	logrus.Infof("Request URL %s", req.Request.URL.String())
	chain.ProcessFilter(req, resp)
}

func getClusterSvcName(clis []client.Client, name, namespace string) (string, error) {
	if len(clis) == 0 {
		return "", errors.New("No available kubernetes config found")
	}
	cli := clis[0]
	rc := rayv1.RayCluster{}
	err := cli.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, &rc)
	if err != nil {
		return "", errors.New("RayCluster not found")
	}
	svcName := rc.Status.Head.ServiceName
	if svcName == "" {
		return "", errors.New("RayCluster head service not ready")
	}
	return svcName + ":8265", nil
}
