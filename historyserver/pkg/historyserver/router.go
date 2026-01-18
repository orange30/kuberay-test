package historyserver

import (
	"bufio"
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
	"sort"
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

func routerTimezone(s *ServerHandler) {
	ws := new(restful.WebService)
	defer restful.Add(ws)
	ws.Path("/timezone").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON).Filter(RequestLogFilter)
	ws.Route(ws.GET("/").To(s.getTimezone).
		Doc("get timezone").
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
	routerTimezone(s)
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

	// For file-based operations (logs), we need the physical folder structure key (Cluster_Namespace)
	physicalClusterKey := clusterNameID + "_" + clusterNamespace

	data, err := s.GetNodes(physicalClusterKey, sessionName)
	if data == nil {
		logrus.Errorf("Failed to get nodes for cluster %s", physicalClusterKey)
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

	// Ray Dashboard expects this endpoint to exist even if there are no events.
	// Prefer returning an empty list (200) over 501 to keep UI functional.
	jobID := strings.TrimSpace(req.QueryParameter("job_id"))
	view := strings.TrimSpace(req.QueryParameter("view"))
	_ = view // Currently ignored; returned payload shape is the same.

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)
	physicalClusterKey := clusterNameID + "_" + clusterNamespace

	events := make([]dashboardEvent, 0)
	const maxEvents = 2000

	// Best-effort: parse Ray JSONL events from object storage.
	// In our MinIO layout, events are typically under:
	// - <session>/node_events/<nodeid>-YYYY-MM-DD-HH
	// - <session>/job_events/<jobIdBase64>/<nodeid>-YYYY-MM-DD-HH
	// If these don't exist, return empty events (still 200).
	if jobID != "" {
		jobDir := resolveJobEventsDir(sessionName, jobID)
		readEventsFromDir(s.reader, physicalClusterKey, jobDir, &events, maxEvents)
	} else {
		nodeDir := path.Join(sessionName, "node_events")
		readEventsFromDir(s.reader, physicalClusterKey, nodeDir, &events, maxEvents)
	}

	if jobID != "" {
		resp.WriteAsJson(map[string]any{
			"result": true,
			"msg":    "success",
			"data": map[string]any{
				"jobId":  jobID,
				"events": events,
			},
		})
		return
	}

	resp.WriteAsJson(map[string]any{
		"result": true,
		"msg":    "success",
		"data": map[string]any{
			"events": map[string]any{
				"global": events,
			},
		},
	})
}

func (s *ServerHandler) getTimezone(req *restful.Request, resp *restful.Response) {
	// Ray Dashboard calls GET /timezone very early during app startup.
	// In historyserver mode, we can serve a local best-effort response.
	// In live mode (cookies present), proxy to the live dashboard.
	if sessionCookie, err := req.Request.Cookie(COOKIE_SESSION_NAME_KEY); err == nil && sessionCookie.Value == "live" {
		clusterNameCookie, err1 := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
		clusterNamespaceCookie, err2 := req.Request.Cookie(COOKIE_CLUSTER_NAMESPACE_KEY)
		if err1 == nil && err2 == nil {
			svcName, err := getClusterSvcName(s.clientManager.clients, clusterNameCookie.Value, clusterNamespaceCookie.Value)
			if err == nil {
				req.SetAttribute(ATTRIBUTE_SERVICE_NAME, svcName)
				s.redirectRequest(req, resp)
				return
			}
		}
	}

	resp.WriteAsJson(currentTimezoneInfo())
}

func currentTimezoneInfo() map[string]string {
	// Match Ray dashboard response shape: {"offset": "+08:00", "value": "Asia/Shanghai"}
	_, off := time.Now().Zone() // seconds east of UTC
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	hours := off / 3600
	minutes := (off % 3600) / 60
	offset := fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)

	// Best-effort mapping from offset to a representative IANA timezone.
	// This mirrors Ray's behavior of returning a canonical timezone per offset.
	offsetToValue := map[string]string{
		"-12:00": "Etc/GMT+12",
		"-11:00": "Pacific/Pago_Pago",
		"-10:00": "Pacific/Honolulu",
		"-09:00": "America/Anchorage",
		"-08:00": "America/Los_Angeles",
		"-07:00": "America/Phoenix",
		"-06:00": "America/Guatemala",
		"-05:00": "America/Bogota",
		"-04:00": "America/Halifax",
		"-03:30": "America/St_Johns",
		"-03:00": "America/Sao_Paulo",
		"-02:00": "America/Godthab",
		"-01:00": "Atlantic/Azores",
		"+00:00": "Etc/UTC",
		"+01:00": "Europe/Amsterdam",
		"+02:00": "Asia/Amman",
		"+03:00": "Asia/Baghdad",
		"+03:30": "Asia/Tehran",
		"+04:00": "Asia/Dubai",
		"+04:30": "Asia/Kabul",
		"+05:00": "Asia/Karachi",
		"+05:30": "Asia/Kolkata",
		"+05:45": "Asia/Kathmandu",
		"+06:00": "Asia/Almaty",
		"+06:30": "Asia/Yangon",
		"+07:00": "Asia/Bangkok",
		"+08:00": "Asia/Shanghai",
		"+09:00": "Asia/Irkutsk",
		"+09:30": "Australia/Adelaide",
		"+10:00": "Australia/Brisbane",
		"+11:00": "Asia/Magadan",
		"+12:00": "Pacific/Auckland",
		"+13:00": "Pacific/Tongatapu",
	}

	value := offsetToValue[offset]
	if value == "" {
		loc := time.Now().Location().String()
		if loc != "" && loc != "Local" {
			value = loc
		} else {
			value = "Etc/UTC"
		}
	}

	return map[string]string{
		"offset": offset,
		"value":  value,
	}
}

func resolveJobEventsDir(sessionName, jobID string) string {
	// job_events directory name in storage is Base64 (with padding).
	// Dashboard may send hex (our newer API shape) or base64 (older shape).
	jobID = strings.TrimSpace(jobID)
	jobDirName := jobID
	// Try hex -> base64 conversion; if it fails, hexToBase64 returns original.
	if converted := hexToBase64(strings.ToLower(jobID)); converted != "" {
		jobDirName = converted
	}
	return path.Join(sessionName, "job_events", jobDirName)
}

type listFilesReader interface {
	ListFiles(rayClusterNameID, dir string) []string
	GetContent(rayClusterNameID, filePath string) io.Reader
}

func readEventsFromDir(reader listFilesReader, physicalClusterKey, dir string, out *[]dashboardEvent, maxEvents int) {
	if dir == "" {
		return
	}
	files := reader.ListFiles(physicalClusterKey, dir)
	sort.Strings(files)
	for _, filename := range files {
		if len(*out) >= maxEvents {
			return
		}
		if filename == "" {
			continue
		}
		// Skip directory-like entries.
		if strings.HasSuffix(filename, "/") {
			continue
		}
		filePath := path.Join(dir, filename)
		r := reader.GetContent(physicalClusterKey, filePath)
		if r == nil {
			continue
		}

		// Robust parsing: support
		// 1) JSONL (multiple JSON objects separated by newlines)
		// 2) Multi-line JSON objects
		// 3) A single JSON array containing multiple objects
		dec := json.NewDecoder(bufio.NewReader(r))
		dec.UseNumber()
		decoded := 0
		for {
			if len(*out) >= maxEvents {
				return
			}
			var v any
			err := dec.Decode(&v)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				// Best-effort: avoid failing the whole endpoint if a file is malformed.
				logrus.Debugf("Failed to decode events file %s: %v", filePath, err)
				break
			}
			decoded++
			switch t := v.(type) {
			case map[string]any:
				*out = append(*out, dashboardEventFromRaw(t))
			case []any:
				for _, item := range t {
					if len(*out) >= maxEvents {
						return
					}
					raw, ok := item.(map[string]any)
					if !ok {
						continue
					}
					*out = append(*out, dashboardEventFromRaw(raw))
				}
			default:
				// Ignore non-object items.
			}
		}
		_ = decoded
	}
}

type dashboardEvent struct {
	EventID        string         `json:"eventId"`
	JobID          string         `json:"jobId"`
	NodeID         string         `json:"nodeId"`
	SourceType     string         `json:"sourceType"`
	SourceHostname string         `json:"sourceHostname"`
	HostName       string         `json:"hostName"`
	SourcePid      int            `json:"sourcePid"`
	Pid            int            `json:"pid"`
	Label          string         `json:"label"`
	Message        string         `json:"message"`
	Timestamp      int64          `json:"timestamp"`
	TimeStamp      int64          `json:"timeStamp"`
	JobName        string         `json:"jobName"`
	Severity       string         `json:"severity"`
	CustomFields   map[string]any `json:"customFields"`
}

func dashboardEventFromRaw(raw map[string]any) dashboardEvent {
	getString := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := raw[k]; ok {
				s, _ := v.(string)
				if s != "" {
					return s
				}
			}
		}
		return ""
	}
	getInt := func(keys ...string) int {
		for _, k := range keys {
			if v, ok := raw[k]; ok {
				switch t := v.(type) {
				case float64:
					return int(t)
				case int:
					return t
				case int64:
					return int(t)
				case json.Number:
					i, err := t.Int64()
					if err == nil {
						return int(i)
					}
				case string:
					// best-effort
					var n json.Number = json.Number(t)
					i, err := n.Int64()
					if err == nil {
						return int(i)
					}
				}
			}
		}
		return 0
	}
	getInt64 := func(keys ...string) int64 {
		for _, k := range keys {
			if v, ok := raw[k]; ok {
				switch t := v.(type) {
				case float64:
					return int64(t)
				case int:
					return int64(t)
				case int64:
					return t
				case json.Number:
					i, err := t.Int64()
					if err == nil {
						return i
					}
				case string:
					var n json.Number = json.Number(t)
					i, err := n.Int64()
					if err == nil {
						return i
					}
				}
			}
		}
		return 0
	}

	knownKeys := map[string]struct{}{}
	markKnown := func(keys ...string) {
		for _, k := range keys {
			knownKeys[k] = struct{}{}
		}
	}

	eventID := getString("eventId", "event_id", "eventID")
	markKnown("eventId", "event_id", "eventID")
	jobID := normalizeMaybeBase64ID(getString("jobId", "job_id", "jobID"))
	markKnown("jobId", "job_id", "jobID")
	nodeID := normalizeMaybeBase64ID(getString("nodeId", "node_id", "nodeID"))
	markKnown("nodeId", "node_id", "nodeID")
	sourceType := getString("sourceType", "source_type")
	markKnown("sourceType", "source_type")
	sourceHostname := getString("sourceHostname", "source_hostname")
	markKnown("sourceHostname", "source_hostname")
	hostName := getString("hostName", "hostname", "host_name")
	markKnown("hostName", "hostname", "host_name")
	sourcePid := getInt("sourcePid", "source_pid")
	markKnown("sourcePid", "source_pid")
	pid := getInt("pid")
	markKnown("pid")
	label := getString("label")
	markKnown("label")
	message := getString("message", "msg")
	markKnown("message", "msg")
	timestamp := getInt64("timestamp", "timeStamp", "time_stamp")
	markKnown("timestamp", "timeStamp", "time_stamp")
	jobName := getString("jobName", "job_name")
	markKnown("jobName", "job_name")
	severity := getString("severity", "level")
	markKnown("severity", "level")

	customFields := map[string]any{}
	if cf, ok := raw["customFields"].(map[string]any); ok {
		customFields = cf
		markKnown("customFields")
	} else if cf, ok := raw["custom_fields"].(map[string]any); ok {
		customFields = cf
		markKnown("custom_fields")
	} else {
		for k, v := range raw {
			if _, ok := knownKeys[k]; ok {
				continue
			}
			customFields[k] = v
		}
	}

	// Keep both timestamp fields for backward compatibility.
	if timestamp == 0 {
		// Some producers may use microseconds; leave as-is (best effort).
	}

	return dashboardEvent{
		EventID:        eventID,
		JobID:          jobID,
		NodeID:         nodeID,
		SourceType:     sourceType,
		SourceHostname: sourceHostname,
		HostName:       hostName,
		SourcePid:      sourcePid,
		Pid:            pid,
		Label:          label,
		Message:        message,
		Timestamp:      timestamp,
		TimeStamp:      timestamp,
		JobName:        jobName,
		Severity:       severity,
		CustomFields:   customFields,
	}
}

func normalizeMaybeBase64ID(id string) string {
	if id == "" {
		return ""
	}
	// Heuristic: if it looks like base64, convert to hex for dashboard consistency.
	if strings.HasSuffix(id, "=") || strings.ContainsAny(id, "+/") {
		hexID := base64ToHex(id)
		if hexID != "" {
			return hexID
		}
	}
	return id
}

func bytesTrimSpace(b []byte) []byte {
	// Small helper to avoid importing bytes for a single use.
	start := 0
	for start < len(b) {
		switch b[start] {
		case ' ', '\n', '\r', '\t':
			start++
			continue
		}
		break
	}
	end := len(b)
	for end > start {
		switch b[end-1] {
		case ' ', '\n', '\r', '\t':
			end--
			continue
		}
		break
	}
	return b[start:end]
}

func (s *ServerHandler) getPrometheusHealth(req *restful.Request, resp *restful.Response) {
	sessionName, _ := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	if s.rayPrometheusHost == "" {
		resp.WriteAsJson(map[string]interface{}{
			"result": false,
			"msg":    "Prometheus host not configured",
		})
		return
	}

	resp.WriteAsJson(map[string]interface{}{
		"result":          true,
		"msg":             "success",
		"prometheus_host": s.rayPrometheusHost,
	})
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
	if sessionName != "" {
		clusterKey = clusterKey + "_" + sessionName
	}

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
	if sessionName != "" {
		clusterKey = clusterKey + "_" + sessionName
	}

	// Get specific job from EventHandler
	// Convert potential hex ID (frontend) to base64 (internal)
	internalID := hexToBase64(jobID)
	job, found := s.eventHandler.GetJobByID(clusterKey, internalID)

	// If not found with converted ID, try original ID as fallback
	if !found && internalID != jobID {
		job, found = s.eventHandler.GetJobByID(clusterKey, jobID)
	}

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
		"job_id":                    base64ToHex(job.JobID),
		"type":                      job.Type,
		"status":                    normalizeJobStatus(job.Status),
		"entrypoint":                job.Entrypoint,
		"message":                   nil,
		"error_type":                nil,
		"start_time":                nil,
		"end_time":                  nil,
		"metadata":                  nil,
		"runtime_env":               nil,
		"driver_info":               nil,
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
	if len(job.Metadata) > 0 {
		result["metadata"] = job.Metadata
	} else {
		result["metadata"] = map[string]string{}
	}

	// Set runtime_env if available (return empty object instead of null for better display)
	if len(job.RuntimeEnv) > 0 {
		result["runtime_env"] = job.RuntimeEnv
	} else {
		result["runtime_env"] = map[string]interface{}{}
	}

	// Set driver_info if available
	// Must match the DriverInfo structure expected by Dashboard
	if job.DriverInfo != nil {
		result["driver_info"] = map[string]interface{}{
			"id":              base64ToHex(job.DriverInfo.ID),
			"node_ip_address": job.DriverInfo.NodeIPAddress,
			"node_id":         base64ToHex(job.DriverInfo.NodeID),
			"pid":             job.DriverInfo.PID,
		}
	}

	// Set driver_agent_http_address if available
	if job.DriverAgentHTTPAddress != "" {
		result["driver_agent_http_address"] = job.DriverAgentHTTPAddress
	}

	// Set driver_node_id if available
	if job.DriverNodeID != "" {
		result["driver_node_id"] = base64ToHex(job.DriverNodeID)
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
	if sessionName != "" {
		clusterKey = clusterKey + "_" + sessionName
	}

	// Convert nodeID from Hex to Base64 format
	// Ray Base Events use Base64 encoding, but URLs/logs use Hex encoding
	nodeIDBase64 := hexToBase64(nodeID)

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
		"now":      0,
		"hostname": "UNKNOWN", // Historical data doesn't have hostname
		"ip":       nodeIP,
		"cpu":      0,
		"bootTime": 0,
		"loadAvg": [][]float64{
			{0, 0, 0}, // System load averages (1min, 5min, 15min)
			{0, 0, 0}, // Per-CPU load averages
		},
		"networkSpeed": []float64{0, 0},          // [sendBps, receiveBps]
		"mem":          []float64{0, 0, 0},       // [total, free, percent]
		"cpus":         []int{0, 0},              // [logical, physical]
		"disk":         map[string]interface{}{}, // Disk usage by mount point
		"cmdline":      []string{},               // Command line
		"state":        "DEAD",                   // Node state
		"logCounts":    0,                        // Log count
		"errorCounts":  0,                        // Error count
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

	// Ray Dashboard expects this endpoint to exist.
	// In history mode, we currently don't reconstruct Serve state, so return an empty payload (200)
	// to keep the UI functional.
	resp.WriteAsJson(map[string]any{
		"grpc_options": map[string]any{
			"port": 0,
		},
		"proxy_location": "Disabled",
		"controller_info": map[string]any{
			"node_id":       nil,
			"node_ip":       nil,
			"actor_id":      nil,
			"actor_name":    nil,
			"worker_id":     nil,
			"log_file_path": nil,
		},
		"proxies":      nil,
		"applications": map[string]any{},
	})
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

	clusterNameID := req.Attribute(COOKIE_CLUSTER_NAME_KEY).(string)
	clusterNamespace := req.Attribute(COOKIE_CLUSTER_NAMESPACE_KEY).(string)

	// For object storage paths, we use the physical folder key: <clusterNameID>_<namespace>
	physicalClusterKey := clusterNameID + "_" + clusterNamespace

	// For in-memory state, we use the logical key: <clusterNameID>_<namespace>_<sessionName>
	clusterKey := physicalClusterKey
	if sessionName != "" {
		clusterKey = clusterKey + "_" + sessionName
	}

	nodeIDs := inferNodeIDsForClusterStatus(s.reader, physicalClusterKey, sessionName)
	jobCount := 0
	if s.eventHandler != nil {
		jobCount = len(s.eventHandler.GetJobs(clusterKey))
	}

	// The Ray Dashboard frontend (v2.51.0) expects a string that includes both
	// "Node status" and "Resources" so it can split and render two cards.
	// We provide a best-effort, autoscaler-like text output.
	now := time.Now().UTC().Format("2006-01-02 15:04:05Z")
	clusterStatus := strings.Join([]string{
		fmt.Sprintf("===== Autoscaler status: %s =====", now),
		"Node status",
		"-----",
		"Healthy:",
		fmt.Sprintf("  %d node(s)", len(nodeIDs)),
		"Pending:",
		"  0 node(s)",
		"Recent failures:",
		"  (none)",
		"Jobs:",
		fmt.Sprintf("  %d total", jobCount),
		"",
		"Resources",
		"-----",
		"Usage:",
		"  (unavailable in history mode)",
		"Demands:",
		"  (none)",
		"",
	}, "\n")

	resp.WriteAsJson(map[string]any{
		"result":  true,
		"message": "success",
		"data": map[string]any{
			"clusterStatus": clusterStatus,
		},
		// Keep "msg" for any older clients that used it.
		"msg": "success",
	})
}

func inferNodeIDsForClusterStatus(reader listFilesReader, physicalClusterKey, sessionName string) []string {
	seen := map[string]struct{}{}
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		seen[id] = struct{}{}
	}

	// Prefer the existing logs layout: <session>/logs/<nodeId>/...
	if sessionName != "" {
		logDir := path.Join(sessionName, "logs")
		nodes := reader.ListFiles(physicalClusterKey, logDir)
		for _, n := range nodes {
			// Storage backends may include directory suffix.
			clean := strings.TrimSuffix(n, "/")
			clean = path.Clean(clean)
			if clean == "." || clean == "" {
				continue
			}
			add(clean)
		}
	}

	if len(seen) == 0 && sessionName != "" {
		// Fallback: infer node ids from node_events file names:
		// <session>/node_events/<nodeId>-YYYY-MM-DD-HH
		eventsDir := path.Join(sessionName, "node_events")
		files := reader.ListFiles(physicalClusterKey, eventsDir)
		for _, f := range files {
			f = strings.TrimSuffix(f, "/")
			base := path.Base(f)
			if base == "." || base == "" {
				continue
			}
			if idx := strings.IndexByte(base, '-'); idx > 0 {
				add(base[:idx])
			} else {
				add(base)
			}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	clusterNameID := clusterName + "_" + clusterNamespace
	if sessionName != "" {
		clusterNameID = clusterNameID + "_" + sessionName
	}

	if sessionName == "live" {
		s.redirectRequest(req, resp)
		return
	}

	filterKey := req.QueryParameter("filter_keys")
	filterValue := req.QueryParameter("filter_values")
	filterPredicate := req.QueryParameter("filter_predicates")

	// If filtering by ID fields, convert hex ID to Base64 to match internal storage
	if filterValue != "" && (filterKey == "actor_id" || filterKey == "job_id" ||
		filterKey == "node_id" || filterKey == "worker_id" || filterKey == "placement_group_id") {
		filterValue = hexToBase64(filterValue)
	}

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
		formattedActors[base64ToHex(actor.ActorID)] = formatActorForResponse(actor)
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
	jobIDHex := base64ToHex(actor.JobID)
	pgIDHex := base64ToHex(actor.PlacementGroupID)

	result := map[string]interface{}{
		"actor_id":           actorIDHex,
		"job_id":             jobIDHex,
		"placement_group_id": pgIDHex,
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
	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY).(string)
	clusterNameID := clusterName + "_" + clusterNamespace
	if sessionName != "" {
		clusterNameID = clusterNameID + "_" + sessionName
	}

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
	if sessionName != "" {
		clusterNameID = clusterNameID + "_" + sessionName
	}

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

	// If filtering by ID fields, convert hex ID to Base64 to match internal storage
	if filterValue != "" && (filterKey == "task_id" || filterKey == "job_id" ||
		filterKey == "node_id" || filterKey == "actor_id" ||
		filterKey == "worker_id" || filterKey == "placement_group_id") {
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
	jobIDHex := base64ToHex(task.JobID)
	pgIDHex := base64ToHex(task.PlacementGroupID)

	result := map[string]interface{}{
		"task_id":            taskIDHex,
		"name":               task.Name,
		"attempt_number":     task.AttemptNumber,
		"state":              string(task.State),
		"job_id":             jobIDHex,
		"node_id":            nodeIDHex,
		"actor_id":           actorIDHex,
		"placement_group_id": pgIDHex,
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
