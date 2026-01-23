package historyserver

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/emicklei/go-restful/v3"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func (s *ServerHandler) listClusters(limit int) []utils.ClusterInfo {
	// Initial continuation marker
	logrus.Debugf("Prepare to get list clusters info ...")
	ctx := context.Background()
	liveClusters, _ := s.clientManager.ListRayClusters(ctx)
	liveClusterNames := []string{}
	liveClusterInfos := []utils.ClusterInfo{}
	for _, liveCluster := range liveClusters {
		liveClusterInfo := utils.ClusterInfo{
			Name:            liveCluster.Name,
			Namespace:       liveCluster.Namespace,
			CreateTime:      liveCluster.CreationTimestamp.String(),
			CreateTimeStamp: liveCluster.CreationTimestamp.Unix(),
			SessionName:     "live",
		}
		liveClusterInfos = append(liveClusterInfos, liveClusterInfo)
		liveClusterNames = append(liveClusterNames, liveCluster.Name)
	}
	logrus.Infof("live clusters: %v", liveClusterNames)
	clusters := s.reader.List()
	sort.Sort(utils.ClusterInfoList(clusters))
	if limit > 0 && limit < len(clusters) {
		clusters = clusters[:limit]
	}
	clusters = append(liveClusterInfos, clusters...)
	return clusters
}

func (s *ServerHandler) _getNodeLogs(rayClusterNameID, sessionId, nodeId, dir string) ([]byte, error) {
	logPath := path.Join(sessionId, "logs", nodeId)
	if dir != "" {
		logPath = path.Join(logPath, dir)
	}
	files := s.reader.ListFiles(rayClusterNameID, logPath)
	ret := map[string]interface{}{
		"data": map[string]interface{}{
			"result": map[string]interface{}{
				"padding": files,
			},
		},
	}
	return json.Marshal(ret)
}

func (s *ServerHandler) GetNodes(rayClusterNameID, sessionId string) ([]byte, error) {
	logPath := path.Join(sessionId, "logs")
	nodes := s.reader.ListFiles(rayClusterNameID, logPath)
	templ := map[string]interface{}{
		"result": true,
		"msg":    "Node summary fetched.",
		"data": map[string]interface{}{
			"summary": []map[string]interface{}{},
		},
	}
	nodeSummary := []map[string]interface{}{}
	for _, node := range nodes {
		nodeSummary = append(nodeSummary, map[string]interface{}{
			"raylet": map[string]interface{}{
				"nodeId": path.Clean(node),
				"state":  "ALIVE",
			},
			"ip": "UNKNOWN",
		})
	}
	templ["data"].(map[string]interface{})["summary"] = nodeSummary
	return json.Marshal(templ)
}

func (h *ServerHandler) getGrafanaHealth(req *restful.Request, resp *restful.Response) {
	sessionNameAttr := req.Attribute(COOKIE_SESSION_NAME_KEY)
	if sessionName, ok := sessionNameAttr.(string); ok && sessionName == "live" {
		h.redirectRequest(req, resp)
		return
	}

	grafanaHost := h.rayGrafanaIframeHost
	if grafanaHost == "" {
		grafanaHost = h.rayGrafanaHost
	}
	grafanaHost = strings.TrimRight(grafanaHost, "/")

	if grafanaHost == "" {
		resp.WriteAsJson(map[string]interface{}{
			"result": false,
			"msg":    "Grafana host not configured",
		})
		return
	}

	sessionName := req.Attribute(COOKIE_SESSION_NAME_KEY)
	sessionNameStr, ok := sessionName.(string)
	if sessionName == nil || !ok || sessionNameStr == "" {
		sessionNameStr = "default"
	}

	resp.WriteAsJson(map[string]interface{}{
		"result": true,
		"msg":    "success",
		"data": map[string]interface{}{
			"grafanaHost":  grafanaHost,
			"grafanaOrgId": "1",
			"sessionName":  sessionNameStr,
			"dashboardUids": map[string]string{
				"default":         "rayDefaultDashboard",
				"serve":           "rayServeDashboard",
				"serveDeployment": "rayServeDeploymentDashboard",
				"data":            "rayDataDashboard",
			},
			"dashboardDatasource": "Prometheus",
		},
		// Keep snake_case fields for backward compatibility with any existing clients.
		"grafana_host": grafanaHost,
	})
}

// staticFileHandler serves static files with security hardening
func (s *ServerHandler) staticFileHandler(req *restful.Request, resp *restful.Response) {
	// Get the path parameter
	pathParam := req.PathParameter("path")
	logrus.Infof("Static file request: path=%s", pathParam)

	// Security: Clean the path to prevent directory traversal
	pathParam = filepath.Clean("/" + pathParam)
	pathParam = strings.TrimPrefix(pathParam, "/")

	// Determine prefix based on cookie
	isHomePage := true
	_, err := req.Request.Cookie(COOKIE_CLUSTER_NAME_KEY)
	isHomePage = err != nil

	var prefix string
	if isHomePage {
		prefix = "homepage"
	} else {
		// Security: Whitelist dashboard versions to prevent arbitrary path access
		version := "v2.51.0" // default version
		if versionCookie, err := req.Request.Cookie(COOKIE_DASHBOARD_VERSION_KEY); err == nil {
			// Validate version format: must start with 'v' and contain only alphanumeric, dots, and hyphens
			if isValidDashboardVersion(versionCookie.Value) {
				version = versionCookie.Value
			} else {
				logrus.Warnf("Invalid dashboard version in cookie: %s, using default", versionCookie.Value)
			}
		}
		prefix = filepath.Join(version, "client", "build")
	}

	// Construct the full path
	fullPath := filepath.Join(s.dashboardDir, prefix, "static", pathParam)

	// Security: Ensure the resolved path is still within dashboardDir
	// This prevents path traversal attacks even after filepath.Clean
	absDashboardDir, err := filepath.Abs(s.dashboardDir)
	if err != nil {
		logrus.Errorf("Failed to get absolute path of dashboardDir: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, "Internal server error")
		return
	}

	absFullPath, err := filepath.Abs(fullPath)
	if err != nil {
		logrus.Errorf("Failed to get absolute path: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, "Internal server error")
		return
	}

	// Verify the file is within the dashboard directory
	if !strings.HasPrefix(absFullPath, absDashboardDir) {
		logrus.Warnf("Security: Path traversal attempt detected. Requested: %s, Resolved: %s", pathParam, absFullPath)
		resp.WriteErrorString(http.StatusForbidden, "Access denied")
		return
	}

	logrus.Infof("Serving static file: %s", absFullPath)

	// Check if file exists
	fileInfo, err := os.Stat(absFullPath)
	if os.IsNotExist(err) {
		logrus.Warnf("Static file not found: %s", absFullPath)
		resp.WriteErrorString(http.StatusNotFound, "File not found")
		return
	}
	if err != nil {
		logrus.Errorf("Error checking file: %v", err)
		resp.WriteErrorString(http.StatusInternalServerError, "Internal server error")
		return
	}

	// Security: Don't allow directory listing
	if fileInfo.IsDir() {
		logrus.Warnf("Directory listing not allowed: %s", absFullPath)
		resp.WriteErrorString(http.StatusForbidden, "Directory listing not allowed")
		return
	}

	// Serve the file
	http.ServeFile(resp.ResponseWriter, req.Request, absFullPath)
}

// isValidDashboardVersion validates dashboard version format
// Allows versions like: v2.51.0, v2.50.0-rc1, etc.
func isValidDashboardVersion(version string) bool {
	if len(version) == 0 || len(version) > 20 {
		return false
	}
	// Must start with 'v'
	if !strings.HasPrefix(version, "v") {
		return false
	}
	// Only allow alphanumeric, dots, and hyphens
	for _, c := range version {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-') {
			return false
		}
	}
	return true
}
