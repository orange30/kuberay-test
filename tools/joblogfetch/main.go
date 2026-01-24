package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	// 固定配置
	liveRayDashboard = "http://1.14.227.231:8265"
	historyServer    = "http://119.29.124.26:30080"
	submissionID     = "ray-data-metrics-demo-z4wnr"
	clusterName      = "raycluster-3"
	clusterNamespace = "default"
	maxLines         = 50000

	// 探活超时
	probeTimeout = 3 * time.Second
)

// ./joblogfetch -cluster raycluster-3 -session session_2026-01-22_22-40-55_855005_1 -namespace default -submission-id ray-data-metrics-demo-z4wnr

type clusterInfo struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	SessionName     string `json:"sessionName"`
	CreateTimeStamp int64  `json:"createTimeStamp"`
}

func main() {
	// 支持命令行参数
	argSession := flag.String("session", "", "指定 historyserver session_name（可选，默认自动选最新）")
	argCluster := flag.String("cluster", "", "Ray Cluster Name (Required)")
	argNamespace := flag.String("namespace", "default", "Ray Cluster Namespace")
	argSubmissionID := flag.String("submission-id", "", "Job Submission ID (Required)")

	flag.Parse()

	// 检查必填参数
	if *argCluster == "" || *argSubmissionID == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -cluster <name> -submission-id <id> [-namespace <ns>] [-session <session>]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	submissionID := *argSubmissionID
	clusterName := *argCluster
	clusterNamespace := *argNamespace

	fmt.Fprintf(os.Stderr, "[INFO] joblogfetch starting...\n")
	fmt.Fprintf(os.Stderr, "[INFO] submission_id=%s\n", submissionID)
	fmt.Fprintf(os.Stderr, "[INFO] live_url=%s\n", liveRayDashboard)
	fmt.Fprintf(os.Stderr, "[INFO] history_url=%s\n", historyServer)

	ctx := context.Background()
	var baseURL, cookieHeader, nodeID string
	var err error
	foundInLive := false

	// 1. 尝试从 Live Ray Dashboard 获取
	fmt.Fprintf(os.Stderr, "[STEP 1] Probing live Ray Dashboard (timeout=%v)...\n", probeTimeout)
	if probeLive(ctx, liveRayDashboard, probeTimeout) {
		fmt.Fprintf(os.Stderr, "[SUCCESS] ✓ Live Ray Dashboard is available (%s)\n", liveRayDashboard)

		fmt.Fprintf(os.Stderr, "[STEP 1.1] Try fetching driver_node_id from Live Dashboard...\n")
		// 尝试直接获取 Job 信息，如果 404 说明 Job 可能已经结束并归档了
		nodeID, err = getDriverNodeID(ctx, liveRayDashboard, "", submissionID)
		if err == nil {
			fmt.Fprintf(os.Stderr, "[SUCCESS] Found job in Live Dashboard, driver_node_id=%s\n", nodeID)
			baseURL = liveRayDashboard
			cookieHeader = ""
			foundInLive = true
		} else {
			fmt.Fprintf(os.Stderr, "[WARNING] Failed to find job in Live Dashboard: %v. Will try HistoryServer.\n", err)
		}
	} else {
		fmt.Fprintf(os.Stderr, "[WARNING] ✗ Live Ray Dashboard unavailable, skipping.\n")
	}

	// 2. 如果 Live 没找到，尝试 HistoryServer
	if !foundInLive {
		fmt.Fprintf(os.Stderr, "[BRANCH] Using HistoryServer fallback\n")
		baseURL = historyServer

		// 获取 Session Name
		var sessionName string
		if *argSession != "" {
			fmt.Fprintf(os.Stderr, "[STEP 2] Using manually specified session_name=%s\n", *argSession)
			sessionName = *argSession
		} else {
			fmt.Fprintf(os.Stderr, "[STEP 2] Auto-fetching latest session from %s...\n", historyServer)
			sessionName, err = getSessionName(ctx, historyServer, clusterName, clusterNamespace)
			if err != nil {
				fatalf("[ERROR] get session_name failed: %v", err)
			}
			fmt.Fprintf(os.Stderr, "[SUCCESS] Got latest session_name=%s\n", sessionName)
		}

		cookieHeader = fmt.Sprintf("cluster_name=%s; cluster_namespace=%s; session_name=%s",
			clusterName, clusterNamespace, sessionName)
		fmt.Fprintf(os.Stderr, "[INFO] Cookie header set: %s\n", cookieHeader)

		// 获取 Node ID
		fmt.Fprintf(os.Stderr, "[STEP 3] Fetching driver_node_id from HistoryServer for submission_id=%s...\n", submissionID)
		nodeID, err = getDriverNodeID(ctx, baseURL, cookieHeader, submissionID)
		if err != nil {
			fatalf("[ERROR] get driver_node_id failed from HistoryServer: %v", err)
		}
		fmt.Fprintf(os.Stderr, "[SUCCESS] driver_node_id=%s\n", nodeID)
	}

	// 4. 拉取日志：/api/v0/logs/file
	filename := fmt.Sprintf("job-driver-%s.log", submissionID)
	fmt.Fprintf(os.Stderr, "[STEP 4] Fetching log: filename=%s, lines=%d from %s\n", filename, maxLines, baseURL)
	if err := fetchLogFile(ctx, baseURL, cookieHeader, nodeID, filename, maxLines); err != nil {
		fatalf("[ERROR] fetch log failed: %v", err)
	}
	fmt.Fprintf(os.Stderr, "[SUCCESS] Log fetched successfully\n")
}

// probeLive 快速探测 live Ray Dashboard 是否可用（ms 级别）
// 1) TCP dial（几十 ms）
// 2) HTTP GET /api/version（总超时 ~250ms）
func probeLive(ctx context.Context, baseURL string, timeout time.Duration) bool {
	fmt.Fprintf(os.Stderr, "[PROBE] Parsing URL: %s\n", baseURL)
	u, err := url.Parse(baseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] URL parse error: %v\n", err)
		return false
	}
	host := u.Host
	if host == "" {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] Empty host\n")
		return false
	}
	fmt.Fprintf(os.Stderr, "[PROBE] Target host: %s\n", host)

	// TCP 探活
	tcpTimeout := timeout / 4
	if tcpTimeout > 2000*time.Millisecond {
		tcpTimeout = 2000 * time.Millisecond
	}
	fmt.Fprintf(os.Stderr, "[PROBE] TCP dial (timeout=%v)...\n", tcpTimeout)
	start := time.Now()
	conn, err := net.DialTimeout("tcp", host, tcpTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] TCP dial failed after %v: %v\n", time.Since(start), err)
		return false
	}
	_ = conn.Close()
	fmt.Fprintf(os.Stderr, "[PROBE-OK] TCP dial succeeded in %v\n", time.Since(start))

	// HTTP 探活
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	versionURL := strings.TrimRight(baseURL, "/") + "/api/version"
	fmt.Fprintf(os.Stderr, "[PROBE] HTTP GET %s (timeout=%v)...\n", versionURL, timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, versionURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] HTTP request creation error: %v\n", err)
		return false
	}
	start = time.Now()
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] HTTP request failed after %v: %v\n", time.Since(start), err)
		return false
	}
	defer resp.Body.Close()
	fmt.Fprintf(os.Stderr, "[PROBE-OK] HTTP response status=%d in %v\n", resp.StatusCode, time.Since(start))
	success := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !success {
		fmt.Fprintf(os.Stderr, "[PROBE-FAIL] Unexpected HTTP status: %d\n", resp.StatusCode)
	}
	return success
}

// getSessionName 从 HistoryServer 的 /clusters/ 获取最新的 session_name
func getSessionName(ctx context.Context, baseURL, clusterName, clusterNamespace string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	clustersURL := strings.TrimRight(baseURL, "/") + "/clusters/"
	fmt.Fprintf(os.Stderr, "[SESSION] GET %s\n", clustersURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, clustersURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[SESSION-ERROR] HTTP request failed: %v\n", err)
		return "", err
	}
	defer resp.Body.Close()
	fmt.Fprintf(os.Stderr, "[SESSION] HTTP status: %d\n", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return "", fmt.Errorf("clusters http status: %s, body=%s", resp.Status, strings.TrimSpace(string(b)))
	}

	var clusters []clusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&clusters); err != nil {
		fmt.Fprintf(os.Stderr, "[SESSION-ERROR] JSON decode failed: %v\n", err)
		return "", err
	}
	fmt.Fprintf(os.Stderr, "[SESSION] Total clusters found: %d\n", len(clusters))

	// 过滤匹配的 cluster 并选最新
	candidates := make([]clusterInfo, 0)
	for _, c := range clusters {
		if c.Name == clusterName && c.Namespace == clusterNamespace && c.SessionName != "" && c.SessionName != "live" {
			candidates = append(candidates, c)
			fmt.Fprintf(os.Stderr, "[SESSION] Matched: %s/%s session=%s timestamp=%d\n",
				c.Namespace, c.Name, c.SessionName, c.CreateTimeStamp)
		}
	}
	if len(candidates) == 0 {
		fmt.Fprintf(os.Stderr, "[SESSION-ERROR] No history sessions for %s/%s\n", clusterNamespace, clusterName)
		return "", fmt.Errorf("no history sessions found for %s/%s", clusterNamespace, clusterName)
	}
	fmt.Fprintf(os.Stderr, "[SESSION] Found %d candidate sessions\n", len(candidates))
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreateTimeStamp > candidates[j].CreateTimeStamp
	})
	fmt.Fprintf(os.Stderr, "[SESSION] Selected latest: %s (timestamp=%d)\n",
		candidates[0].SessionName, candidates[0].CreateTimeStamp)
	return candidates[0].SessionName, nil
}

// getDriverNodeID 通过 /api/jobs/{submission_id} 获取 driver_node_id
func getDriverNodeID(ctx context.Context, baseURL, cookieHeader, submissionID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	jobURL := strings.TrimRight(baseURL, "/") + "/api/jobs/" + url.PathEscape(submissionID)
	fmt.Fprintf(os.Stderr, "[JOB] GET %s\n", jobURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jobURL, nil)
	if err != nil {
		return "", err
	}
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
		fmt.Fprintf(os.Stderr, "[JOB] Cookie header set\n")
	}

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[JOB-ERROR] HTTP request failed: %v\n", err)
		return "", err
	}
	defer resp.Body.Close()
	fmt.Fprintf(os.Stderr, "[JOB] HTTP status: %d\n", resp.StatusCode)

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "[JOB-ERROR] Response body (first 500 chars): %s\n",
			truncate(strings.TrimSpace(string(b)), 500))
		return "", fmt.Errorf("job detail http status: %s, body=%s", resp.Status, strings.TrimSpace(string(b)))
	}
	fmt.Fprintf(os.Stderr, "[JOB] Response size: %d bytes\n", len(b))

	var obj any
	if err := json.Unmarshal(b, &obj); err != nil {
		fmt.Fprintf(os.Stderr, "[JOB-ERROR] JSON unmarshal failed: %v\n", err)
		return "", err
	}

	nodeID := findStringField(obj, "driver_node_id")
	if nodeID == "" {
		fmt.Fprintf(os.Stderr, "[JOB-ERROR] driver_node_id not found in response\n")
		return "", errors.New("driver_node_id not found in /api/jobs response")
	}
	fmt.Fprintf(os.Stderr, "[JOB] Found driver_node_id: %s\n", nodeID)
	return nodeID, nil
}

// fetchLogFile 从 /api/v0/logs/file 拉取日志并输出到 stdout
func fetchLogFile(ctx context.Context, baseURL, cookieHeader, nodeID, filename string, lines int) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	q := url.Values{}
	q.Set("node_id", nodeID)
	q.Set("filename", filename)
	q.Set("lines", fmt.Sprintf("%d", lines))
	logURL := strings.TrimRight(baseURL, "/") + "/api/v0/logs/file?" + q.Encode()
	fmt.Fprintf(os.Stderr, "[LOG] GET %s\n", logURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, logURL, nil)
	if err != nil {
		return err
	}
	if cookieHeader != "" {
		req.Header.Set("Cookie", cookieHeader)
		fmt.Fprintf(os.Stderr, "[LOG] Cookie header set\n")
	}
	req.Header.Set("Accept", "*/*")

	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[LOG-ERROR] HTTP request failed: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	fmt.Fprintf(os.Stderr, "[LOG] HTTP status: %d\n", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		fmt.Fprintf(os.Stderr, "[LOG-ERROR] Response body: %s\n", strings.TrimSpace(string(b)))
		return fmt.Errorf("logs/file http status: %s, body=%s", resp.Status, strings.TrimSpace(string(b)))
	}

	fmt.Fprintf(os.Stderr, "[LOG] Streaming log content to stdout...\n")
	n, err := io.Copy(os.Stdout, resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[LOG-ERROR] Stream copy failed after %d bytes: %v\n", n, err)
		return err
	}
	fmt.Fprintf(os.Stderr, "[LOG] Streamed %d bytes\n", n)
	return nil
}

// findStringField 递归查找 JSON 对象中的字符串字段
func findStringField(obj any, key string) string {
	switch v := obj.(type) {
	case map[string]any:
		if raw, ok := v[key]; ok {
			if s, ok := raw.(string); ok {
				return s
			}
		}
		for _, child := range v {
			if s := findStringField(child, key); s != "" {
				return s
			}
		}
	case []any:
		for _, child := range v {
			if s := findStringField(child, key); s != "" {
				return s
			}
		}
	}
	return ""
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
