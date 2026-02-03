package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type options struct {
	rootDir     string
	clusterKey  string // name_namespace
	sessionName string

	maxKeys  int
	maxPrint int
	verbose  bool
}

func getenvRequired(key string) (string, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return "", fmt.Errorf("missing env %s", key)
	}
	return v, nil
}

func newCOSClient(bucketURL, secretID, secretKey string) (*cos.Client, error) {
	u, err := url.Parse(bucketURL)
	if err != nil {
		return nil, fmt.Errorf("parse COS_BUCKET_URL: %w", err)
	}
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Timeout: 60 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  secretID,
			SecretKey: secretKey,
		},
	})
	return client, nil
}

func listPrefixes(ctx context.Context, client *cos.Client, prefix string, maxKeys int) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	opt := &cos.BucketGetOptions{
		Prefix:    prefix,
		Delimiter: "/",
		MaxKeys:   maxKeys,
	}

	out := make([]string, 0)
	isTruncated := true
	marker := ""
	for isTruncated {
		opt.Marker = marker
		v, _, err := client.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}
		for _, p := range v.CommonPrefixes {
			base := path.Base(strings.TrimSuffix(p, "/")) + "/"
			out = append(out, base)
		}
		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}
	return out, nil
}

func listObjects(ctx context.Context, client *cos.Client, prefix string, maxKeys int) ([]string, error) {
	opt := &cos.BucketGetOptions{
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}

	out := make([]string, 0)
	isTruncated := true
	marker := ""
	for isTruncated {
		opt.Marker = marker
		v, _, err := client.Bucket.Get(ctx, opt)
		if err != nil {
			return nil, err
		}
		for _, c := range v.Contents {
			out = append(out, c.Key)
		}
		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}
	return out, nil
}

func printList(title string, items []string, max int) {
	sort.Strings(items)
	fmt.Printf("\n== %s (count=%d) ==\n", title, len(items))
	if len(items) == 0 {
		return
	}
	if max <= 0 {
		max = len(items)
	}
	if len(items) < max {
		max = len(items)
	}
	for i := 0; i < max; i++ {
		fmt.Printf("- %s\n", items[i])
	}
	if len(items) > max {
		fmt.Printf("... (%d more)\n", len(items)-max)
	}
}

func main() {
	var o options
	flag.StringVar(&o.rootDir, "ray-root-dir", "", "Ray root dir prefix in bucket (e.g. mce-proj-bk0lwhh1)")
	flag.StringVar(&o.clusterKey, "cluster", "", "cluster key in format <name>_<namespace> (e.g. rayjob-job-xxx_mce-proj-bk0lwhh1)")
	flag.StringVar(&o.sessionName, "session", "", "session name (e.g. session_2026-01-30_21-00-36_308550_1)")
	flag.IntVar(&o.maxKeys, "max-keys", 1000, "max keys per COS list request")
	flag.IntVar(&o.maxPrint, "max-print", 50, "max items to print per section")
	flag.BoolVar(&o.verbose, "v", false, "verbose logging")
	flag.Parse()

	_ = o.verbose

	bucketURL, err := getenvRequired("COS_BUCKET_URL")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	secretID, err := getenvRequired("COS_SECRET_ID")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	secretKey, err := getenvRequired("COS_SECRET_KEY")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if strings.TrimSpace(o.rootDir) == "" {
		fmt.Fprintln(os.Stderr, "missing --ray-root-dir")
		os.Exit(2)
	}

	client, err := newCOSClient(bucketURL, secretID, secretKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx := context.Background()

	metadirPrefix := path.Join(o.rootDir, "metadir") + "/"
	clusters, err := listPrefixes(ctx, client, metadirPrefix, o.maxKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list metadir failed: %v\n", err)
		os.Exit(1)
	}
	printList("metadir clusters under "+metadirPrefix, clusters, o.maxPrint)

	if strings.TrimSpace(o.clusterKey) == "" {
		fmt.Println("\nDone.")
		return
	}

	sessionsPrefix := path.Join(o.rootDir, "metadir", o.clusterKey) + "/"
	sessions, err := listPrefixes(ctx, client, sessionsPrefix, o.maxKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list sessions failed: %v\n", err)
		os.Exit(1)
	}
	printList("sessions under "+sessionsPrefix, sessions, o.maxPrint)

	if strings.TrimSpace(o.sessionName) == "" {
		fmt.Println("\nDone.")
		return
	}

	clusterPhysicalPrefix := path.Join(o.rootDir, o.clusterKey, o.sessionName)
	jobEventsPrefix := path.Join(clusterPhysicalPrefix, "job_events") + "/"
	nodeEventsPrefix := path.Join(clusterPhysicalPrefix, "node_events") + "/"
	logsPrefix := path.Join(clusterPhysicalPrefix, "logs") + "/"

	jobDirs, _ := listPrefixes(ctx, client, jobEventsPrefix, o.maxKeys)
	nodeObjs, _ := listObjects(ctx, client, nodeEventsPrefix, o.maxKeys)
	logNodeDirs, _ := listPrefixes(ctx, client, logsPrefix, o.maxKeys)

	printList("job_events job dirs under "+jobEventsPrefix, jobDirs, o.maxPrint)
	printList("node_events objects under "+nodeEventsPrefix, nodeObjs, o.maxPrint)
	printList("logs node dirs under "+logsPrefix, logNodeDirs, o.maxPrint)

	if len(logNodeDirs) > 0 {
		nodeName := strings.TrimSuffix(logNodeDirs[0], "/")
		p := path.Join(clusterPhysicalPrefix, "logs", nodeName, "events", "event_JOBS.log")
		objs, _ := listObjects(ctx, client, p, o.maxKeys)
		printList("event_JOBS.log at "+p, objs, o.maxPrint)
	}

	fmt.Println("\nDone.")
}
