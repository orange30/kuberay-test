package cos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tencentyun/cos-go-sdk-v5"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type CosHandler struct {
	Client         *cos.Client
	RootDir        string
	RayClusterName string
	RayClusterID   string
	RayNodeName    string
}

func (h *CosHandler) CreateDirectory(d string) error {
	// COS handles directories implicitly, but mimicking S3 behavior of creating 0-byte object ending in /
	name := fmt.Sprintf("%s/", path.Clean(d))
	_, err := h.Client.Object.Put(context.Background(), name, strings.NewReader(""), nil)
	if err != nil {
		logrus.Errorf("Failed to create directory %s: %v", name, err)
		return err
	}
	return nil
}

func (h *CosHandler) WriteFile(file string, reader io.ReadSeeker) error {
	_, err := h.Client.Object.Put(context.Background(), file, reader, nil)
	return err
}

func (h *CosHandler) ListFiles(clusterId string, dir string) []string {
	prefix := path.Join(h.RootDir, clusterId, dir)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var files []string
	isTruncated := true
	marker := ""
	
	opt := &cos.BucketGetOptions{
		Prefix:    prefix,
		Delimiter: "/",
		MaxKeys:   1000,
	}

	for isTruncated {
		opt.Marker = marker
		v, _, err := h.Client.Bucket.Get(context.Background(), opt)
		if err != nil {
			logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
			return []string{}
		}

		for _, content := range v.Contents {
			// content.Key is the full path
			// we want relative path or base name based on how s3 implementation does it. 
			// checking s3 implementation: it returns base name or relative path depending on logic.
			// s3 _listFiles(..., true) returns only base name. 
			// ListFiles calls _listFiles(..., true).
			// So we return base name.
			if strings.HasSuffix(content.Key, "/") {
				continue
			}
			files = append(files, path.Base(content.Key))
		}
		
		for _, commonPrefix := range v.CommonPrefixes {
			files = append(files, path.Base(commonPrefix)+"/")
		}

		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}
	return files
}

func (h *CosHandler) List() []utils.ClusterInfo {
	prefix := path.Join(h.RootDir, "metadir")
	if !strings.HasSuffix(prefix, "/") {
        prefix += "/"
    }
	
	clusters := make(utils.ClusterInfoList, 0, 10)
	
	isTruncated := true
	marker := ""
	opt := &cos.BucketGetOptions{
		Prefix:  prefix,
		MaxKeys: 1000,
	}

	for isTruncated {
		opt.Marker = marker
		v, _, err := h.Client.Bucket.Get(context.Background(), opt)
		if err != nil {
			logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
			return clusters
		}

		for _, object := range v.Contents {
			c := &utils.ClusterInfo{}
			// Key example: root/metadir/cluster_namespace/session_date_time
			metaInfo := strings.Trim(strings.TrimPrefix(object.Key, prefix), "/")
			metas := strings.Split(metaInfo, "/")
			if len(metas) < 2 {
				continue
			}
			
			namespaceName := strings.Split(metas[0], "_")
			if len(namespaceName) < 2 {
				continue 
			}
			c.Name = namespaceName[0]
			c.Namespace = namespaceName[1]
			c.SessionName = metas[1]
			
			sessionInfo := strings.Split(metas[1], "_")
			// session_date_time => session, date, time
			if len(sessionInfo) < 3 {
				continue
			}
			date := sessionInfo[1]
			dataTime := sessionInfo[2]
			createTime, err := time.Parse("2006-01-02_15-04-05", date+"_"+dataTime)
			if err != nil {
				logrus.Errorf("Failed to parse time %s: %v", date+"_"+dataTime, err)
				continue
			}
			c.CreateTimeStamp = createTime.Unix()
			c.CreateTime = createTime.UTC().Format(("2006-01-02T15:04:05Z"))
			clusters = append(clusters, *c)
		}

		isTruncated = v.IsTruncated
		marker = v.NextMarker
	}
	
	sort.Sort(clusters)
	return clusters
}

func (h *CosHandler) GetContent(clusterId string, fileName string) io.Reader {
	fullPath := path.Join(h.RootDir, clusterId, fileName)
	resp, err := h.Client.Object.Get(context.Background(), fullPath, nil)
	if err != nil {
		logrus.Errorf("Failed to get object %s: %v", fullPath, err)
		return nil
	}
	
	// Read into memory to allow closing the response body
    // Using io.ReadAll is risky for large files but matches S3 implementation
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		logrus.Errorf("Failed to read all data from object %s: %v", fullPath, err)
		return nil
	}
	return strings.NewReader(string(data))
}

func New(c *config) (*CosHandler, error) {
	u, err := url.Parse(c.BucketURL)
	if err != nil {
		return nil, fmt.Errorf("Invalid COS Bucket URL: %v", err)
	}
	
	b := &cos.BaseURL{BucketURL: u}
	client := cos.NewClient(b, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  c.SecretID,
			SecretKey: c.SecretKey,
		},
	})

	return &CosHandler{
		Client:         client,
		RootDir:        c.RootDir,
		RayClusterName: c.RayClusterName,
		RayClusterID:   c.RayClusterID,
		RayNodeName:    c.RayNodeName,
	}, nil
}

func NewReader(c *types.RayHistoryServerConfig, jd map[string]interface{}) (storage.StorageReader, error) {
	config := &config{}
	config.completeHSConfig(c, jd)
	return New(config)
}

func NewWriter(c *types.RayCollectorConfig, jd map[string]interface{}) (storage.StorageWriter, error) {
	config := &config{}
	config.complete(c, jd)
	return New(config)
}
