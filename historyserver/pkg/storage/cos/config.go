package cos

import (
	"fmt"
	"os"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

// readCredentialFromFileOrEnv 读取凭证，优先从文件读取，回退到环境变量
// 支持：COS_SECRET_ID_FILE -> COS_SECRET_ID
func readCredentialFromFileOrEnv(envVarName string) (string, error) {
	// 1. 优先从 *_FILE 环境变量读取文件路径
	filePath := os.Getenv(envVarName + "_FILE")
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("failed to read %s from %s: %v", envVarName, filePath, err)
		}
		value := strings.TrimSpace(string(data))
		if value != "" {
			return value, nil
		}
	}
	
	// 2. 回退到直接读取环境变量（向后兼容）
	return os.Getenv(envVarName), nil
}

type config struct {
	BucketURL    string
	SecretID     string
	SecretKey    string
	SessionToken string  // 支持临时密钥
	types.RayCollectorConfig
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]interface{}) {
	c.RayCollectorConfig = *rcc
	// 支持从文件或环境变量读取凭证
	c.SecretID, _ = readCredentialFromFileOrEnv("COS_SECRET_ID")
	c.SecretKey, _ = readCredentialFromFileOrEnv("COS_SECRET_KEY")
	c.SessionToken, _ = readCredentialFromFileOrEnv("COS_SESSION_TOKEN")
	
	if len(jd) == 0 {
		c.BucketURL = os.Getenv("COS_BUCKET_URL")
	} else {
		if bucketURL, ok := jd["cosBucketURL"]; ok {
			c.BucketURL = bucketURL.(string)
		}
	}
}

func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]interface{}) {
	c.RayCollectorConfig = types.RayCollectorConfig{
		RootDir: rcc.RootDir,
	}
	// 支持从文件或环境变量读取凭证
	c.SecretID, _ = readCredentialFromFileOrEnv("COS_SECRET_ID")
	c.SecretKey, _ = readCredentialFromFileOrEnv("COS_SECRET_KEY")
	c.SessionToken, _ = readCredentialFromFileOrEnv("COS_SESSION_TOKEN")
	
	if len(jd) == 0 {
		c.BucketURL = os.Getenv("COS_BUCKET_URL")
	} else {
		if bucketURL, ok := jd["cosBucketURL"]; ok {
			c.BucketURL = bucketURL.(string)
		}
	}
}
