package cos

import (
	"os"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

type config struct {
	BucketURL string
	SecretID  string
	SecretKey string
	types.RayCollectorConfig
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]interface{}) {
	c.RayCollectorConfig = *rcc
	c.SecretID = os.Getenv("COS_SECRET_ID")
	c.SecretKey = os.Getenv("COS_SECRET_KEY")
	
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
	c.SecretID = os.Getenv("COS_SECRET_ID")
	c.SecretKey = os.Getenv("COS_SECRET_KEY")
	
	if len(jd) == 0 {
		c.BucketURL = os.Getenv("COS_BUCKET_URL")
	} else {
		if bucketURL, ok := jd["cosBucketURL"]; ok {
			c.BucketURL = bucketURL.(string)
		}
	}
}
