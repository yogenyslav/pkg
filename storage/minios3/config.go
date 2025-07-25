package minios3

// Config is the configuration for the Minio S3 storage.
type Config struct {
	Host      string   `yaml:"host"`
	AccessKey string   `yaml:"access_key"`
	SecretKey string   `yaml:"secret_key"`
	Buckets   []Bucket `yaml:"buckets"`
	Ssl       bool     `yaml:"ssl"`
	Port      string   `yaml:"port"`
}

// Bucket is the configuration for a bucket.
type Bucket struct {
	Name   string `yaml:"name"`
	Region string `yaml:"region"`
	Lock   bool   `yaml:"lock"`
}
