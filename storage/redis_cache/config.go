package rediscache

// Config is the configuration for the Redis cache.
type Config struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	DB       int    `yaml:"db"`
}
