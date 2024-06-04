package prom

// Config is the struct for Prometheus configuration.
type Config struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}
