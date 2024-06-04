package tracing

import (
	"fmt"
)

// Config is the struct for tracing configuration.
type Config struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// URL returns the URL for the tracing server.
func (c *Config) URL() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
