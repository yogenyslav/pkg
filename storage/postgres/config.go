package postgres

import (
	"fmt"
)

// Config is the configuration for the PostgreSQL client.
type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	DB           string `yaml:"db"`
	Ssl          bool   `yaml:"ssl"`
	Port         int    `yaml:"port"`
	RetryTimeout int    `yaml:"retry_timeout"`
}

// URL returns the connection URL.
func (c *Config) URL() string {
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=", c.User, c.Password, c.Host, c.Port, c.DB)
	if c.Ssl {
		url += "enable"
	} else {
		url += "disable"
	}
	return url
}
