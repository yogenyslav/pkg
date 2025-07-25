package mongo

import (
	"fmt"
	"net"
)

// Config is the configuration for the MongoDB client.
type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	DB           string `yaml:"db"`
	AuthType     string `yaml:"auth_type"`
	Port         string `yaml:"port"`
	RetryTimeout int    `yaml:"retry_timeout"`
}

// URL returns the connection URL.
func (c Config) URL() string {
	url := fmt.Sprintf("mongodb://%s/%s", net.JoinHostPort(c.Host, c.Port), c.DB)
	if c.AuthType != "" && c.AuthType != "no" {
		url = fmt.Sprintf("mongodb://%s:%s@%s/%s", c.User, c.Password, net.JoinHostPort(c.Host, c.Port), c.DB)
	}
	return url
}
