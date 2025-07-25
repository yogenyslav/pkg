package postgres

import (
	"fmt"
	"net"
)

// Config is the configuration for the PostgreSQL client.
type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	DB           string `yaml:"db"`
	Ssl          bool   `yaml:"ssl"`
	Port         string `yaml:"port"`
	RetryTimeout int    `yaml:"retry_timeout"`
}

// URL assembles config values into a conn string.
func (cfg *Config) URL() string {
	sslMode := "disable"
	if cfg.Ssl {
		sslMode = "enable"
	}
	return fmt.Sprintf(
		"postgresql://%s:%s@%s/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		net.JoinHostPort(cfg.Host, cfg.Port),
		cfg.DB,
		sslMode,
	)
}
