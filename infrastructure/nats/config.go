package nats

import (
	"fmt"
	"net"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// Config is a configuration for nats broker/cluster.
type Config struct {
	Nodes     []NodeConfig     `yaml:"nodes"`
	Stream    StreamConfig     `yaml:"stream"`
	Consumers []ConsumerConfig `yaml:"consumers"`
}

// NodeConfig is a configuration for a single nats node.
type NodeConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// // URL returns a connection url for nats node.
func (n NodeConfig) URL() string {
	return fmt.Sprintf("nats://%s:%s@%s", n.User, n.Password, net.JoinHostPort(n.Host, n.Port))
}

// URL returns a single conn string for nats broker or a multiple hosts splitted by ', ' for cluster.
func (cfg Config) URL() string {
	switch len(cfg.Nodes) {
	case 0:
		panic("no nats broker specified")
	case 1:
		return cfg.Nodes[0].URL()
	default:
		hosts := make([]string, 0, len(cfg.Nodes))
		for _, node := range cfg.Nodes {
			hosts = append(hosts, node.URL())
		}
		return strings.Join(hosts, ", ")
	}
}

// StreamConfig is a configuration for jetstream stream.
type StreamConfig struct {
	Name            string                     `yaml:"name"`
	Subjects        []string                   `yaml:"subjects"`
	RetentionPolicy jetstream.RetentionPolicy  `yaml:"retention_policy"`
	MaxAgeSec       int64                      `yaml:"max_age_sec"`
	Replicas        int                        `yaml:"replicas"`
	Compression     jetstream.StoreCompression `yaml:"compressions"`
}

// ConsumerConfig is a configuration for consumer of jetstream stream.
type ConsumerConfig struct {
	ConsumerName string              `yaml:"consumer_name"`
	Stream       string              `yaml:"stream"`
	AckPolicy    jetstream.AckPolicy `yaml:"ack_policy"`
	Filters      []string            `yaml:"filters"`
}
