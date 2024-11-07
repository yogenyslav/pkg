// Package prom provides a configurable Prometheus server.
package prom

import (
	"net"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// HandlePrometheus starts a Prometheus server with the given configuration.
func HandlePrometheus(cfg *Config, endpoint string, logger *zerolog.Logger) {
	http.Handle(endpoint, promhttp.Handler())
	if err := http.ListenAndServe(net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), nil); err != nil { //nolint:gosec // no security issue here
		logger.Err(err).Msg("listening prometheus failed")
	}
}
