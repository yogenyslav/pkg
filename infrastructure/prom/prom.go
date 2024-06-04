// Package prom provides a configurable Prometheus server.
package prom

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

// HandlePrometheus starts a Prometheus server with the given configuration.
func HandlePrometheus(cfg *Config) {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), nil); err != nil { //nolint:gosec // no security issue here
		log.Error().Err(err).Msg("listening prometheus failed")
	}
}
