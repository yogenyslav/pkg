package prom

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

func HandlePrometheus(host string, port int) {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(fmt.Sprintf("%s:%d", host, port), nil); err != nil {
		log.Fatal().Err(err).Msg("listening prometheus failed")
	}
}
