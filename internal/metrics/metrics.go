// Package metrics defines the Prometheus metrics the service exports.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics groups the service's collectors so tests can use a private registry.
type Metrics struct {
	Decisions     *prometheus.CounterVec
	Latency       *prometheus.HistogramVec
	BackendErrors *prometheus.CounterVec
}

// New creates and registers the collectors on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_decisions_total",
			Help: "Rate limit decisions by rule and result (allowed, denied, fail_open).",
		}, []string{"rule", "result"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ratelimit_check_duration_seconds",
			Help:    "Time spent deciding a Check, including the backend round-trip.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
		}, []string{"backend"}),
		BackendErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_backend_errors_total",
			Help: "Backend failures (e.g. Redis timeouts) by rule.",
		}, []string{"rule"}),
	}
	reg.MustRegister(m.Decisions, m.Latency, m.BackendErrors)
	return m
}
