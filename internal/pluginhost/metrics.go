package pluginhost

import (
	"errors"
	"sync"
	"time"

	"jaiscloud/internal/model"

	"github.com/jaisrajms/jaiscloud/sdk"
	"github.com/prometheus/client_golang/prometheus"
)

// Plugin metrics are registered only when a plugin is loaded, so a process
// without plugins exports exactly the series it did before.
var (
	metricsOnce sync.Once
	callsTotal  *prometheus.CounterVec
	callSeconds *prometheus.HistogramVec
)

func registerMetrics() {
	metricsOnce.Do(func() {
		callsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jaiscloud_plugin_calls_total",
			Help: "Calls from the emulator to plugins, by phase and outcome.",
		}, []string{"plugin", "phase", "outcome"})
		callSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "jaiscloud_plugin_call_seconds",
			Help:    "Duration of calls from the emulator to plugins.",
			Buckets: prometheus.DefBuckets,
		}, []string{"plugin", "phase"})
		for _, c := range []prometheus.Collector{callsTotal, callSeconds} {
			if err := prometheus.Register(c); err != nil {
				var are prometheus.AlreadyRegisteredError
				if !errors.As(err, &are) {
					panic(err)
				}
			}
		}
	})
}

func observe(plugin, phase, outcome string, d time.Duration) {
	if callsTotal == nil {
		return
	}
	callsTotal.WithLabelValues(plugin, phase, outcome).Inc()
	callSeconds.WithLabelValues(plugin, phase).Observe(d.Seconds())
}

// outcome classifies a finished call for the metrics label.
func outcome(err error, res sdk.Result) string {
	var pe *model.ProviderError
	switch {
	case err == nil && res.Err != nil:
		return "error"
	case err == nil:
		return "ok"
	case errors.As(err, &pe):
		switch pe.Code {
		case "PluginTimeout":
			return "timeout"
		case "PluginProtocolError":
			return "protocol"
		case "PluginUnsupportedPayload":
			return "unsupported"
		}
		return "unavailable"
	}
	return "unavailable"
}
