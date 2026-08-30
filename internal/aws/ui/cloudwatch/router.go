package cloudwatchui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the CloudWatch UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Metrics
	r.Get("/metrics", h.ListMetrics)
	r.Get("/metrics/statistics", h.GetMetricStatistics)

	// Alarms
	r.Get("/alarms", h.ListAlarms)
	r.Post("/alarms", h.PutAlarm)
	r.Delete("/alarms", h.DeleteAlarm)
	r.Post("/alarms/state", h.SetAlarmState)
	r.Post("/alarms/enable", h.EnableAlarmActions)
	r.Post("/alarms/disable", h.DisableAlarmActions)

	// Dashboards
	r.Get("/dashboards", h.ListDashboards)
	r.Get("/dashboards/detail", h.GetDashboard)
	r.Put("/dashboards", h.PutDashboard)
	r.Delete("/dashboards", h.DeleteDashboard)

	return r
}
