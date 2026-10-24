package monitoringui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Monitoring UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Metrics explorer.
	r.Get("/metricDescriptors", h.ListMetricDescriptors)
	r.Get("/timeSeries", h.ListTimeSeries)

	// Alerting.
	r.Get("/alertPolicies", h.ListAlertPolicies)
	r.Post("/alertPolicies", h.CreateAlertPolicy)
	r.Get("/alertPolicies/{id}", h.GetAlertPolicy)
	r.Patch("/alertPolicies/{id}", h.UpdateAlertPolicy)
	r.Delete("/alertPolicies/{id}", h.DeleteAlertPolicy)

	// Notification channels.
	r.Get("/notificationChannels", h.ListNotificationChannels)
	r.Post("/notificationChannels", h.CreateNotificationChannel)
	r.Get("/notificationChannels/{id}", h.GetNotificationChannel)
	r.Patch("/notificationChannels/{id}", h.UpdateNotificationChannel)
	r.Delete("/notificationChannels/{id}", h.DeleteNotificationChannel)
	r.Get("/notificationChannelDescriptors", h.ListNotificationChannelDescriptors)

	return r
}
