package monitoring

import (
	"errors"

	"jaiscloud/internal/model"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

// mapStoreError translates the store's sentinel errors into transport-neutral
// ProviderErrors. Both transports get identical errors, so the gRPC status and
// the REST envelope cannot drift.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, monitoringstore.ErrMetricDescriptorNotFound),
		errors.Is(err, monitoringstore.ErrAlertPolicyNotFound),
		errors.Is(err, monitoringstore.ErrNotificationChannelNotFound),
		errors.Is(err, monitoringstore.ErrServiceNotFound),
		errors.Is(err, monitoringstore.ErrServiceLevelObjectiveNotFound):
		return notFound("resource not found")
	case errors.Is(err, monitoringstore.ErrAlertPolicyExists):
		return model.NewProviderError("AlreadyExists", "alert policy already exists", 409)
	case errors.Is(err, monitoringstore.ErrNotificationChannelExists):
		return model.NewProviderError("AlreadyExists", "notification channel already exists", 409)
	case errors.Is(err, monitoringstore.ErrServiceExists):
		return model.NewProviderError("AlreadyExists", "service already exists", 409)
	case errors.Is(err, monitoringstore.ErrServiceLevelObjectiveExists):
		return model.NewProviderError("AlreadyExists", "service level objective already exists", 409)
	}
	return err
}
