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

// invalidMaskPath reports an update_mask path that names no field of the
// resource. Real GCP validates the mask against the resource's field set and
// answers INVALID_ARGUMENT (AIP-134 / AIP-161), not an unimplemented operation,
// so an unmappable path is a client error rather than a 501.
func invalidMaskPath(path string) error {
	return invalidArgument("unsupported update_mask path: " + path)
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
