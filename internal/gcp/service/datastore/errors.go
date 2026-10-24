package datastore

import (
	"errors"

	"jaiscloud/internal/model"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// ProviderError constructors for the statuses the Datastore core emits. The
// transports map these to their wire encodings (gRPC status / REST envelope).

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func failedPrecondition(msg string, httpStatus int) error {
	return model.NewProviderError("FailedPrecondition", msg, httpStatus)
}

func alreadyExists(msg string) error {
	return model.NewProviderError("AlreadyExists", msg, 409)
}

// notFound reports a write that addressed an absent entity. Real Datastore
// documents NOT_FOUND for exactly one case: "the request attempted to update an
// entity that does not exist" (Firestore in Datastore mode, "Errors and Error
// Handling"). Reads report a miss as an empty Lookup result, and a delete of an
// absent entity is idempotent, so this status only comes from an update.
func notFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

func aborted(msg string) error {
	return model.NewProviderError("Aborted", msg, 409)
}

// mapStoreError translates the store's sentinel errors into transport-neutral
// ProviderErrors. Both transports get identical errors, so the gRPC status and
// the REST envelope cannot drift.
func mapStoreError(err error) error {
	switch {
	case errors.Is(err, dsstore.ErrAborted):
		return aborted("transaction was aborted due to concurrent modification")
	case errors.Is(err, dsstore.ErrConflict):
		return failedPrecondition("precondition failed", 400)
	case errors.Is(err, dsstore.ErrEntityExists):
		return alreadyExists("entity already exists")
	case errors.Is(err, dsstore.ErrEntityNotFound):
		return notFound("entity not found")
	case errors.Is(err, dsstore.ErrInvalidKey):
		return invalidArgument("invalid key")
	default:
		return err
	}
}
