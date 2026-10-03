package grpc

import (
	"errors"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCStatus converts a provider/service error into a gRPC status error.
// Provider errors are resolved through gcperr.Resolve, so the gRPC mapping uses
// the same precedence as the REST codecs: google.rpc Status name, then the
// provider Code alias, then the HTTP status; anything else is INTERNAL. This is
// the shared error-mapping used by every GCP gRPC service (Firestore, Pub/Sub,
// Secret Manager, KMS).
func GRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return status.Error(codes.Internal, err.Error())
	}
	name, _ := gcperr.Resolve(perr)
	if c, ok := gcperr.GRPCCodeForStatus(name); ok {
		return status.Error(c, perr.Message)
	}
	return status.Error(codes.Internal, perr.Message)
}
