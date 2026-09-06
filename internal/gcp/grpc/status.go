package grpc

import (
	"errors"

	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCStatus converts a provider/service error into a gRPC status error.
// Provider errors are resolved in precedence order: google.rpc Status name,
// then the provider Code alias, then the HTTP status; anything else is
// INTERNAL. This is the shared error-mapping used by every GCP gRPC service
// (Firestore, Pub/Sub, Secret Manager, KMS).
func GRPCStatus(err error) error {
	if err == nil {
		return nil
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return status.Error(codes.Internal, err.Error())
	}
	if perr.Status != "" {
		if c, ok := statusToCode(perr.Status); ok {
			return status.Error(c, perr.Message)
		}
	}
	if c, ok := codeAlias(perr.Code); ok {
		return status.Error(c, perr.Message)
	}
	if c, ok := httpToCode(perr.HTTPStatus); ok {
		return status.Error(c, perr.Message)
	}
	return status.Error(codes.Internal, perr.Message)
}

// statusToCode maps a google.rpc status name to its gRPC code. The bool is
// false for unrecognized names so the caller can fall through to the HTTP/Code
// fallbacks instead of silently returning UNKNOWN.
func statusToCode(s string) (codes.Code, bool) {
	switch s {
	case "FAILED_PRECONDITION":
		return codes.FailedPrecondition, true
	case "ABORTED":
		return codes.Aborted, true
	case "NOT_FOUND":
		return codes.NotFound, true
	case "INVALID_ARGUMENT":
		return codes.InvalidArgument, true
	case "ALREADY_EXISTS":
		return codes.AlreadyExists, true
	case "PERMISSION_DENIED":
		return codes.PermissionDenied, true
	case "UNAUTHENTICATED":
		return codes.Unauthenticated, true
	case "RESOURCE_EXHAUSTED":
		return codes.ResourceExhausted, true
	case "UNIMPLEMENTED":
		return codes.Unimplemented, true
	case "INTERNAL":
		return codes.Internal, true
	case "UNKNOWN":
		return codes.Unknown, true
	}
	return codes.Unknown, false
}

// httpToCode maps an HTTP status to its conventional gRPC code.
func httpToCode(h int) (codes.Code, bool) {
	switch h {
	case 400:
		return codes.InvalidArgument, true
	case 401:
		return codes.Unauthenticated, true
	case 403:
		return codes.PermissionDenied, true
	case 404:
		return codes.NotFound, true
	case 409:
		return codes.Aborted, true
	case 429:
		return codes.ResourceExhausted, true
	case 500:
		return codes.Internal, true
	case 501:
		return codes.Unimplemented, true
	}
	return codes.Unknown, false
}

// codeAlias maps a provider Code string to a gRPC code, covering the canonical
// names plus the non-canonical aliases used across providers (InvalidRequest →
// InvalidArgument, UnsupportedOperation → Unimplemented).
func codeAlias(c string) (codes.Code, bool) {
	switch c {
	case "InvalidArgument", "InvalidRequest", "InvalidParameter":
		return codes.InvalidArgument, true
	case "NotFound":
		return codes.NotFound, true
	case "AlreadyExists":
		return codes.AlreadyExists, true
	case "FailedPrecondition":
		return codes.FailedPrecondition, true
	case "Aborted":
		return codes.Aborted, true
	case "PermissionDenied":
		return codes.PermissionDenied, true
	case "Unauthenticated":
		return codes.Unauthenticated, true
	case "ResourceExhausted":
		return codes.ResourceExhausted, true
	case "UnsupportedOperation", "UnknownService":
		return codes.Unimplemented, true
	case "Internal", "InternalError":
		return codes.Internal, true
	}
	return codes.Unknown, false
}
