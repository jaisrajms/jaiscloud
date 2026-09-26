// Package gcperr is the single source of truth for the mapping between a GCP
// provider error and the google.rpc status name plus HTTP status that the REST
// codecs and the gRPC server must agree on.
//
// Before this package the REST path (adapter/gcs.go gcpStatusString) and the
// gRPC path (grpc/status.go httpToCode/statusToCode) carried independent
// tables that drifted: HTTP 409 resolved to ALREADY_EXISTS on REST but ABORTED
// on gRPC, and HTTP 503 resolved to UNAVAILABLE on REST but fell through to
// INTERNAL on gRPC. Both transports now resolve through Resolve.
package gcperr

import (
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
)

// Canonical google.rpc status names.
const (
	OK                 = "OK"
	Cancelled          = "CANCELLED"
	Unknown            = "UNKNOWN"
	InvalidArgument    = "INVALID_ARGUMENT"
	DeadlineExceeded   = "DEADLINE_EXCEEDED"
	NotFound           = "NOT_FOUND"
	AlreadyExists      = "ALREADY_EXISTS"
	PermissionDenied   = "PERMISSION_DENIED"
	ResourceExhausted  = "RESOURCE_EXHAUSTED"
	FailedPrecondition = "FAILED_PRECONDITION"
	Aborted            = "ABORTED"
	OutOfRange         = "OUT_OF_RANGE"
	Unimplemented      = "UNIMPLEMENTED"
	Internal           = "INTERNAL"
	Unavailable        = "UNAVAILABLE"
	DataLoss           = "DATA_LOSS"
	Unauthenticated    = "UNAUTHENTICATED"
)

// StatusForHTTP maps an HTTP status to the canonical google.rpc status name
// used as the fallback when a ProviderError carries neither an explicit Status
// nor a recognised Code alias.
//
// FAILED_PRECONDITION and OUT_OF_RANGE both map to HTTP 400 in the
// google.rpc.Code convention; UNAVAILABLE maps to 503; ABORTED and
// ALREADY_EXISTS share HTTP 409, so 409 falls back to ALREADY_EXISTS (the more
// common case) and ABORTED must be selected explicitly via Status or Code.
func StatusForHTTP(httpStatus int) string {
	switch httpStatus {
	case 400:
		return InvalidArgument
	case 401:
		return Unauthenticated
	case 403:
		return PermissionDenied
	case 404:
		return NotFound
	case 409:
		return AlreadyExists
	case 412:
		return FailedPrecondition
	case 429:
		return ResourceExhausted
	case 499:
		return Cancelled
	case 500:
		return Internal
	case 501:
		return Unimplemented
	case 503:
		return Unavailable
	case 504:
		return DeadlineExceeded
	default:
		return Unknown
	}
}

// HTTPForStatus is the inverse of StatusForHTTP for the statuses whose HTTP
// mapping is unambiguous. Unknown names return 0.
func HTTPForStatus(status string) int {
	switch status {
	case InvalidArgument, FailedPrecondition, OutOfRange:
		return 400
	case Unauthenticated:
		return 401
	case PermissionDenied:
		return 403
	case NotFound:
		return 404
	case AlreadyExists, Aborted:
		return 409
	case ResourceExhausted:
		return 429
	case Cancelled:
		return 499
	case Internal, Unknown, DataLoss:
		return 500
	case Unimplemented:
		return 501
	case Unavailable:
		return 503
	case DeadlineExceeded:
		return 504
	default:
		return 0
	}
}

// AliasForCode maps a provider Code (the canonical names plus the historical
// aliases used across providers) to a canonical google.rpc status name.
func AliasForCode(code string) (string, bool) {
	switch code {
	case "InvalidArgument", "InvalidRequest", "InvalidParameter":
		return InvalidArgument, true
	case "NotFound":
		return NotFound, true
	case "AlreadyExists", "Conflict":
		return AlreadyExists, true
	case "FailedPrecondition", "PreconditionFailed", "bucketNotEmpty":
		return FailedPrecondition, true
	case "OutOfRange":
		return OutOfRange, true
	case "Aborted":
		return Aborted, true
	case "PermissionDenied":
		return PermissionDenied, true
	case "Unauthenticated":
		return Unauthenticated, true
	case "ResourceExhausted":
		return ResourceExhausted, true
	case "Unavailable", "ServiceUnavailable":
		return Unavailable, true
	case "UnsupportedOperation", "UnknownService":
		return Unimplemented, true
	case "Internal", "InternalError":
		return Internal, true
	case "DeadlineExceeded":
		return DeadlineExceeded, true
	case "Cancelled", "Canceled":
		return Cancelled, true
	case "DataLoss":
		return DataLoss, true
	default:
		return "", false
	}
}

// Resolve returns the canonical google.rpc status name and HTTP status for a
// provider error, using one precedence for both transports: an explicit
// Status, then a Code alias, then the HTTP status. A missing HTTP status is
// derived from the resolved name (default 500).
func Resolve(perr *model.ProviderError) (status string, httpStatus int) {
	if perr == nil {
		return Unknown, 500
	}
	httpStatus = perr.HTTPStatus
	if httpStatus == 0 {
		httpStatus = 500
	}
	status = perr.Status
	if status == "" {
		if s, ok := AliasForCode(perr.Code); ok {
			status = s
		} else {
			status = StatusForHTTP(httpStatus)
		}
	}
	if perr.HTTPStatus == 0 {
		if h := HTTPForStatus(status); h != 0 {
			httpStatus = h
		}
	}
	return status, httpStatus
}

// GRPCCodeForStatus maps a canonical google.rpc status name to its gRPC code.
// The bool is false for unrecognized names so the caller can fall back (both
// the gRPC transport and the REST protobuf encoder use INTERNAL). It is the
// single source of the name→code table shared by both transports.
func GRPCCodeForStatus(status string) (codes.Code, bool) {
	switch status {
	case FailedPrecondition:
		return codes.FailedPrecondition, true
	case OutOfRange:
		return codes.OutOfRange, true
	case Aborted:
		return codes.Aborted, true
	case NotFound:
		return codes.NotFound, true
	case InvalidArgument:
		return codes.InvalidArgument, true
	case AlreadyExists:
		return codes.AlreadyExists, true
	case PermissionDenied:
		return codes.PermissionDenied, true
	case Unauthenticated:
		return codes.Unauthenticated, true
	case ResourceExhausted:
		return codes.ResourceExhausted, true
	case Unimplemented:
		return codes.Unimplemented, true
	case Internal:
		return codes.Internal, true
	case Unknown:
		return codes.Unknown, true
	case Unavailable:
		return codes.Unavailable, true
	case Cancelled:
		return codes.Canceled, true
	case DataLoss:
		return codes.DataLoss, true
	case DeadlineExceeded:
		return codes.DeadlineExceeded, true
	default:
		return codes.Unknown, false
	}
}
