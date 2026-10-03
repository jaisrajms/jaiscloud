package gcp

import (
	"encoding/json"
	"testing"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rpcName maps a gRPC code to its canonical google.rpc name. It is written
// independently of the production statusToCode mapping so the REST assertion
// and the gRPC assertion below can be compared against the same expected
// canonical name without being tautological.
func rpcName(c codes.Code) string {
	switch c {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	case codes.Unknown:
		return "UNKNOWN"
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.OutOfRange:
		return "OUT_OF_RANGE"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	case codes.Internal:
		return "INTERNAL"
	case codes.Unavailable:
		return "UNAVAILABLE"
	case codes.DataLoss:
		return "DATA_LOSS"
	case codes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return c.String()
	}
}

func TestRESTGRPCErrorStatusParity(t *testing.T) {
	cases := []struct {
		name       string
		perr       *model.ProviderError
		wantStatus string
		wantCode   codes.Code
		wantHTTP   int
	}{
		{
			name:       "NotFound",
			perr:       &model.ProviderError{Code: "NotFound", HTTPStatus: 404},
			wantStatus: "NOT_FOUND",
			wantCode:   codes.NotFound,
			wantHTTP:   404,
		},
		{
			name:       "AbortedCode",
			perr:       &model.ProviderError{Code: "Aborted", HTTPStatus: 409},
			wantStatus: "ABORTED",
			wantCode:   codes.Aborted,
			wantHTTP:   409,
		},
		{
			name:       "ConflictIsAlreadyExists",
			perr:       &model.ProviderError{Code: "Conflict", HTTPStatus: 409},
			wantStatus: "ALREADY_EXISTS",
			wantCode:   codes.AlreadyExists,
			wantHTTP:   409,
		},
		{
			name:       "BucketNotEmptyIsFailedPrecondition",
			perr:       &model.ProviderError{Code: "bucketNotEmpty", HTTPStatus: 409},
			wantStatus: "FAILED_PRECONDITION",
			wantCode:   codes.FailedPrecondition,
			wantHTTP:   409,
		},
		{
			name:       "ExplicitStatusWins",
			perr:       &model.ProviderError{Code: "InvalidRequest", HTTPStatus: 503, Status: "UNAVAILABLE"},
			wantStatus: "UNAVAILABLE",
			wantCode:   codes.Unavailable,
			wantHTTP:   503,
		},
		{
			name:       "HTTPFallbackUnavailable",
			perr:       &model.ProviderError{Code: "Anything", HTTPStatus: 503},
			wantStatus: "UNAVAILABLE",
			wantCode:   codes.Unavailable,
			wantHTTP:   503,
		},
		{
			name:       "FailedPreconditionCodeOn400",
			perr:       &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 400},
			wantStatus: "FAILED_PRECONDITION",
			wantCode:   codes.FailedPrecondition,
			wantHTTP:   400,
		},
		{
			name:       "StatusOverridesCode",
			perr:       &model.ProviderError{Code: "NotFound", HTTPStatus: 404, Status: "ABORTED"},
			wantStatus: "ABORTED",
			wantCode:   codes.Aborted,
			wantHTTP:   404,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			httpStatus, _, body := (&JSONCodec{Service: "test"}).EncodeError(nil, tc.perr)

			var env struct {
				Error struct {
					Status string `json:"status"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &env); err != nil {
				t.Fatalf("unmarshal error envelope: %v (body=%s)", err, body)
			}
			if env.Error.Status != tc.wantStatus {
				t.Fatalf("REST error.status = %q, want %q", env.Error.Status, tc.wantStatus)
			}
			if httpStatus != tc.wantHTTP {
				t.Fatalf("EncodeError HTTP status = %d, want %d", httpStatus, tc.wantHTTP)
			}

			gerr := grpcutil.GRPCStatus(tc.perr)
			if gerr == nil {
				t.Fatal("GRPCStatus returned nil for non-nil error")
			}
			if got := rpcName(status.Code(gerr)); got != tc.wantStatus {
				t.Fatalf("gRPC status = %q, want %q", got, tc.wantStatus)
			}
			if got := status.Code(gerr); got != tc.wantCode {
				t.Fatalf("gRPC code = %v, want %v", got, tc.wantCode)
			}
		})
	}
}
