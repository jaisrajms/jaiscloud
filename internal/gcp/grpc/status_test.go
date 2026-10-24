package grpc

import (
	"testing"

	"jaiscloud/internal/model"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCStatus(t *testing.T) {
	tests := []struct {
		name string
		perr *model.ProviderError
		want codes.Code
	}{
		{
			name: "NotFound",
			perr: &model.ProviderError{Code: "NotFound", HTTPStatus: 404},
			want: codes.NotFound,
		},
		{
			name: "AbortedCode",
			perr: &model.ProviderError{Code: "Aborted", HTTPStatus: 409},
			want: codes.Aborted,
		},
		{
			name: "ConflictIsAlreadyExists",
			perr: &model.ProviderError{Code: "Conflict", HTTPStatus: 409},
			want: codes.AlreadyExists,
		},
		{
			name: "BucketNotEmptyIsFailedPrecondition",
			perr: &model.ProviderError{Code: "bucketNotEmpty", HTTPStatus: 409},
			want: codes.FailedPrecondition,
		},
		{
			name: "ExplicitStatusWins",
			perr: &model.ProviderError{Code: "InvalidRequest", HTTPStatus: 503, Status: "UNAVAILABLE"},
			want: codes.Unavailable,
		},
		{
			name: "HTTPFallbackUnavailable",
			perr: &model.ProviderError{Code: "Anything", HTTPStatus: 503},
			want: codes.Unavailable,
		},
		{
			name: "FailedPreconditionCodeOn400",
			perr: &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 400},
			want: codes.FailedPrecondition,
		},
		{
			name: "StatusOverridesCode",
			perr: &model.ProviderError{Code: "NotFound", HTTPStatus: 404, Status: "ABORTED"},
			want: codes.Aborted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := status.Code(GRPCStatus(tt.perr))
			if got != tt.want {
				t.Fatalf("status.Code(GRPCStatus(%+v)) = %v, want %v", tt.perr, got, tt.want)
			}
		})
	}
}

// TestGRPCStatusErrorInfoDetail verifies a ProviderError.Details ErrorInfo is
// attached to the gRPC status (Pub/Sub exactly-once invalid-ack failures depend
// on it for the per-ack AcknowledgeStatus).
func TestGRPCStatusErrorInfoDetail(t *testing.T) {
	perr := &model.ProviderError{
		Code: "InvalidArgument", HTTPStatus: 400,
		Message: "bad ack ids",
		Details: []model.ErrorDetail{{
			Type:     "google.rpc.ErrorInfo",
			Reason:   "EXACTLY_ONCE_ACKID_FAILURE",
			Domain:   "pubsub.googleapis.com",
			Metadata: map[string]string{"ack-1": "PERMANENT_FAILURE_INVALID_ACK_ID"},
		}},
	}
	st, _ := status.FromError(GRPCStatus(perr))
	var got *errdetails.ErrorInfo
	for _, d := range st.Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			got = ei
		}
	}
	if got == nil {
		t.Fatalf("no ErrorInfo detail on %v", st)
	}
	if got.Reason != "EXACTLY_ONCE_ACKID_FAILURE" || got.Domain != "pubsub.googleapis.com" ||
		got.Metadata["ack-1"] != "PERMANENT_FAILURE_INVALID_ACK_ID" {
		t.Fatalf("ErrorInfo = %+v", got)
	}
}
