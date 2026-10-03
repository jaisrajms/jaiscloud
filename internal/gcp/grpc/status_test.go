package grpc

import (
	"testing"

	"jaiscloud/internal/model"

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
