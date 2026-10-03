package gcperr

import (
	"testing"

	"jaiscloud/internal/model"

	"google.golang.org/grpc/codes"
)

func TestStatusForHTTP(t *testing.T) {
	cases := map[int]string{
		400: InvalidArgument,
		401: Unauthenticated,
		403: PermissionDenied,
		404: NotFound,
		409: AlreadyExists,
		412: FailedPrecondition,
		429: ResourceExhausted,
		499: Cancelled,
		500: Internal,
		501: Unimplemented,
		503: Unavailable,
		504: DeadlineExceeded,
		418: Unknown,
	}
	for httpStatus, want := range cases {
		if got := StatusForHTTP(httpStatus); got != want {
			t.Errorf("StatusForHTTP(%d) = %q, want %q", httpStatus, got, want)
		}
	}
}

func TestHTTPForStatus(t *testing.T) {
	cases := map[string]int{
		InvalidArgument:    400,
		FailedPrecondition: 400,
		OutOfRange:         400,
		NotFound:           404,
		AlreadyExists:      409,
		Aborted:            409,
		ResourceExhausted:  429,
		Cancelled:          499,
		Internal:           500,
		Unimplemented:      501,
		Unavailable:        503,
		DeadlineExceeded:   504,
		"BOGUS":            0,
	}
	for status, want := range cases {
		if got := HTTPForStatus(status); got != want {
			t.Errorf("HTTPForStatus(%q) = %d, want %d", status, got, want)
		}
	}
}

func TestResolvePrecedence(t *testing.T) {
	cases := []struct {
		name       string
		perr       *model.ProviderError
		wantStatus string
		wantHTTP   int
	}{
		{"explicit status wins", &model.ProviderError{Code: "Conflict", HTTPStatus: 409, Status: Aborted}, Aborted, 409},
		{"code alias aborted", &model.ProviderError{Code: "Aborted", HTTPStatus: 409}, Aborted, 409},
		{"code alias conflict", &model.ProviderError{Code: "Conflict", HTTPStatus: 409}, AlreadyExists, 409},
		{"code alias bucketNotEmpty", &model.ProviderError{Code: "bucketNotEmpty", HTTPStatus: 409}, FailedPrecondition, 409},
		{"code alias preconditionFailed", &model.ProviderError{Code: "PreconditionFailed", HTTPStatus: 412}, FailedPrecondition, 412},
		{"failed precondition at 400", &model.ProviderError{Code: "FailedPrecondition", HTTPStatus: 400}, FailedPrecondition, 400},
		{"unavailable code alias", &model.ProviderError{Code: "ServiceUnavailable", HTTPStatus: 503}, Unavailable, 503},
		{"http fallback 503", &model.ProviderError{Code: "OddUnknownCode", HTTPStatus: 503}, Unavailable, 503},
		{"http fallback 404", &model.ProviderError{HTTPStatus: 404}, NotFound, 404},
		{"derive http from status", &model.ProviderError{Status: Unavailable}, Unavailable, 503},
		{"nil error", nil, Unknown, 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotStatus, gotHTTP := Resolve(tc.perr)
			if gotStatus != tc.wantStatus || gotHTTP != tc.wantHTTP {
				t.Fatalf("Resolve() = (%q, %d), want (%q, %d)", gotStatus, gotHTTP, tc.wantStatus, tc.wantHTTP)
			}
		})
	}
}

func TestGRPCCodeForStatus(t *testing.T) {
	cases := map[string]codes.Code{
		InvalidArgument:    codes.InvalidArgument,
		NotFound:           codes.NotFound,
		AlreadyExists:      codes.AlreadyExists,
		FailedPrecondition: codes.FailedPrecondition,
		Aborted:            codes.Aborted,
		OutOfRange:         codes.OutOfRange,
		PermissionDenied:   codes.PermissionDenied,
		Unauthenticated:    codes.Unauthenticated,
		ResourceExhausted:  codes.ResourceExhausted,
		Unimplemented:      codes.Unimplemented,
		Internal:           codes.Internal,
		Unknown:            codes.Unknown,
		Unavailable:        codes.Unavailable,
		Cancelled:          codes.Canceled,
		DataLoss:           codes.DataLoss,
		DeadlineExceeded:   codes.DeadlineExceeded,
	}
	for status, want := range cases {
		got, ok := GRPCCodeForStatus(status)
		if !ok || got != want {
			t.Errorf("GRPCCodeForStatus(%q) = (%v, %v), want (%v, true)", status, got, ok, want)
		}
	}
	if _, ok := GRPCCodeForStatus("BOGUS"); ok {
		t.Errorf("GRPCCodeForStatus(BOGUS) ok = true, want false")
	}
}
