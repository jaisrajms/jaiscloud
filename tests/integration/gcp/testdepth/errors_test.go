package testdepth

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// httpStatusError builds the REST error an official apiary client surfaces: an
// *googleapi.Error whose body is the documented GCP envelope.
func httpStatusError(code int, token, message string) *googleapi.Error {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": token},
	})
	return &googleapi.Error{Code: code, Message: message, Body: string(body)}
}

func TestStatusOf(t *testing.T) {
	obs, ok := StatusOf(httpStatusError(404, "NOT_FOUND", "no such thing"))
	if !ok || obs.Kind != KindHTTP || obs.HTTPStatus != 404 || obs.Status != "NOT_FOUND" || obs.Message != "no such thing" {
		t.Fatalf("StatusOf(*googleapi.Error) = %+v, %v", obs, ok)
	}

	obs, ok = StatusOf(status.Error(codes.AlreadyExists, "already exists"))
	if !ok || obs.Kind != KindGRPC || obs.Code != codes.AlreadyExists || obs.Status != "ALREADY_EXISTS" {
		t.Fatalf("StatusOf(grpc status) = %+v, %v", obs, ok)
	}

	obs, ok = StatusOf(errors.New("plain"))
	if !ok || obs.Kind != KindOther {
		t.Fatalf("StatusOf(plain error) = %+v, %v, want KindOther", obs, ok)
	}

	if _, ok := StatusOf(nil); ok {
		t.Fatal("StatusOf(nil) reported an error")
	}
}

func TestStatusOfReadsTheAPIErrorWrapper(t *testing.T) {
	// Newer cloud clients wrap the apiary error; errors.As must unwrap it.
	wrapped := fmt.Errorf("create: %w", httpStatusError(409, "ALREADY_EXISTS", "exists"))
	obs, _ := StatusOf(wrapped)
	if obs.Kind != KindHTTP || obs.HTTPStatus != 409 || obs.Status != "ALREADY_EXISTS" {
		t.Fatalf("StatusOf(wrapped) = %+v, want the unwrapped HTTP 409 envelope", obs)
	}
}

func TestStatusName(t *testing.T) {
	cases := map[codes.Code]string{
		codes.OK:                 "OK",
		codes.NotFound:           "NOT_FOUND",
		codes.AlreadyExists:      "ALREADY_EXISTS",
		codes.InvalidArgument:    "INVALID_ARGUMENT",
		codes.FailedPrecondition: "FAILED_PRECONDITION",
		codes.PermissionDenied:   "PERMISSION_DENIED",
		codes.Unimplemented:      "UNIMPLEMENTED",
		codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
		// The one doubled-letter google.rpc name (gcperr's Cancelled).
		codes.Canceled: "CANCELLED",
	}
	for code, want := range cases {
		if got := StatusName(code); got != want {
			t.Errorf("StatusName(%s) = %q, want %q", code, got, want)
		}
	}
}

func TestSpecCatalogue(t *testing.T) {
	specs := map[string]ErrorSpec{
		"not found":           SpecNotFound(),
		"already exists":      SpecAlreadyExists(),
		"invalid argument":    SpecInvalidArgument(),
		"permission denied":   SpecPermissionDenied(),
		"aborted":             SpecAborted(),
		"precondition failed": SpecPreconditionFailed(),
		"conditional request": SpecConditionalRequestFailed(),
		"unimplemented":       SpecUnimplemented(),
	}
	for name, spec := range specs {
		if spec.HTTPStatus == 0 || spec.Code == codes.OK || spec.Status == "" {
			t.Errorf("%s spec = %+v, want the HTTP status, the gRPC code and the google.rpc status name", name, spec)
		}
		if spec.Status != StatusName(spec.Code) {
			t.Errorf("%s spec status = %q, want %q", name, spec.Status, StatusName(spec.Code))
		}
		rendered := spec.String()
		if !strings.Contains(rendered, fmt.Sprintf("HTTP %d", spec.HTTPStatus)) ||
			!strings.Contains(rendered, fmt.Sprintf("gRPC %s", spec.Code)) ||
			!strings.Contains(rendered, spec.Status) {
			t.Errorf("%s spec renders as %q, want the HTTP status, code and status name", name, rendered)
		}
	}
}

// TestPreconditionFailedPairing pins the google.rpc.Code pairing: FAILED_PRECONDITION
// is HTTP 400 in the convention the service cores follow (gcperr, the conformance
// validator's 400 row); 412 is the conditional-request variant, which needs its own
// spec so a session cannot assert 412 against a REST client that returns 400.
func TestPreconditionFailedPairing(t *testing.T) {
	if got := SpecPreconditionFailed(); got.HTTPStatus != 400 || got.Code != codes.FailedPrecondition || got.Status != "FAILED_PRECONDITION" {
		t.Fatalf("SpecPreconditionFailed() = %+v, want the HTTP 400 pairing", got)
	}
	if got := SpecConditionalRequestFailed(); got.HTTPStatus != 412 || got.Code != codes.FailedPrecondition || got.Status != "FAILED_PRECONDITION" {
		t.Fatalf("SpecConditionalRequestFailed() = %+v, want the HTTP 412 variant", got)
	}
	if got := CheckError(httpStatusError(400, "FAILED_PRECONDITION", "not ready"), SpecPreconditionFailed()); len(got) != 0 {
		t.Fatalf("a REST 400 FAILED_PRECONDITION reported %v, want a match", got)
	}
	if got := strings.Join(CheckError(httpStatusError(400, "FAILED_PRECONDITION", "not ready"), SpecConditionalRequestFailed()), "\n"); !strings.Contains(got, "expected HTTP 412, the client returned HTTP 400") {
		t.Fatalf("the 400/412 pairing is not distinguished: %q", got)
	}
}

func TestCheckErrorMatchesBothTransports(t *testing.T) {
	// One catalogue spec asserts whichever transport the caller's client uses.
	if got := CheckError(httpStatusError(404, "NOT_FOUND", "gone"), SpecNotFound()); len(got) != 0 {
		t.Errorf("REST 404 vs SpecNotFound = %v, want a match", got)
	}
	if got := CheckError(status.Error(codes.NotFound, "gone"), SpecNotFound()); len(got) != 0 {
		t.Errorf("gRPC NotFound vs SpecNotFound = %v, want a match", got)
	}
	if got := CheckError(httpStatusError(409, "ALREADY_EXISTS", "exists"), SpecAlreadyExists()); len(got) != 0 {
		t.Errorf("REST 409 vs SpecAlreadyExists = %v, want a match", got)
	}
}

func TestCheckErrorSeededNegatives(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		want   ErrorSpec
		expect string
	}{
		{
			name:   "no error at all",
			err:    nil,
			want:   SpecNotFound(),
			expect: "expected an error matching HTTP 404 / gRPC NotFound / NOT_FOUND, the client returned none",
		},
		{
			name:   "wrong HTTP status",
			err:    httpStatusError(500, "INTERNAL", "boom"),
			want:   SpecNotFound(),
			expect: "expected HTTP 404, the client returned HTTP 500 (boom)",
		},
		{
			name:   "wrong gRPC code",
			err:    status.Error(codes.NotFound, "gone"),
			want:   SpecAlreadyExists(),
			expect: "expected gRPC AlreadyExists",
		},
		{
			name:   "wrong google.rpc status token",
			err:    httpStatusError(404, "INTERNAL", "gone"),
			want:   SpecNotFound(),
			expect: `expected status NOT_FOUND, the client reported "INTERNAL"`,
		},
		{
			name:   "unsupported spec",
			err:    status.Error(codes.NotFound, "gone"),
			want:   ErrorSpec{},
			expect: "ErrorSpec names neither HTTPStatus nor Code",
		},
		{
			name:   "gRPC-only spec against a REST client",
			err:    httpStatusError(404, "NOT_FOUND", "gone"),
			want:   ErrorSpec{Code: codes.NotFound},
			expect: "the client returned an HTTP error (HTTP 404 NOT_FOUND) but the spec only names the gRPC code NotFound",
		},
		{
			name:   "REST-only spec against a gRPC client",
			err:    status.Error(codes.NotFound, "gone"),
			want:   ErrorSpec{HTTPStatus: 404},
			expect: "the client returned a gRPC status (gRPC NotFound) but the spec only names HTTP 404",
		},
		{
			name:   "untyped error",
			err:    errors.New("connection refused"),
			want:   SpecNotFound(),
			expect: "the client returned a non-status error (*errors.errorString: connection refused)",
		},
		{
			name:   "message mismatch",
			err:    httpStatusError(404, "NOT_FOUND", "no such service account"),
			want:   ErrorSpec{HTTPStatus: 404, Code: codes.NotFound, Status: "NOT_FOUND", MessageContains: "bucket"},
			expect: `expected a message containing "bucket", the client reported "no such service account"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(CheckError(tc.err, tc.want), "\n")
			if !strings.Contains(got, tc.expect) {
				t.Fatalf("CheckError reported %q, want it to contain %q", got, tc.expect)
			}
		})
	}
}

func TestCheckErrorReason(t *testing.T) {
	withReason := httpStatusError(409, "ALREADY_EXISTS", "exists")
	withReason.Errors = []googleapi.ErrorItem{{Reason: "alreadyExists", Message: "exists"}}

	if got := CheckError(withReason, ErrorSpec{HTTPStatus: 409, Code: codes.AlreadyExists, Status: "ALREADY_EXISTS", Reason: "alreadyExists"}); len(got) != 0 {
		t.Fatalf("a matching reason reported %v", got)
	}
	got := strings.Join(CheckError(withReason, ErrorSpec{HTTPStatus: 409, Code: codes.AlreadyExists, Status: "ALREADY_EXISTS", Reason: "notFound"}), "\n")
	if !strings.Contains(got, `expected reason "notFound", the envelope carries "alreadyExists"`) {
		t.Fatalf("a mismatched reason reported %q", got)
	}
	// The emulator's envelope is details-based and carries no ErrorItems: a spec
	// that names a reason must not fail an envelope without one.
	if got := CheckError(httpStatusError(409, "ALREADY_EXISTS", "exists"), ErrorSpec{HTTPStatus: 409, Code: codes.AlreadyExists, Status: "ALREADY_EXISTS", Reason: "alreadyExists"}); len(got) != 0 {
		t.Fatalf("an envelope without ErrorItems reported %v, want the reason assertion skipped", got)
	}
}

func TestAssertError(t *testing.T) {
	if rec := wantReported(func(tb TB) { AssertError(tb, httpStatusError(404, "NOT_FOUND", "gone"), SpecNotFound()) }); rec.failed() {
		t.Fatalf("a matching envelope reported %q", rec.reported())
	}
	rec := wantReported(func(tb TB) { AssertError(tb, httpStatusError(409, "ALREADY_EXISTS", "exists"), SpecNotFound()) })
	if !strings.Contains(rec.reported(), "expected HTTP 404, the client returned HTTP 409") {
		t.Fatalf("a mismatched envelope reported %q", rec.reported())
	}
}

func TestRunNegatives(t *testing.T) {
	RunNegatives(t, []NegativeCase{
		{
			Name: "duplicate create is ALREADY_EXISTS",
			Run:  func() error { return httpStatusError(409, "ALREADY_EXISTS", "exists") },
			Want: SpecAlreadyExists(),
		},
		{
			Name: "gRPC negative",
			Run:  func() error { return status.Error(codes.InvalidArgument, "bad page size") },
			Want: SpecInvalidArgument(),
		},
	})
}
