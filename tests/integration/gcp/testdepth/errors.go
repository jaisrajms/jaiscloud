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

// Kind is the transport an error arrived on.
type Kind string

const (
	// KindHTTP is a REST error: the official apiary client's *googleapi.Error.
	KindHTTP Kind = "http"
	// KindGRPC is a gRPC error: an error carrying a *status.Status.
	KindGRPC Kind = "grpc"
	// KindOther is neither — an official-client wrapper the kit does not know,
	// or a plain error. StatusOf prints its type so a session can extend it.
	KindOther Kind = "other"
)

// Observed is what StatusOf extracted from a client error.
type Observed struct {
	Kind       Kind
	HTTPStatus int        // KindHTTP only
	Code       codes.Code // KindGRPC only
	Status     string     // the google.rpc status name ("NOT_FOUND", "ALREADY_EXISTS")
	Reason     string     // the apiary ErrorItem reason, when the envelope carries one
	Message    string
}

func (o Observed) String() string {
	switch o.Kind {
	case KindHTTP:
		return fmt.Sprintf("HTTP %d %s", o.HTTPStatus, o.Status)
	case KindGRPC:
		return fmt.Sprintf("gRPC %s", o.Code)
	default:
		return "non-status error"
	}
}

// ErrorSpec is a documented negative: the envelope a real client must be able to
// branch on. Set HTTPStatus for the REST half and Code for the gRPC half — the
// catalogue helpers set both, so one spec asserts whichever transport the caller's
// client actually uses. Status is the google.rpc status name, which real GCP
// emits both as the REST envelope's `error.status` and as the gRPC code name, so
// it is asserted on both transports.
type ErrorSpec struct {
	HTTPStatus int
	Code       codes.Code
	Status     string
	// Reason is the apiary `error.errors[].reason` (real GCP: "notFound",
	// "alreadyExists"). Real GCP populates it; the emulator's envelope is
	// details-based, so it is only asserted when the client returned at least
	// one ErrorItem.
	Reason string
	// MessageContains is asserted against the message when non-empty.
	MessageContains string
}

func (s ErrorSpec) String() string {
	var parts []string
	if s.HTTPStatus != 0 {
		parts = append(parts, fmt.Sprintf("HTTP %d", s.HTTPStatus))
	}
	if s.Code != codes.OK {
		parts = append(parts, fmt.Sprintf("gRPC %s", s.Code))
	}
	if s.Status != "" {
		parts = append(parts, s.Status)
	}
	if s.Reason != "" {
		parts = append(parts, fmt.Sprintf("reason %q", s.Reason))
	}
	if s.MessageContains != "" {
		parts = append(parts, fmt.Sprintf("message containing %q", s.MessageContains))
	}
	return strings.Join(parts, " / ")
}

// The documented envelope catalogue. These are the negatives every service's p3
// class asserts: the code pair and the google.rpc status name real GCP returns.
// A service that also fixes the apiary reason or the message text extends the
// spec (`s.Reason = "notFound"`).
func SpecNotFound() ErrorSpec         { return spec(codes.NotFound, 404) }
func SpecAlreadyExists() ErrorSpec    { return spec(codes.AlreadyExists, 409) }
func SpecInvalidArgument() ErrorSpec  { return spec(codes.InvalidArgument, 400) }
func SpecPermissionDenied() ErrorSpec { return spec(codes.PermissionDenied, 403) }
func SpecAborted() ErrorSpec          { return spec(codes.Aborted, 409) }
func SpecUnimplemented() ErrorSpec    { return spec(codes.Unimplemented, 501) }

// SpecPreconditionFailed is the google.rpc.Code pairing for FAILED_PRECONDITION:
// HTTP 400 ("the operation was rejected because the system is not in a state
// required for the operation's execution"). The service cores in this repo build
// it with 400 (kms, tasks, scheduler, datastore, resourcemanager, logging), which
// is what the conformance validator's 400 row accepts.
func SpecPreconditionFailed() ErrorSpec { return spec(codes.FailedPrecondition, 400) }

// SpecConditionalRequestFailed is the HTTP 412 variant of FAILED_PRECONDITION:
// the same google.rpc status, returned when a *conditional request*'s
// precondition (an etag `If-Match`) does not hold — the one documented 412 case
// in GCP (Cloud Storage object/conditional writes).
func SpecConditionalRequestFailed() ErrorSpec { return spec(codes.FailedPrecondition, 412) }

func spec(code codes.Code, httpStatus int) ErrorSpec {
	return ErrorSpec{HTTPStatus: httpStatus, Code: code, Status: StatusName(code)}
}

// StatusName renders a gRPC code the way the google.rpc status is spelled in the
// REST envelope: codes.NotFound -> "NOT_FOUND", codes.AlreadyExists ->
// "ALREADY_EXISTS".
func StatusName(code codes.Code) string {
	// codes.Canceled is the one enum whose google.rpc spelling doubles a letter
	// ("Canceled" -> "CANCELLED", the name both gcperr and the conformance
	// validator use).
	if code == codes.Canceled {
		return "CANCELLED"
	}
	name := code.String()
	var b strings.Builder
	var prev rune
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			// A word boundary, not an acronym: NotFound -> NOT_FOUND, OK -> OK.
			if prev >= 'a' && prev <= 'z' || prev >= '0' && prev <= '9' {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
		default:
			b.WriteRune(r)
		}
		prev = r
	}
	return b.String()
}

// StatusOf extracts the typed error an official client returned: a
// *googleapi.Error over REST, a gRPC status over gRPC, and the error's type for
// anything else. The bool is false only when err is nil.
func StatusOf(err error) (Observed, bool) {
	if err == nil {
		return Observed{}, false
	}
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		obs := Observed{
			Kind:       KindHTTP,
			HTTPStatus: ge.Code,
			Status:     httpStatusToken(ge),
			Message:    ge.Message,
		}
		if len(ge.Errors) > 0 {
			obs.Reason = ge.Errors[0].Reason
		}
		return obs, true
	}
	if st, ok := status.FromError(err); ok {
		return Observed{
			Kind:    KindGRPC,
			Code:    st.Code(),
			Status:  StatusName(st.Code()),
			Message: st.Message(),
		}, true
	}
	return Observed{Kind: KindOther, Message: err.Error()}, true
}

// httpStatusToken reads the google.rpc status name out of the REST error
// envelope ({"error":{"code":404,"message":"…","status":"NOT_FOUND"}}).
func httpStatusToken(ge *googleapi.Error) string {
	if len(ge.Body) == 0 {
		return ""
	}
	var env struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(ge.Body), &env); err != nil {
		return ""
	}
	return env.Error.Status
}

// CheckError returns one message per way err fails to match a documented
// negative, and no message when it matches. It is pure, so the kit's own tests
// can drive a wrong code or an unwrapped error through it.
func CheckError(err error, want ErrorSpec) []string {
	if err == nil {
		return []string{fmt.Sprintf("expected an error matching %s, the client returned none", want)}
	}
	if want.HTTPStatus == 0 && want.Code == codes.OK {
		return []string{"ErrorSpec names neither HTTPStatus nor Code"}
	}
	obs, _ := StatusOf(err)
	var out []string
	switch obs.Kind {
	case KindHTTP:
		if want.HTTPStatus == 0 {
			out = append(out, fmt.Sprintf("the client returned an HTTP error (%s) but the spec only names the gRPC code %s",
				obs, want.Code))
		} else if obs.HTTPStatus != want.HTTPStatus {
			out = append(out, fmt.Sprintf("expected HTTP %d, the client returned HTTP %d (%s)",
				want.HTTPStatus, obs.HTTPStatus, obs.Message))
		}
	case KindGRPC:
		if want.Code == codes.OK {
			out = append(out, fmt.Sprintf("the client returned a gRPC status (%s) but the spec only names HTTP %d",
				obs, want.HTTPStatus))
		} else if obs.Code != want.Code {
			out = append(out, fmt.Sprintf("expected gRPC %s, the client returned %s (%s)",
				want.Code, obs.Code, obs.Message))
		}
	default:
		out = append(out, fmt.Sprintf("the client returned a non-status error (%T: %s); the official client must surface the typed error",
			err, err))
	}
	if want.Status != "" && obs.Kind != KindOther && obs.Status != want.Status {
		out = append(out, fmt.Sprintf("expected status %s, the client reported %q", want.Status, obs.Status))
	}
	if want.Reason != "" && obs.Kind == KindHTTP && obs.Reason != "" && obs.Reason != want.Reason {
		out = append(out, fmt.Sprintf("expected reason %q, the envelope carries %q", want.Reason, obs.Reason))
	}
	if want.MessageContains != "" && !strings.Contains(obs.Message, want.MessageContains) {
		out = append(out, fmt.Sprintf("expected a message containing %q, the client reported %q", want.MessageContains, obs.Message))
	}
	return out
}

// AssertError fails t unless err matches the documented negative.
func AssertError(t TB, err error, want ErrorSpec) {
	t.Helper()
	for _, violation := range CheckError(err, want) {
		t.Errorf("error envelope: %s", violation)
	}
}

// NegativeCase is one row of a p3 negative table.
type NegativeCase struct {
	Name string
	Run  func() error
	Want ErrorSpec
}

// RunNegatives runs a negative table as subtests, asserting each documented
// envelope.
func RunNegatives(t *testing.T, cases []NegativeCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			AssertError(t, tc.Run(), tc.Want)
		})
	}
}
