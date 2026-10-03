package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// stubThrottle is a test double implementing ThrottleController.
type stubThrottle struct {
	applied []byte
	status  []byte
	err     error
}

func (s *stubThrottle) SetThrottleConfig(body []byte) ([]byte, error) {
	s.applied = body
	if s.err != nil {
		return nil, s.err
	}
	return s.status, nil
}

func (s *stubThrottle) ThrottleStatus() []byte { return s.status }

func TestAdminThrottleSet(t *testing.T) {
	s := &stubThrottle{status: []byte(`{"enabled":true}`)}
	h := NewHandler()
	h.RegisterThrottle(s)

	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/throttle", strings.NewReader(`{"mode":"fault"}`))
	rec := httptest.NewRecorder()
	h.SetThrottle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type: %q", got)
	}
	if string(s.applied) != `{"mode":"fault"}` {
		t.Fatalf("body forwarded: %q", s.applied)
	}
	if rec.Body.String() != `{"enabled":true}` {
		t.Fatalf("status body: %q", rec.Body.String())
	}
}

func TestAdminThrottleSetRejectsBadRequest(t *testing.T) {
	h := NewHandler()
	h.RegisterThrottle(&stubThrottle{err: errors.New("unknown mode")})
	req := httptest.NewRequest(http.MethodPost, "/_jaiscloud/throttle", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.SetThrottle(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: %d", rec.Code)
	}
}

func TestAdminThrottleGet(t *testing.T) {
	h := NewHandler()
	h.RegisterThrottle(&stubThrottle{status: []byte(`{"enabled":false,"mode":"off"}`)})
	req := httptest.NewRequest(http.MethodGet, "/_jaiscloud/throttle", nil)
	rec := httptest.NewRecorder()
	h.GetThrottle(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"enabled":false,"mode":"off"}` {
		t.Fatalf("status body: %d %q", rec.Code, rec.Body.String())
	}
}

func TestAdminThrottleNotConfigured(t *testing.T) {
	h := NewHandler()
	for _, tc := range []struct {
		name   string
		method string
		fn     func(http.ResponseWriter, *http.Request)
	}{
		{"set", http.MethodPost, h.SetThrottle},
		{"get", http.MethodGet, h.GetThrottle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.fn(rec, httptest.NewRequest(tc.method, "/_jaiscloud/throttle", nil))
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status: %d", rec.Code)
			}
		})
	}
}
