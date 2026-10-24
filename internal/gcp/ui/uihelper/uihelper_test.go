package uihelper

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestPathParam(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"plain", "users", "users"},
		// A client-encoded path separator: chi routes on RawPath, so it must be
		// percent-decoded once.
		{"encoded slash", "users%2Falice%2Forders", "users/alice/orders"},
		// A literal "%2F" in an id is encoded as "%252F". chi routes on the
		// already-decoded Path here, so it must NOT be decoded again.
		{"literal percent-2F", "a%252Fb", "a%2Fb"},
		{"literal percent", "a%25b", "a%b"},
		{"literal 100 percent", "100%25", "100%"},
		{"plus", "a%2Bb", "a+b"},
		{"ampersand", "a%26b", "a&b"},
		{"colon", "a%3Ab", "a:b"},
		{"non-ascii", "caf%C3%A9", "café"},
		{"space", "a%20b", "a b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chi.NewRouter()
			var got string
			r.Get("/x/{v}", func(w http.ResponseWriter, req *http.Request) {
				got = PathParam(req, "v")
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/x/"+tt.raw, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("route did not match: status %d", rec.Code)
			}
			if got != tt.want {
				t.Errorf("PathParam(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
