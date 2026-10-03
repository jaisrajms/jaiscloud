package gcp

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestMetadataRoutesRequireFlavor(t *testing.T) {
	r := chi.NewRouter()
	RegisterMetadataRoutes(r, MetadataConfig{ProjectID: "proj", ServiceAccount: "sa@example.com"})

	// Without the header → 400.
	req := httptest.NewRequest("GET", "/computeMetadata/v1/project/project-id", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 without Metadata-Flavor, got %d", rec.Code)
	}

	// With the header → 200 + body.
	req = httptest.NewRequest("GET", "/computeMetadata/v1/project/project-id", nil)
	req.Header.Set("Metadata-Flavor", "Google")
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "proj" {
		t.Errorf("expected project id 'proj', got %q", rec.Body.String())
	}
}

func TestMockAccessToken(t *testing.T) {
	token := mockAccessToken("sa@example.com", "proj")
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	if claims["email"] != "sa@example.com" {
		t.Errorf("expected email sa@example.com, got %v", claims["email"])
	}
	if claims["project_id"] != "proj" {
		t.Errorf("expected project_id proj, got %v", claims["project_id"])
	}
}
