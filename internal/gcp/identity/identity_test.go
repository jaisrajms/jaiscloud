package identity

import (
	"net/http/httptest"
	"testing"
)

func TestFromRequest_BearerTokenEmail(t *testing.T) {
	// Header: {"alg":"HS256"}, Payload: {"email":"dev@example.com","sub":"dev"},
	// Signature ignored.
	token := "eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImRldkBleGFtcGxlLmNvbSIsInN1YiI6ImRldiJ9.sig"
	r := httptest.NewRequest("GET", "/v1/projects/my-proj/topics/t", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	got := FromRequest(r)
	if got.ProjectID != "my-proj" {
		t.Errorf("expected project from path, got %q", got.ProjectID)
	}
	if got.ServiceAccount != "dev@example.com" {
		t.Errorf("expected SA from token, got %q", got.ServiceAccount)
	}
	if got.AccessKey != token {
		t.Errorf("expected access key = token")
	}
}

func TestServiceAccountFromToken(t *testing.T) {
	token := "eyJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImRldkBleGFtcGxlLmNvbSIsInN1YiI6ImRldiJ9.sig"
	if got := ServiceAccountFromToken(token); got != "dev@example.com" {
		t.Errorf("ServiceAccountFromToken = %q, want dev@example.com", got)
	}
	// Falls back to sub when email is absent.
	token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdWJAb25seSJ9.sig"
	if got := ServiceAccountFromToken(token); got != "sub@only" {
		t.Errorf("ServiceAccountFromToken = %q, want sub@only", got)
	}
	if got := ServiceAccountFromToken("opaque-token"); got != "" {
		t.Errorf("ServiceAccountFromToken(opaque) = %q, want empty", got)
	}
}

func TestFromRequest_NoCredential(t *testing.T) {
	r := httptest.NewRequest("GET", "/storage/v1/b/bkt/o", nil)
	got := FromRequest(r)
	if got.ProjectID != DefaultProjectID {
		t.Errorf("expected default project, got %q", got.ProjectID)
	}
	if got.Source != SourceDefault {
		t.Errorf("expected SourceDefault, got %v", got.Source)
	}
}

func TestProjectFromPath(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/topics/t":        "p",
		"/v2/projects/another/secrets/s": "another",
		"/storage/v1/b/bkt/o":            "",
		"/v1beta/projects/beta/topics/t": "beta",
		// Project-segment custom method (Cloud Resource Manager): the ':' must
		// not be captured as part of the project id.
		"/v1/projects/p:getIamPolicy": "p",
	}
	for in, want := range cases {
		if got := ProjectFromPath(in); got != want {
			t.Errorf("ProjectFromPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFromRequest_QueryProject(t *testing.T) {
	r := httptest.NewRequest("POST", "/storage/v1/b?project=proj", nil)
	got := FromRequest(r)
	if got.ProjectID != "proj" {
		t.Errorf("expected project from ?project= query, got %q", got.ProjectID)
	}
	if got.Source == SourceDefault {
		t.Error("expected an explicit (non-default) source when ?project= is present")
	}
}

func TestFromRequest_PathWinsOverQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/projects/path-proj/topics/t?project=query-proj", nil)
	got := FromRequest(r)
	if got.ProjectID != "path-proj" {
		t.Errorf("expected path project to win, got %q", got.ProjectID)
	}
}
