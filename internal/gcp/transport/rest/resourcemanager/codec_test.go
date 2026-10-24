package resourcemanager

import (
	"bytes"
	"net/http/httptest"
	"testing"
)

func TestCodecDecode(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method, path, action string
	}{
		{"GET", "/v1/projects/p", "ProjectGet"},
		{"PUT", "/v1/projects/p", "ProjectUpdate"},
		{"DELETE", "/v1/projects/p", "ProjectDelete"},
		{"POST", "/v1/projects/p:undelete", "ProjectUndelete"},
		{"POST", "/v1/projects/p:getIamPolicy", "ProjectGetIamPolicy"},
		{"POST", "/v1/projects/p:setIamPolicy", "ProjectSetIamPolicy"},
		{"POST", "/v1/projects/p:testIamPermissions", "ProjectTestIamPermissions"},
	}
	for _, tc := range cases {
		nr, err := c.Decode(httptest.NewRequest(tc.method, tc.path, nil), nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Params["project"] != "p" {
			t.Errorf("%s %s: project = %v, want p", tc.method, tc.path, nr.Params["project"])
		}
	}

	// The collection route has no project segment and decodes to create/list.
	for _, tc := range []struct {
		method, path, action string
	}{
		{"POST", "/v1/projects", "ProjectCreate"},
		{"GET", "/v1/projects", "ProjectList"},
	} {
		nr, err := c.Decode(httptest.NewRequest(tc.method, tc.path, nil), nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if _, ok := nr.Params["project"]; ok {
			t.Errorf("%s %s: collection route must not set a project param", tc.method, tc.path)
		}
	}

	// setIamPolicy body is parsed and surfaced.
	body := []byte(`{"policy":{"bindings":[]}}`)
	nr, err := c.Decode(httptest.NewRequest("POST", "/v1/projects/p:setIamPolicy", bytes.NewReader(body)), body)
	if err != nil {
		t.Fatalf("setIamPolicy decode: %v", err)
	}
	if _, ok := nr.Params["body"].(map[string]any); !ok {
		t.Errorf("expected parsed body, got %#v", nr.Params["body"])
	}

	// A trailing resource segment, an unknown custom verb, and an unsupported
	// collection method are rejected.
	if _, err := c.Decode(httptest.NewRequest("GET", "/v1/projects/p/topics/t", nil), nil); err == nil {
		t.Error("expected unsupported operation error for trailing resource segment")
	}
	if _, err := c.Decode(httptest.NewRequest("POST", "/v1/projects/p:move", nil), nil); err == nil {
		t.Error("expected unsupported operation error for :move")
	}
	if _, err := c.Decode(httptest.NewRequest("DELETE", "/v1/projects", nil), nil); err == nil {
		t.Error("expected unsupported operation error for DELETE /v1/projects")
	}
}
