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

	// setIamPolicy body is parsed and surfaced.
	body := []byte(`{"policy":{"bindings":[]}}`)
	nr, err := c.Decode(httptest.NewRequest("POST", "/v1/projects/p:setIamPolicy", bytes.NewReader(body)), body)
	if err != nil {
		t.Fatalf("setIamPolicy decode: %v", err)
	}
	if _, ok := nr.Params["body"].(map[string]any); !ok {
		t.Errorf("expected parsed body, got %#v", nr.Params["body"])
	}

	// A trailing resource segment and an unknown custom verb are rejected.
	if _, err := c.Decode(httptest.NewRequest("GET", "/v1/projects/p/topics/t", nil), nil); err == nil {
		t.Error("expected unsupported operation error for trailing resource segment")
	}
	if _, err := c.Decode(httptest.NewRequest("POST", "/v1/projects/p:undelete", nil), nil); err == nil {
		t.Error("expected unsupported operation error for :undelete")
	}
}
