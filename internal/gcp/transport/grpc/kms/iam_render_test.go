package kms

import (
	"testing"

	"jaiscloud/internal/gcp/policy"
)

// TestPolicyToProtoOmitsDefaultVersion pins the AUD3-16 fix: the KMS gRPC
// renderer mirrors the REST renderer (internal/gcp/transport/rest/kms.iamPolicyMap,
// which matches real Cloud KMS), so a default/empty policy renders without the
// semantic default version 1 — matching the `{"etag": ...}` REST body — while a
// non-default version is preserved. A regression here reintroduces the
// cross-transport divergence the parity harness caught (the same stored policy
// rendered `version: 1` over gRPC but not over REST).
func TestPolicyToProtoOmitsDefaultVersion(t *testing.T) {
	def := policyToProto(policy.Policy{Version: 1, Etag: "ACAB", Bindings: []any{}})
	if got := def.GetVersion(); got != 0 {
		t.Errorf("default policy version = %d, want 0 (omitted)", got)
	}
	if got := len(def.GetBindings()); got != 0 {
		t.Errorf("default policy bindings = %d, want 0 (omitted)", got)
	}
	if string(def.GetEtag()) != "ACAB" {
		t.Errorf("default policy etag = %q, want ACAB", def.GetEtag())
	}

	v3 := policyToProto(policy.Policy{
		Version: 3,
		Bindings: []any{map[string]any{
			"role": "roles/viewer", "members": []any{"user:a@example.com"},
		}},
	})
	if got := v3.GetVersion(); got != 3 {
		t.Errorf("version-3 policy version = %d, want 3", got)
	}
	if got := len(v3.GetBindings()); got != 1 {
		t.Fatalf("version-3 policy bindings = %d, want 1", got)
	}
	if got := v3.GetBindings()[0].GetRole(); got != "roles/viewer" {
		t.Errorf("version-3 policy role = %q, want roles/viewer", got)
	}
}
