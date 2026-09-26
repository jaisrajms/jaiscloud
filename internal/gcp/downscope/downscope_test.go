package downscope

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/clock"
)

// option builds the STS `options` JSON for one rule.
func option(bucket, expression string, permissions ...string) string {
	rule := map[string]any{
		"availableResource":    "//storage.googleapis.com/projects/_/buckets/" + bucket,
		"availablePermissions": permissions,
	}
	if expression != "" {
		rule["availabilityCondition"] = map[string]any{"expression": expression}
	}
	doc := map[string]any{"accessBoundary": map[string]any{"accessBoundaryRules": []any{rule}}}
	b, _ := json.Marshal(doc)
	return string(b)
}

func TestParseOptionsBucketWide(t *testing.T) {
	rules, err := ParseOptions(option("bkt", "", PermissionObjectViewer))
	if err != nil {
		t.Fatalf("ParseOptions: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(rules))
	}
	if rules[0].Bucket != "bkt" || rules[0].ObjectPrefix != "" {
		t.Errorf("unexpected rule %+v", rules[0])
	}
}

func TestParseOptionsPrefixConditions(t *testing.T) {
	expr := "resource.name.startsWith('projects/_/buckets/bkt/objects/allowed/')"
	rules, err := ParseOptions(option("bkt", expr, PermissionObjectViewer))
	if err != nil {
		t.Fatalf("ParseOptions(resource.name): %v", err)
	}
	if rules[0].ObjectPrefix != "allowed/" {
		t.Errorf("objectPrefix = %q, want allowed/", rules[0].ObjectPrefix)
	}

	// The list-prefix attribute form, and the OR of both forms, must resolve to
	// the same prefix.
	list := "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('allowed/')"
	for _, expression := range []string{list, expr + " || " + list, expr + " || " + list + " || " + expr} {
		rules, err = ParseOptions(option("bkt", expression, PermissionObjectViewer))
		if err != nil {
			t.Fatalf("ParseOptions(%q): %v", expression, err)
		}
		if rules[0].ObjectPrefix != "allowed/" {
			t.Errorf("objectPrefix = %q, want allowed/ (%q)", rules[0].ObjectPrefix, expression)
		}
	}

	// A trailing slash is added when the prefix omits it.
	rules, err = ParseOptions(option("bkt", "resource.name.startsWith('projects/_/buckets/bkt/objects/allowed')", PermissionObjectViewer))
	if err != nil {
		t.Fatalf("ParseOptions(no trailing slash): %v", err)
	}
	if rules[0].ObjectPrefix != "allowed/" {
		t.Errorf("objectPrefix = %q, want allowed/", rules[0].ObjectPrefix)
	}
}

func TestParseOptionsErrors(t *testing.T) {
	prefix := "resource.name.startsWith('projects/_/buckets/bkt/objects/allowed/')"
	list := "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('allowed/')"
	cases := []struct {
		name    string
		options string
	}{
		{"empty", ""},
		{"not json", "{not json"},
		{"no rules", `{"accessBoundary":{"accessBoundaryRules":[]}}`},
		{"unsupported resource", `{"accessBoundary":{"accessBoundaryRules":[{"availableResource":"//compute.googleapis.com/projects/p/zones/z","availablePermissions":["inRole:roles/storage.objectViewer"]}]}}`},
		{"no permissions", option("bkt", "")},
		{"unsupported permission", option("bkt", "", "inRole:roles/storage.admin")},
		{"empty expression", option("bkt", " ", PermissionObjectViewer)},
		{"unsupported expression", option("bkt", "resource.name.endsWith('x')", PermissionObjectViewer)},
		{"prefix wrong bucket", option("bkt", "resource.name.startsWith('projects/_/buckets/other/objects/allowed/')", PermissionObjectViewer)},
		{"mismatched prefix", option("bkt", prefix+" || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('other/')", PermissionObjectViewer)},
		{"unsupported list attribute", option("bkt", "api.getAttribute('storage.googleapis.com/other', '').startsWith('allowed/')", PermissionObjectViewer)},
		{"non-empty list default", option("bkt", "api.getAttribute('storage.googleapis.com/objectListPrefix', 'x').startsWith('allowed/')", PermissionObjectViewer)},
		{"unterminated literal", option("bkt", "resource.name.startsWith('unterminated)", PermissionObjectViewer)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseOptions(tc.options); err == nil {
				t.Fatalf("expected an error for %s", tc.name)
			}
		})
	}
	// Sanity: the list default must be the empty string literal.
	if _, err := ParseOptions(option("bkt", list, PermissionObjectViewer)); err != nil {
		t.Fatalf("valid list expression rejected: %v", err)
	}
}

// token builds a downscoped bearer token carrying the given rules.
func token(t *testing.T, rules []Rule) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"access_boundary": rules})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return TokenPrefix + "hdr." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestAllowedNonDownscopedIsUnrestricted(t *testing.T) {
	for _, auth := range []string{"", "Bearer some-access-token", "Bearer floci-gcp-impersonated-abc"} {
		if err := Allowed(auth, ReadObject, "bkt", "anything"); err != nil {
			t.Errorf("Allowed(%q) = %v, want nil", auth, err)
		}
	}
}

func TestAllowedObjectOps(t *testing.T) {
	rules := []Rule{{Bucket: "bkt", ObjectPrefix: "allowed/", Permissions: []string{PermissionObjectViewer, PermissionLegacyBucketWriter}}}
	auth := "Bearer " + token(t, rules)

	cases := []struct {
		op     Op
		bucket string
		name   string
		want   bool
	}{
		{ReadObject, "bkt", "allowed/file.txt", true},
		{ReadObject, "bkt", "allowed_sibling/file.txt", false},
		{ReadObject, "bkt", "allowed", false},
		{ReadObject, "other", "allowed/file.txt", false},
		{WriteObject, "bkt", "allowed/file.txt", true},
		{WriteObject, "bkt", "allowed_sibling/file.txt", false},
		{DeleteObject, "bkt", "allowed/file.txt", true},
		{DeleteObject, "bkt", "allowed_sibling/file.txt", false},
		{List, "bkt", "allowed/", true},
		{List, "bkt", "allowed", false},
		{List, "bkt", "allowed_sibling/", false},
		{List, "bkt", "", false},
		{BucketAdmin, "bkt", "", false},
	}
	for _, tc := range cases {
		err := Allowed(auth, tc.op, tc.bucket, tc.name)
		if tc.want && err != nil {
			t.Errorf("Allowed(op=%d, bucket=%s, name=%q) = %v, want nil", tc.op, tc.bucket, tc.name, err)
		}
		if !tc.want && !errors.Is(err, ErrDenied) {
			t.Errorf("Allowed(op=%d, bucket=%s, name=%q) = %v, want ErrDenied", tc.op, tc.bucket, tc.name, err)
		}
	}
}

func TestAllowedPermissionsPerOp(t *testing.T) {
	// objectViewer may read and list but not write/delete.
	viewer := "Bearer " + token(t, []Rule{{Bucket: "bkt", Permissions: []string{PermissionObjectViewer}}})
	if err := Allowed(viewer, ReadObject, "bkt", "o"); err != nil {
		t.Errorf("objectViewer read: %v", err)
	}
	if err := Allowed(viewer, List, "bkt", ""); err != nil {
		t.Errorf("objectViewer list: %v", err)
	}
	if !errors.Is(Allowed(viewer, WriteObject, "bkt", "o"), ErrDenied) {
		t.Error("objectViewer must not write")
	}
	if !errors.Is(Allowed(viewer, DeleteObject, "bkt", "o"), ErrDenied) {
		t.Error("objectViewer must not delete")
	}

	// legacyObjectReader may read but not list/write/delete.
	reader := "Bearer " + token(t, []Rule{{Bucket: "bkt", Permissions: []string{PermissionLegacyObjectReader}}})
	if err := Allowed(reader, ReadObject, "bkt", "o"); err != nil {
		t.Errorf("legacyObjectReader read: %v", err)
	}
	if !errors.Is(Allowed(reader, List, "bkt", ""), ErrDenied) {
		t.Error("legacyObjectReader must not list")
	}
	if !errors.Is(Allowed(reader, WriteObject, "bkt", "o"), ErrDenied) {
		t.Error("legacyObjectReader must not write")
	}

	// legacyBucketWriter may list/write/delete but not read bytes.
	writer := "Bearer " + token(t, []Rule{{Bucket: "bkt", Permissions: []string{PermissionLegacyBucketWriter}}})
	if err := Allowed(writer, WriteObject, "bkt", "o"); err != nil {
		t.Errorf("legacyBucketWriter write: %v", err)
	}
	if err := Allowed(writer, DeleteObject, "bkt", "o"); err != nil {
		t.Errorf("legacyBucketWriter delete: %v", err)
	}
	if err := Allowed(writer, List, "bkt", ""); err != nil {
		t.Errorf("legacyBucketWriter list: %v", err)
	}
	if !errors.Is(Allowed(writer, ReadObject, "bkt", "o"), ErrDenied) {
		t.Error("legacyBucketWriter must not read")
	}
}

func TestAllowedWholeBucketRule(t *testing.T) {
	auth := "Bearer " + token(t, []Rule{{Bucket: "bkt", Permissions: []string{PermissionObjectViewer}}})
	if err := Allowed(auth, ReadObject, "bkt", "nested/deep/file.txt"); err != nil {
		t.Errorf("whole-bucket read: %v", err)
	}
	if err := Allowed(auth, List, "bkt", ""); err != nil {
		t.Errorf("whole-bucket list: %v", err)
	}
}

func TestAllowedMalformedDownscopedTokenDenies(t *testing.T) {
	for _, token := range []string{
		TokenPrefix + "garbage",
		TokenPrefix + "a.b", // payload not valid base64
		TokenPrefix + "a." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".c",
		TokenPrefix + "hdr." + base64.RawURLEncoding.EncodeToString([]byte(`{}`)) + ".sig", // no boundary
	} {
		if err := Allowed("Bearer "+token, ReadObject, "bkt", "o"); !errors.Is(err, ErrDenied) {
			t.Errorf("Allowed(%q) = %v, want ErrDenied", token, err)
		}
	}
}

func TestRulesFromAuthorization(t *testing.T) {
	rules := []Rule{{Bucket: "bkt", ObjectPrefix: "p/", Permissions: []string{PermissionObjectViewer}}}
	got, downscoped := RulesFromAuthorization("Bearer " + token(t, rules))
	if !downscoped || len(got) != 1 || got[0].Bucket != "bkt" {
		t.Fatalf("RulesFromAuthorization = %+v, %v", got, downscoped)
	}
	if _, downscoped := RulesFromAuthorization("Bearer ordinary"); downscoped {
		t.Error("ordinary token must not be reported as downscoped")
	}
	if _, downscoped := RulesFromAuthorization("Bearer " + TokenPrefix + "x"); !downscoped {
		t.Error("prefixed token must be reported as downscoped even if malformed")
	}
}

// TestTokenFromClockIndependent asserts the boundary round-trips regardless of
// payload ordering by re-encoding with the identity claims an access token
// carries.
func TestTokenWithIdentityClaims(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"email":           "sa@example.com",
		"sub":             "sa@example.com",
		"project_id":      "proj",
		"exp":             clock.RealNow().Unix() + 3600,
		"access_boundary": []Rule{{Bucket: "bkt", Permissions: []string{PermissionObjectViewer}}},
	})
	auth := "Bearer " + TokenPrefix + "hdr." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
	if err := Allowed(auth, ReadObject, "bkt", "o"); err != nil {
		t.Fatalf("expected identity-carrying downscoped token to allow read: %v", err)
	}
}
