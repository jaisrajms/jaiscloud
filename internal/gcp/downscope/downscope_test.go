package downscope

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
)

// option builds the STS `options` JSON for one rule.
func option(bucket, expression string, permissions ...string) string {
	return optionFor("//storage.googleapis.com/projects/_/buckets/"+bucket, expression, permissions...)
}

// optionFor builds the STS `options` JSON for one rule over an arbitrary
// availableResource string.
func optionFor(resource, expression string, permissions ...string) string {
	rule := map[string]any{
		"availableResource":    resource,
		"availablePermissions": permissions,
	}
	if expression != "" {
		rule["availabilityCondition"] = map[string]any{"expression": expression}
	}
	doc := map[string]any{"accessBoundary": map[string]any{"accessBoundaryRules": []any{rule}}}
	b, _ := json.Marshal(doc)
	return string(b)
}

// TestParseOptionsResourceTypes pins the documented resource surface: Credential
// Access Boundaries apply to Cloud Storage buckets only, and the accepted
// availableResource is the bucket full resource name. Object- and folder-level
// restrictions use the availability condition instead.
func TestParseOptionsResourceTypes(t *testing.T) {
	valid := []struct {
		name     string
		resource string
		bucket   string
	}{
		{"bucket", "//storage.googleapis.com/projects/_/buckets/my-bucket", "my-bucket"},
		{"bucket with digits and dots", "//storage.googleapis.com/projects/_/buckets/bkt.2024.data", "bkt.2024.data"},
		{"bucket with underscores", "//storage.googleapis.com/projects/_/buckets/my_bucket", "my_bucket"},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			rules, err := ParseOptions(optionFor(tc.resource, "", PermissionObjectViewer))
			if err != nil {
				t.Fatalf("ParseOptions(%q): %v", tc.resource, err)
			}
			if len(rules) != 1 || rules[0].Bucket != tc.bucket {
				t.Fatalf("rules = %+v, want bucket %q", rules, tc.bucket)
			}
			if rules[0].AvailableResource != tc.resource {
				t.Errorf("AvailableResource = %q, want %q", rules[0].AvailableResource, tc.resource)
			}
			if rules[0].AvailabilityCondition != nil {
				t.Errorf("condition = %+v, want nil", rules[0].AvailabilityCondition)
			}
		})
	}

	// Resource names the CAB documentation does not admit. Non-Cloud-Storage
	// services do not support Credential Access Boundaries, so accepting them
	// would hand the client a boundary the emulator does not enforce.
	invalid := []struct {
		name     string
		resource string
	}{
		{"bigquery dataset", "//bigquery.googleapis.com/projects/p/datasets/d"},
		{"bigquery table", "//bigquery.googleapis.com/projects/p/datasets/d/tables/t"},
		{"spanner database", "//spanner.googleapis.com/projects/p/instances/i/databases/d"},
		{"kms crypto key", "//cloudkms.googleapis.com/projects/p/locations/global/keyRings/r/cryptoKeys/k"},
		{"cloudresourcemanager project", "//cloudresourcemanager.googleapis.com/projects/p"},
		{"compute zone", "//compute.googleapis.com/projects/p/zones/z"},
		{"object resource", "//storage.googleapis.com/projects/_/buckets/b/objects/o"},
		{"non-underscore project segment", "//storage.googleapis.com/projects/my-project/buckets/b"},
		{"missing scheme", "storage.googleapis.com/projects/_/buckets/b"},
		{"empty bucket", "//storage.googleapis.com/projects/_/buckets/"},
		{"trailing slash", "//storage.googleapis.com/projects/_/buckets/b/"},
		{"empty", ""},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseOptions(optionFor(tc.resource, "", PermissionObjectViewer))
			if err == nil {
				t.Fatalf("ParseOptions(%q) succeeded, want an error", tc.resource)
			}
			if !strings.Contains(err.Error(), "Cloud Storage buckets only") {
				t.Errorf("error = %v, want the documented Cloud Storage resource message", err)
			}
		})
	}
}

// TestParseOptionsDocumentedLimits covers the documented boundary caps: at most
// 10 access boundary rules and at most 2048 characters in an availability
// condition expression.
func TestParseOptionsDocumentedLimits(t *testing.T) {
	rule := map[string]any{
		"availableResource":    "//storage.googleapis.com/projects/_/buckets/bkt",
		"availablePermissions": []string{PermissionObjectViewer},
	}
	build := func(n int) string {
		rules := make([]any, n)
		for i := range rules {
			rules[i] = rule
		}
		doc := map[string]any{"accessBoundary": map[string]any{"accessBoundaryRules": rules}}
		b, _ := json.Marshal(doc)
		return string(b)
	}
	if rules, err := ParseOptions(build(maxRules)); err != nil || len(rules) != maxRules {
		t.Fatalf("ParseOptions(%d rules) = %d rules, %v; want %d rules", maxRules, len(rules), err, maxRules)
	}
	if _, err := ParseOptions(build(maxRules + 1)); err == nil {
		t.Fatalf("ParseOptions(%d rules) succeeded, want an error", maxRules+1)
	}

	expression := func(extra int) string {
		const head = "resource.name.startsWith('projects/_/buckets/bkt/objects/"
		const tail = "')"
		pad := maxExpressionLength - len(head) - len(tail) + extra
		return head + strings.Repeat("a", pad) + tail
	}
	atLimit := expression(0)
	if len(atLimit) != maxExpressionLength {
		t.Fatalf("test expression is %d characters, want %d", len(atLimit), maxExpressionLength)
	}
	wantPrefix := strings.Repeat("a", maxExpressionLength-len("resource.name.startsWith('projects/_/buckets/bkt/objects/")-len("')")) + "/"
	if rules, err := ParseOptions(option("bkt", atLimit, PermissionObjectViewer)); err != nil {
		t.Fatalf("ParseOptions(%d-character expression): %v", maxExpressionLength, err)
	} else if rules[0].ObjectPrefix != wantPrefix {
		t.Errorf("objectPrefix length = %d, want %d", len(rules[0].ObjectPrefix), len(wantPrefix))
	}
	if _, err := ParseOptions(option("bkt", expression(1), PermissionObjectViewer)); err == nil {
		t.Fatalf("ParseOptions(%d-character expression) succeeded, want an error", maxExpressionLength+1)
	}
}

// TestParseOptionsConditionRoundTrip parses the documented object-prefix example
// and checks that the condition — expression, title and description — survives
// the minted token's access_boundary claim in the documented JSON shape.
func TestParseOptionsConditionRoundTrip(t *testing.T) {
	const expression = "resource.name.startsWith('projects/_/buckets/example-bucket/objects/customer-a/invoices/') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('customer-a/invoices/')"
	const options = `{"accessBoundary":{"accessBoundaryRules":[{
		"availablePermissions":["inRole:roles/storage.objectViewer"],
		"availableResource":"//storage.googleapis.com/projects/_/buckets/example-bucket",
		"availabilityCondition":{
			"expression":"` + expression + `",
			"title":"Customer A invoices",
			"description":"Read and list Customer A invoices only"
		}}]}}`

	rules, err := ParseOptions(options)
	if err != nil {
		t.Fatalf("ParseOptions: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1", len(rules))
	}
	rule := rules[0]
	if rule.Bucket != "example-bucket" || rule.ObjectPrefix != "customer-a/invoices/" {
		t.Errorf("rule = %+v, want bucket example-bucket prefix customer-a/invoices/", rule)
	}
	wantCondition := AvailabilityCondition{
		Expression:  expression,
		Title:       "Customer A invoices",
		Description: "Read and list Customer A invoices only",
	}
	if rule.AvailabilityCondition == nil || *rule.AvailabilityCondition != wantCondition {
		t.Fatalf("condition = %+v, want %+v", rule.AvailabilityCondition, wantCondition)
	}

	// The minted token carries the boundary, and the claim round-trips the
	// documented rule fields.
	auth := "Bearer " + token(t, rules)
	got, downscoped := RulesFromAuthorization(auth)
	if !downscoped || len(got) != 1 {
		t.Fatalf("RulesFromAuthorization = %+v, %v", got, downscoped)
	}
	if got[0].AvailabilityCondition == nil || *got[0].AvailabilityCondition != wantCondition {
		t.Fatalf("round-tripped condition = %+v, want %+v", got[0].AvailabilityCondition, wantCondition)
	}
	raw, err := json.Marshal(got[0])
	if err != nil {
		t.Fatalf("marshal rule: %v", err)
	}
	var claim map[string]any
	if err := json.Unmarshal(raw, &claim); err != nil {
		t.Fatalf("unmarshal rule: %v", err)
	}
	for _, key := range []string{"availableResource", "availablePermissions", "availabilityCondition"} {
		if _, ok := claim[key]; !ok {
			t.Errorf("claim rule is missing the documented %q field: %s", key, raw)
		}
	}
	condition, ok := claim["availabilityCondition"].(map[string]any)
	if !ok {
		t.Fatalf("availabilityCondition is not an object: %s", raw)
	}
	for _, key := range []string{"expression", "title", "description"} {
		if _, ok := condition[key]; !ok {
			t.Errorf("availabilityCondition is missing %q: %s", key, raw)
		}
	}

	if err := Allowed(auth, ReadObject, "example-bucket", "customer-a/invoices/2024-01.pdf"); err != nil {
		t.Errorf("read inside the condition: %v", err)
	}
	if err := Allowed(auth, ReadObject, "example-bucket", "customer-b/2024-01.pdf"); !errors.Is(err, ErrDenied) {
		t.Errorf("read outside the condition = %v, want ErrDenied", err)
	}
}

// TestParseOptionsFolderCondition covers the documented hierarchical-namespace
// folder form, which names a folder in resource.name and maps onto the object
// prefix the GCS provider enforces.
func TestParseOptionsFolderCondition(t *testing.T) {
	expression := "resource.name.startsWith('projects/_/buckets/example-bucket/objects/example-folder')" +
		" || resource.name.startsWith('projects/_/buckets/example-bucket/folders/example-folder')" +
		" || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('example-folder/')"
	rules, err := ParseOptions(option("example-bucket", expression, PermissionObjectViewer))
	if err != nil {
		t.Fatalf("ParseOptions(folder condition): %v", err)
	}
	if rules[0].ObjectPrefix != "example-folder/" {
		t.Errorf("objectPrefix = %q, want example-folder/", rules[0].ObjectPrefix)
	}
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
	return "hdr." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestAllowedNonDownscopedIsUnrestricted(t *testing.T) {
	for _, auth := range []string{"", "Bearer some-access-token", "Bearer opaque-jwt-like.token.value"} {
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

// TestAllowedTokenWithoutBoundaryIsUnrestricted asserts that an opaque token
// whose payload has no access_boundary claim (an ordinary access token, a
// non-JWT opaque token, or a malformed JWT) is not treated as downscoped.
func TestAllowedTokenWithoutBoundaryIsUnrestricted(t *testing.T) {
	noClaim, _ := json.Marshal(map[string]any{"email": "sa@example.com"})
	for _, token := range []string{
		"garbage",
		"a.b", // payload not valid base64
		"a." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".c",
		"hdr." + base64.RawURLEncoding.EncodeToString(noClaim) + ".sig", // valid payload, no access_boundary
	} {
		if err := Allowed("Bearer "+token, ReadObject, "bkt", "o"); err != nil {
			t.Errorf("Allowed(%q) = %v, want unrestricted (nil)", token, err)
		}
	}
}

// TestAllowedEmptyBoundaryDenies asserts that a downscoped token whose boundary
// claim is present but empty is enforced and denies every operation.
func TestAllowedEmptyBoundaryDenies(t *testing.T) {
	auth := "Bearer " + token(t, []Rule{})
	if err := Allowed(auth, ReadObject, "bkt", "o"); !errors.Is(err, ErrDenied) {
		t.Errorf("Allowed(empty boundary) = %v, want ErrDenied", err)
	}
	if _, downscoped := RulesFromAuthorization(auth); !downscoped {
		t.Error("empty boundary claim must still be reported as downscoped")
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
	auth := "Bearer hdr." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
	if err := Allowed(auth, ReadObject, "bkt", "o"); err != nil {
		t.Fatalf("expected identity-carrying downscoped token to allow read: %v", err)
	}
}
