package clouddns

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// iamNR builds an IAM request for a zone, optionally with a body.
func iamNR(zone string, body map[string]any) *model.NormalizedRequest {
	params := map[string]any{"managedZone": zone}
	if body != nil {
		params["body"] = body
	}
	return newNR(params)
}

func getZonePolicy(t *testing.T, p *Provider, zone string) map[string]any {
	t.Helper()
	resp, err := p.GetManagedZoneIamPolicy(context.Background(), iamNR(zone, nil))
	if err != nil {
		t.Fatalf("get iam policy: %v", err)
	}
	return resp.Data
}

// TestManagedZoneIamPolicyRoundTrip covers the get/set/test trio on an existing
// zone: the empty default policy, the stored policy echoed back verbatim (the
// provider reads the etag into state, so a verbatim read is what makes a second
// plan converge), and the permission echo.
func TestManagedZoneIamPolicyRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	empty := getZonePolicy(t, p, "zone-a")
	if empty["etag"] != "ACAB" {
		t.Errorf("empty policy etag = %v, want ACAB", empty["etag"])
	}
	if got, _ := empty["bindings"].([]any); len(got) != 0 {
		t.Errorf("empty policy bindings = %v, want []", empty["bindings"])
	}

	resp, err := p.SetManagedZoneIamPolicy(ctx, iamNR("zone-a", map[string]any{
		"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    "roles/dns.admin",
				"members": []any{"user:tf@example.com"},
			}},
		},
	}))
	if err != nil {
		t.Fatalf("set iam policy: %v", err)
	}
	stored, _ := resp.Data["bindings"].([]any)
	if len(stored) != 1 {
		t.Fatalf("stored bindings = %v, want 1", resp.Data["bindings"])
	}
	etag, _ := resp.Data["etag"].(string)
	if etag == "" || etag == "ACAB" {
		t.Errorf("stored etag = %q, want a derived etag", etag)
	}

	// A plain get (no options) returns the stored policy unchanged.
	got := getZonePolicy(t, p, "zone-a")
	if got["etag"] != etag {
		t.Errorf("get etag = %v, want %v", got["etag"], etag)
	}
	gotBindings, _ := got["bindings"].([]any)
	if len(gotBindings) != 1 {
		t.Fatalf("get bindings = %v, want the stored binding", got["bindings"])
	}
	binding, _ := gotBindings[0].(map[string]any)
	if binding["role"] != "roles/dns.admin" {
		t.Errorf("get role = %v, want roles/dns.admin", binding["role"])
	}

	resp, err = p.TestManagedZoneIamPermissions(ctx, iamNR("zone-a", map[string]any{
		"permissions": []any{"dns.managedZones.get", "dns.managedZones.update"},
	}))
	if err != nil {
		t.Fatalf("test iam permissions: %v", err)
	}
	perms, _ := resp.Data["permissions"].([]string)
	if len(perms) != 2 {
		t.Fatalf("permissions = %v, want both requested", resp.Data["permissions"])
	}
}

// TestManagedZoneIamPolicyEtagOCC pins the shared policy plane's concurrency
// control: a stale etag is ABORTED/409 and does not overwrite the stored policy.
func TestManagedZoneIamPolicyEtagOCC(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")

	if _, err := p.SetManagedZoneIamPolicy(ctx, iamNR("zone-a", map[string]any{
		"policy": map[string]any{"bindings": []any{map[string]any{
			"role": "roles/dns.viewer", "members": []any{"user:first@example.com"},
		}}},
	})); err != nil {
		t.Fatalf("first set: %v", err)
	}

	_, err := p.SetManagedZoneIamPolicy(ctx, iamNR("zone-a", map[string]any{
		"policy": map[string]any{
			"etag":     "stale",
			"bindings": []any{map[string]any{"role": "roles/viewer", "members": []any{"user:second@example.com"}}},
		},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Status != "ABORTED" || pe.HTTPStatus != 409 {
		t.Fatalf("stale etag = %v, want ABORTED/409", err)
	}

	got := getZonePolicy(t, p, "zone-a")
	bindings, _ := got["bindings"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("bindings after failed write = %v, want the first binding only", got["bindings"])
	}
	if b, _ := bindings[0].(map[string]any); b["role"] != "roles/dns.viewer" {
		t.Errorf("role after failed write = %v, want roles/dns.viewer", b["role"])
	}
}

// TestManagedZoneIamMissingZone pins the existence check: an absent zone is
// NotFound on every verb and never creates a policy row that a later zone of
// the same name would inherit.
func TestManagedZoneIamMissingZone(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	if _, err := p.GetManagedZoneIamPolicy(ctx, iamNR("nope", nil)); !isCode(err, "NotFound") {
		t.Errorf("get on missing zone = %v, want NotFound", err)
	}
	if _, err := p.SetManagedZoneIamPolicy(ctx, iamNR("nope", map[string]any{
		"policy": map[string]any{"bindings": []any{}},
	})); !isCode(err, "NotFound") {
		t.Errorf("set on missing zone = %v, want NotFound", err)
	}
	if _, err := p.TestManagedZoneIamPermissions(ctx, iamNR("nope", map[string]any{
		"permissions": []any{"dns.managedZones.get"},
	})); !isCode(err, "NotFound") {
		t.Errorf("test on missing zone = %v, want NotFound", err)
	}

	// The failed writes left no policy behind: creating the zone now serves the
	// empty default, not a resurrected binding.
	mustCreateZone(t, p, "nope")
	if got := getZonePolicy(t, p, "nope"); got["etag"] != "ACAB" {
		t.Errorf("re-created zone policy etag = %v, want ACAB", got["etag"])
	}

	// A request without a zone name is INVALID_ARGUMENT, not a policy read.
	if _, err := p.GetManagedZoneIamPolicy(ctx, iamNR("", nil)); !isCode(err, "InvalidArgument") {
		t.Errorf("get without zone = %v, want InvalidArgument", err)
	}
}

// TestManagedZoneIamPolicyIsolation covers the store keying: two zones keep
// separate policies, and deleting a zone drops its policy so a zone re-created
// under the same name starts from the empty default (real Cloud DNS binds the
// policy to the resource's lifetime).
func TestManagedZoneIamPolicyIsolation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreateZone(t, p, "zone-a")
	mustCreateZone(t, p, "zone-b")

	setPolicy := func(zone, role string) {
		t.Helper()
		if _, err := p.SetManagedZoneIamPolicy(ctx, iamNR(zone, map[string]any{
			"policy": map[string]any{"bindings": []any{map[string]any{
				"role": role, "members": []any{"user:iam@example.com"},
			}}},
		})); err != nil {
			t.Fatalf("set %s: %v", zone, err)
		}
	}
	setPolicy("zone-a", "roles/dns.admin")
	setPolicy("zone-b", "roles/dns.viewer")

	a, _ := getZonePolicy(t, p, "zone-a")["bindings"].([]any)
	b, _ := getZonePolicy(t, p, "zone-b")["bindings"].([]any)
	if roleOf(a) != "roles/dns.admin" || roleOf(b) != "roles/dns.viewer" {
		t.Fatalf("policies crossed zones: a=%v b=%v", a, b)
	}

	if _, err := p.DeleteManagedZone(ctx, newNR(map[string]any{"managedZone": "zone-a"})); err != nil {
		t.Fatalf("delete zone: %v", err)
	}
	mustCreateZone(t, p, "zone-a")
	if got := getZonePolicy(t, p, "zone-a"); got["etag"] != "ACAB" {
		t.Errorf("re-created zone inherited a policy: etag = %v", got["etag"])
	}
	// The sibling zone keeps its policy.
	if b, _ := getZonePolicy(t, p, "zone-b")["bindings"].([]any); len(b) != 1 {
		t.Errorf("zone-b policy lost: %v", b)
	}
}

func roleOf(bindings []any) string {
	if len(bindings) == 0 {
		return ""
	}
	b, _ := bindings[0].(map[string]any)
	role, _ := b["role"].(string)
	return role
}
