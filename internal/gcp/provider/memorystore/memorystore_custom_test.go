package memorystore

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// errStatus returns a ProviderError's HTTP status, or 0.
func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

// instanceNR builds a request for an instance-scoped custom method.
func instanceNR(location, id, verb string, body map[string]any) *model.NormalizedRequest {
	name := "locations/" + location + "/instances/" + id
	if verb != "" {
		name += ":" + verb
	}
	p := map[string]any{"location": location, "instanceId": id, "name": name}
	if body != nil {
		p["body"] = body
	}
	return newNR(p)
}

func createInstance(t *testing.T, p *Provider, id, tier string) {
	t.Helper()
	body := instanceBody("primary")
	if tier != "" {
		body["tier"] = tier
	}
	_, err := p.CreateInstance(context.Background(), newNR(map[string]any{
		"location": "us-central1", "instanceId": id, "body": body,
	}))
	if err != nil {
		t.Fatalf("CreateInstance(%s): %v", id, err)
	}
}

func TestGetAuthString(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	createInstance(t, p, "r1", "BASIC")

	// The emulator runs no Redis and never enables AUTH: real Memorystore
	// documents an empty response in that case.
	resp, err := p.GetAuthString(ctx, instanceNR("us-central1", "r1", "authString", nil))
	if err != nil {
		t.Fatalf("GetAuthString: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("GetAuthString data = %v, want empty", resp.Data)
	}

	// A missing instance is NotFound — the handler must not fall back to
	// getInstance.
	if _, err := p.GetAuthString(ctx, instanceNR("us-central1", "missing", "authString", nil)); err == nil || errStatus(err) != 404 {
		t.Fatalf("GetAuthString(missing): err = %v, want 404", err)
	}
}

func TestFailoverTierPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	createInstance(t, p, "basic", "BASIC")
	createInstance(t, p, "ha", "STANDARD_HA")

	if _, err := p.FailoverInstance(ctx, instanceNR("us-central1", "basic", "failover", nil)); err == nil || errStatus(err) != 400 {
		t.Fatalf("failover on BASIC: err = %v, want 400", err)
	}
	resp, err := p.FailoverInstance(ctx, instanceNR("us-central1", "ha", "failover", nil))
	if err != nil {
		t.Fatalf("failover on STANDARD_HA: %v", err)
	}
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("failover op not done: %v", resp.Data)
	}
}

func TestImportExportRequireConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	createInstance(t, p, "r1", "BASIC")

	for _, tc := range []struct {
		name string
		fn   func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
		key  string
		sub  string
	}{
		{"import", p.ImportInstance, "inputConfig", "gcsSource"},
		{"export", p.ExportInstance, "outputConfig", "gcsDestination"},
	} {
		if _, err := tc.fn(ctx, instanceNR("us-central1", "r1", tc.name, map[string]any{})); err == nil || errStatus(err) != 400 {
			t.Errorf("%s without config: err = %v, want 400", tc.name, err)
		}
		body := map[string]any{tc.key: map[string]any{tc.sub: map[string]any{"uri": "gs://bucket/obj.rdb"}}}
		resp, err := tc.fn(ctx, instanceNR("us-central1", "r1", tc.name, body))
		if err != nil {
			t.Fatalf("%s with config: %v", tc.name, err)
		}
		if done, _ := resp.Data["done"].(bool); !done {
			t.Errorf("%s op not done: %v", tc.name, resp.Data)
		}
	}
}

func TestRescheduleMaintenanceValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	createInstance(t, p, "r1", "BASIC")

	if _, err := p.RescheduleMaintenance(ctx, instanceNR("us-central1", "r1", "rescheduleMaintenance", map[string]any{})); err == nil || errStatus(err) != 400 {
		t.Fatalf("no rescheduleType: err = %v, want 400", err)
	}
	if _, err := p.RescheduleMaintenance(ctx, instanceNR("us-central1", "r1", "rescheduleMaintenance",
		map[string]any{"rescheduleType": "SPECIFIC_TIME"})); err == nil || errStatus(err) != 400 {
		t.Fatalf("SPECIFIC_TIME without scheduleTime: err = %v, want 400", err)
	}
	resp, err := p.RescheduleMaintenance(ctx, instanceNR("us-central1", "r1", "rescheduleMaintenance",
		map[string]any{"rescheduleType": "IMMEDIATE"}))
	if err != nil {
		t.Fatalf("IMMEDIATE: %v", err)
	}
	if done, _ := resp.Data["done"].(bool); !done {
		t.Fatalf("reschedule op not done: %v", resp.Data)
	}
}
