package memorystore

import (
	"context"
	"testing"
)

// These tests cover the redis.v1 instance fields the official
// terraform-google-memorystore module sends (TGM2): create/get round-trip for
// every stored field, the provider's update-mask paths (including the
// mask-authoritative clearing the provider relies on when it disables AUTH or
// removes a block), and the enum/shape validation the Discovery document
// declares.

func getInstance(t *testing.T, p *Provider, id string) map[string]any {
	t.Helper()
	resp, err := p.GetInstance(context.Background(), newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": id,
	}))
	if err != nil {
		t.Fatalf("get instance %s: %v", id, err)
	}
	return resp.Data
}

func updateInstance(t *testing.T, p *Provider, id, mask string, body map[string]any) map[string]any {
	t.Helper()
	op, err := p.UpdateInstance(context.Background(), newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": id,
		"updateMask": mask,
		"body":       body,
	}))
	if err != nil {
		t.Fatalf("update %s (mask %q): %v", id, mask, err)
	}
	return instanceResponse(t, op.Data)
}

func TestInstanceModuleFieldCreateRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op, err := p.CreateInstance(ctx, newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": "inst-module",
		"body": map[string]any{
			"displayName":           "module instance",
			"tier":                  "STANDARD_HA",
			"memorySizeGb":          5,
			"redisVersion":          "REDIS_7_0",
			"alternativeLocationId": "us-central1-b",
			"authorizedNetwork":     "projects/proj/global/networks/default",
			"reservedIpRange":       "10.0.0.0/29",
			"secondaryIpRange":      "10.0.1.0/28",
			"connectMode":           "DIRECT_PEERING",
			"transitEncryptionMode": "SERVER_AUTHENTICATION",
			"readReplicasMode":      "READ_REPLICAS_DISABLED",
			"replicaCount":          1,
			"customerManagedKey":    "projects/proj/locations/us-central1/keyRings/kr/cryptoKeys/key",
			"authEnabled":           true,
			"redisConfigs":          map[string]any{"maxmemory-policy": "noeviction"},
			"labels":                map[string]any{"env": "module"},
			"maintenancePolicy": map[string]any{
				"description": "gate window",
				"weeklyMaintenanceWindow": []any{
					map[string]any{
						"day":       "MONDAY",
						"startTime": map[string]any{"hours": 1, "minutes": 30},
					},
				},
			},
			"persistenceConfig": map[string]any{
				"persistenceMode":   "RDB",
				"rdbSnapshotPeriod": "SIX_HOURS",
			},
		},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	created := instanceResponse(t, op.Data)
	if created["authEnabled"] != true {
		t.Errorf("create response authEnabled = %v, want true", created["authEnabled"])
	}

	inst := getInstance(t, p, "inst-module")
	for key, want := range map[string]any{
		"displayName":           "module instance",
		"tier":                  "STANDARD_HA",
		"memorySizeGb":          float64(5),
		"redisVersion":          "REDIS_7_0",
		"alternativeLocationId": "us-central1-b",
		"authorizedNetwork":     "projects/proj/global/networks/default",
		"reservedIpRange":       "10.0.0.0/29",
		"secondaryIpRange":      "10.0.1.0/28",
		"connectMode":           "DIRECT_PEERING",
		"transitEncryptionMode": "SERVER_AUTHENTICATION",
		"readReplicasMode":      "READ_REPLICAS_DISABLED",
		"replicaCount":          float64(1),
		"customerManagedKey":    "projects/proj/locations/us-central1/keyRings/kr/cryptoKeys/key",
		"authEnabled":           true,
	} {
		if got := inst[key]; got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	if got := inst["labels"].(map[string]any)["env"]; got != "module" {
		t.Errorf("labels.env = %v", got)
	}
	if got := inst["redisConfigs"].(map[string]any)["maxmemory-policy"]; got != "noeviction" {
		t.Errorf("redisConfigs[maxmemory-policy] = %v", got)
	}

	mp, ok := inst["maintenancePolicy"].(map[string]any)
	if !ok {
		t.Fatalf("maintenancePolicy = %v, want an object", inst["maintenancePolicy"])
	}
	if mp["description"] != "gate window" {
		t.Errorf("maintenancePolicy.description = %v", mp["description"])
	}
	windows, ok := mp["weeklyMaintenanceWindow"].([]any)
	if !ok || len(windows) != 1 {
		t.Fatalf("weeklyMaintenanceWindow = %v, want one window", mp["weeklyMaintenanceWindow"])
	}
	window := windows[0].(map[string]any)
	if window["day"] != "MONDAY" {
		t.Errorf("weeklyMaintenanceWindow.day = %v", window["day"])
	}
	start := window["startTime"].(map[string]any)
	if start["hours"] != float64(1) || start["minutes"] != float64(30) {
		t.Errorf("weeklyMaintenanceWindow.startTime = %v", start)
	}

	pc, ok := inst["persistenceConfig"].(map[string]any)
	if !ok {
		t.Fatalf("persistenceConfig = %v, want an object", inst["persistenceConfig"])
	}
	if pc["persistenceMode"] != "RDB" || pc["rdbSnapshotPeriod"] != "SIX_HOURS" {
		t.Errorf("persistenceConfig = %v", pc)
	}
}

// TestInstanceAuthDisabledIsRendered pins the documented default: the provider
// never sends authEnabled=false (its create/update guards drop a false bool),
// and its read treats an absent value as false, so the emulator renders the
// documented default instead of omitting the field.
func TestInstanceAuthDisabledIsRendered(t *testing.T) {
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-auth")

	inst := getInstance(t, p, "inst-auth")
	got, ok := inst["authEnabled"]
	if !ok {
		t.Fatalf("authEnabled is omitted from GET; want the documented false default")
	}
	if got != false {
		t.Errorf("authEnabled = %v, want false", got)
	}
}

func TestInstanceModuleFieldValidation(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	cases := []struct {
		name string
		body map[string]any
	}{
		{"connectMode", map[string]any{"tier": "STANDARD_HA", "connectMode": "PEERING"}},
		{"transitEncryptionMode", map[string]any{"tier": "STANDARD_HA", "transitEncryptionMode": "TLS"}},
		{"readReplicasMode", map[string]any{"tier": "STANDARD_HA", "readReplicasMode": "ENABLED"}},
		{"readReplicasOnBasic", map[string]any{"tier": "BASIC", "readReplicasMode": "READ_REPLICAS_ENABLED"}},
		{"persistenceMode", map[string]any{
			"tier":              "STANDARD_HA",
			"persistenceConfig": map[string]any{"persistenceMode": "AOF"},
		}},
		{"snapshotPeriod", map[string]any{
			"tier":              "STANDARD_HA",
			"persistenceConfig": map[string]any{"persistenceMode": "RDB", "rdbSnapshotPeriod": "TWO_HOURS"},
		}},
		{"maintenanceDay", map[string]any{
			"tier": "STANDARD_HA",
			"maintenancePolicy": map[string]any{
				"weeklyMaintenanceWindow": []any{
					map[string]any{"day": "FUNDAY", "startTime": map[string]any{"hours": 1}},
				},
			},
		}},
		{"maintenanceStartTime", map[string]any{
			"tier": "STANDARD_HA",
			"maintenancePolicy": map[string]any{
				"weeklyMaintenanceWindow": []any{
					map[string]any{"day": "MONDAY", "startTime": map[string]any{"hours": 24}},
				},
			},
		}},
		{"maintenanceMissingStartTime", map[string]any{
			"tier": "STANDARD_HA",
			"maintenancePolicy": map[string]any{
				"weeklyMaintenanceWindow": []any{map[string]any{"day": "MONDAY"}},
			},
		}},
	}
	for i, tc := range cases {
		id := "inst-invalid-" + string(rune('a'+i))
		_, err := p.CreateInstance(ctx, newNR(map[string]any{
			"location":   "us-central1",
			"instanceId": id,
			"body":       tc.body,
		}))
		if !isCode(err, "InvalidArgument") {
			t.Errorf("%s: expected InvalidArgument, got %v", tc.name, err)
		}
	}
}

func TestInstanceModuleMaskUpdates(t *testing.T) {
	p := newProvider()
	mustCreateWithBody(t, p, "inst-mask", map[string]any{
		"tier":                  "STANDARD_HA",
		"memorySizeGb":          1,
		"connectMode":           "DIRECT_PEERING",
		"transitEncryptionMode": "DISABLED",
		"readReplicasMode":      "READ_REPLICAS_DISABLED",
		"authEnabled":           false,
	})

	// AUTH false -> true, then true -> false with the field absent from the
	// body (what hashicorp/google 7.46.2 sends): the mask is authoritative.
	updateInstance(t, p, "inst-mask", "authEnabled", map[string]any{"authEnabled": true})
	if got := getInstance(t, p, "inst-mask")["authEnabled"]; got != true {
		t.Fatalf("authEnabled after enable = %v, want true", got)
	}
	updateInstance(t, p, "inst-mask", "authEnabled", map[string]any{})
	if got := getInstance(t, p, "inst-mask")["authEnabled"]; got != false {
		t.Errorf("authEnabled after disable-with-empty-body = %v, want false", got)
	}

	// readReplicasMode applies in place and is clearable.
	updateInstance(t, p, "inst-mask", "readReplicasMode", map[string]any{"readReplicasMode": "READ_REPLICAS_ENABLED"})
	if got := getInstance(t, p, "inst-mask")["readReplicasMode"]; got != "READ_REPLICAS_ENABLED" {
		t.Errorf("readReplicasMode = %v, want READ_REPLICAS_ENABLED", got)
	}
	updateInstance(t, p, "inst-mask", "readReplicasMode", map[string]any{})
	if got := getInstance(t, p, "inst-mask")["readReplicasMode"]; got != nil {
		t.Errorf("readReplicasMode after clearing = %v, want omitted", got)
	}

	// secondaryIpRange.
	updateInstance(t, p, "inst-mask", "secondaryIpRange", map[string]any{"secondaryIpRange": "10.0.2.0/28"})
	if got := getInstance(t, p, "inst-mask")["secondaryIpRange"]; got != "10.0.2.0/28" {
		t.Errorf("secondaryIpRange = %v", got)
	}

	// memorySizeGb and labels.
	updateInstance(t, p, "inst-mask", "memorySizeGb,labels", map[string]any{
		"memorySizeGb": 8,
		"labels":       map[string]any{"team": "core"},
	})
	inst := getInstance(t, p, "inst-mask")
	if inst["memorySizeGb"] != float64(8) {
		t.Errorf("memorySizeGb = %v, want 8", inst["memorySizeGb"])
	}
	if got := inst["labels"].(map[string]any)["team"]; got != "core" {
		t.Errorf("labels.team = %v", got)
	}
	updateInstance(t, p, "inst-mask", "labels", map[string]any{})
	if _, ok := getInstance(t, p, "inst-mask")["labels"]; ok {
		t.Errorf("labels still rendered after a masked clear: %v", getInstance(t, p, "inst-mask")["labels"])
	}

	// maintenancePolicy set, replaced, then cleared by an empty masked body.
	updateInstance(t, p, "inst-mask", "maintenancePolicy", map[string]any{
		"maintenancePolicy": map[string]any{
			"description": "one",
			"weeklyMaintenanceWindow": []any{
				map[string]any{"day": "MONDAY", "startTime": map[string]any{"hours": 1}},
			},
		},
	})
	if got := getInstance(t, p, "inst-mask")["maintenancePolicy"].(map[string]any)["description"]; got != "one" {
		t.Errorf("maintenancePolicy.description = %v, want one", got)
	}
	updateInstance(t, p, "inst-mask", "maintenancePolicy", map[string]any{
		"maintenancePolicy": map[string]any{
			"description": "two",
			"weeklyMaintenanceWindow": []any{
				map[string]any{"day": "WEDNESDAY", "startTime": map[string]any{"hours": 3}},
			},
		},
	})
	if got := getInstance(t, p, "inst-mask")["maintenancePolicy"].(map[string]any)["description"]; got != "two" {
		t.Errorf("maintenancePolicy.description = %v, want two", got)
	}
	updateInstance(t, p, "inst-mask", "maintenancePolicy", map[string]any{})
	if got, ok := getInstance(t, p, "inst-mask")["maintenancePolicy"]; ok {
		t.Errorf("maintenancePolicy after clearing = %v, want omitted", got)
	}

	// persistenceConfig set, then cleared.
	updateInstance(t, p, "inst-mask", "persistenceConfig", map[string]any{
		"persistenceConfig": map[string]any{
			"persistenceMode":   "RDB",
			"rdbSnapshotPeriod": "ONE_HOUR",
		},
	})
	pc := getInstance(t, p, "inst-mask")["persistenceConfig"].(map[string]any)
	if pc["rdbSnapshotPeriod"] != "ONE_HOUR" {
		t.Errorf("persistenceConfig = %v", pc)
	}
	updateInstance(t, p, "inst-mask", "persistenceConfig", map[string]any{})
	if got, ok := getInstance(t, p, "inst-mask")["persistenceConfig"]; ok {
		t.Errorf("persistenceConfig after clearing = %v, want omitted", got)
	}

	// maintenanceVersion is output-only: accepted, never stored.
	updateInstance(t, p, "inst-mask", "maintenanceVersion", map[string]any{"maintenanceVersion": "20210712_00_00"})
	if got, ok := getInstance(t, p, "inst-mask")["maintenanceVersion"]; ok {
		t.Errorf("maintenanceVersion = %v, want it to stay output-only", got)
	}

	// A masked update must not touch a create-only (ForceNew) field the body
	// happens to carry.
	updateInstance(t, p, "inst-mask", "displayName", map[string]any{
		"displayName":           "renamed",
		"connectMode":           "PRIVATE_SERVICE_ACCESS",
		"transitEncryptionMode": "SERVER_AUTHENTICATION",
	})
	inst = getInstance(t, p, "inst-mask")
	if inst["connectMode"] != "DIRECT_PEERING" {
		t.Errorf("connectMode = %v, want the create-time DIRECT_PEERING", inst["connectMode"])
	}
	if inst["transitEncryptionMode"] != "DISABLED" {
		t.Errorf("transitEncryptionMode = %v, want the create-time DISABLED", inst["transitEncryptionMode"])
	}
}

func TestInstanceUnknownModuleMaskStillRejected(t *testing.T) {
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-unknown")

	_, err := p.UpdateInstance(context.Background(), newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": "inst-unknown",
		"updateMask": "nodeType",
		"body":       map[string]any{},
	}))
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for an unknown mask path, got %v", err)
	}
}

func mustCreateWithBody(t *testing.T, p *Provider, id string, body map[string]any) {
	t.Helper()
	if _, err := p.CreateInstance(context.Background(), newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": id,
		"body":       body,
	})); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}
