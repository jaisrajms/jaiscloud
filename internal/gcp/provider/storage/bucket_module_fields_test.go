package storage

import (
	"context"
	"encoding/json"
	"testing"
)

// The six bucket fields this file covers are the ones
// terraform-google-cloud-storage emits on every bucket (autoclass and
// hierarchicalNamespace, both with enabled=false) or when the caller
// configures them (website, logging, customPlacementConfig, ipFilter). The
// emulator used to drop them on create and GET, which is what made the module's
// `plan` a permadiff (or, for the two immutable blocks, a replacement).

// bucketAutoclass returns the response's autoclass object with its enabled flag
// split out, so a test can distinguish "absent" from "explicitly false".
func bucketAutoclass(t *testing.T, data map[string]any) (present, enabled bool, terminal string) {
	t.Helper()
	ac, ok := data["autoclass"].(map[string]any)
	if !ok {
		return false, false, ""
	}
	enabled, _ = ac["enabled"].(bool)
	terminal, _ = ac["terminalStorageClass"].(string)
	return true, enabled, terminal
}

// assertJSONEqual compares two JSON-shaped values structurally (encoding/json
// sorts map keys, so the comparison is stable).
func assertJSONEqual(t *testing.T, label string, got, want any) {
	t.Helper()
	gb, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("%s: marshal got: %v", label, err)
	}
	wb, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("%s: marshal want: %v", label, err)
	}
	if string(gb) != string(wb) {
		t.Errorf("%s = %s, want %s", label, gb, wb)
	}
}

// TestBucketInsertRoundTripsModuleFields covers create -> read-back of all six
// fields, including the explicit `autoclass { enabled = false }` the module
// always sends (a false bool must survive, not be treated as "absent").
func TestBucketInsertRoundTripsModuleFields(t *testing.T) {
	p := newTestProvider()
	body := map[string]any{
		"name": "module-fields",
		"autoclass": map[string]any{
			"enabled":              false,
			"terminalStorageClass": "ARCHIVE",
		},
		"hierarchicalNamespace": map[string]any{"enabled": false},
		"website": map[string]any{
			"mainPageSuffix": "index.html",
			"notFoundPage":   "404.html",
		},
		"logging": map[string]any{
			"logBucket":       "module-fields-logs",
			"logObjectPrefix": "access-logs/",
		},
		"ipFilter": map[string]any{
			"mode":                "Enabled",
			"allowCrossOrgVpcs":   true,
			"publicNetworkSource": map[string]any{"allowedIpCidrRanges": []any{"203.0.113.0/24"}},
			"vpcNetworkSources": []any{map[string]any{
				"network":             "projects/proj/global/networks/default",
				"allowedIpCidrRanges": []any{"10.0.0.0/8"},
			}},
			"allowAllServiceAgentAccess": true,
		},
	}
	inserted := insertBucket(t, p, body)

	present, enabled, terminal := bucketAutoclass(t, inserted)
	if !present || enabled || terminal != "ARCHIVE" {
		t.Errorf("create autoclass = present %v / enabled %v / terminal %q, want present/false/ARCHIVE",
			present, enabled, terminal)
	}
	assertJSONEqual(t, "create hierarchicalNamespace", inserted["hierarchicalNamespace"], map[string]any{"enabled": false})
	assertJSONEqual(t, "create website", inserted["website"], body["website"])
	assertJSONEqual(t, "create logging", inserted["logging"], body["logging"])
	assertJSONEqual(t, "create ipFilter", inserted["ipFilter"], body["ipFilter"])

	// The same values must come back from a fresh GET (the stored meta, not
	// the in-flight request object) and from the bucket list.
	for label, data := range map[string]map[string]any{
		"get":  getBucket(t, p, "module-fields"),
		"list": listBucketByName(t, p, "module-fields"),
	} {
		present, enabled, terminal = bucketAutoclass(t, data)
		if !present || enabled || terminal != "ARCHIVE" {
			t.Errorf("%s autoclass = present %v / enabled %v / terminal %q, want present/false/ARCHIVE",
				label, present, enabled, terminal)
		}
		assertJSONEqual(t, label+" hierarchicalNamespace", data["hierarchicalNamespace"], map[string]any{"enabled": false})
		assertJSONEqual(t, label+" website", data["website"], body["website"])
		assertJSONEqual(t, label+" logging", data["logging"], body["logging"])
		assertJSONEqual(t, label+" ipFilter", data["ipFilter"], body["ipFilter"])
	}
}

// listBucketByName returns a bucket's item from buckets.list.
func listBucketByName(t *testing.T, p *Provider, name string) map[string]any {
	t.Helper()
	nr := bucketParams()
	resp, err := p.BucketsList(context.Background(), nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	for _, item := range items {
		m, _ := item.(map[string]any)
		if m["name"] == name {
			return m
		}
	}
	t.Fatalf("bucket %q not in list response", name)
	return nil
}

// TestBucketInsertRoundTripsImmutableBlocks covers the two create-only blocks
// the provider marks ForceNew: hierarchical namespace enabled=true and custom
// dual-region placement. Their read-back is what stops the module planning a
// replacement on the next run.
func TestBucketInsertRoundTripsImmutableBlocks(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{
		"name":                  "hns-bucket",
		"location":              "NAM4",
		"hierarchicalNamespace": map[string]any{"enabled": true},
		"customPlacementConfig": map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}},
	})

	data := getBucket(t, p, "hns-bucket")
	assertJSONEqual(t, "hierarchicalNamespace", data["hierarchicalNamespace"], map[string]any{"enabled": true})
	assertJSONEqual(t, "customPlacementConfig", data["customPlacementConfig"],
		map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}})

	// getStorageLayout must reflect the stored flag rather than hard-coding
	// false (gcloud/SDK uploads read it before streaming).
	nr := bucketParams()
	nr.Params["bucket"] = "hns-bucket"
	layout, err := p.BucketsGetStorageLayout(context.Background(), nr)
	if err != nil {
		t.Fatalf("getStorageLayout: %v", err)
	}
	assertJSONEqual(t, "storageLayout.hierarchicalNamespace", layout.Data["hierarchicalNamespace"],
		map[string]any{"enabled": true})
}

// TestBucketsUpdateModuleFields covers the provider's PATCH path: autoclass,
// website, logging and ipFilter are replaced when the body carries them, and
// the two immutable blocks are left untouched.
func TestBucketsUpdateModuleFields(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{
		"name":                  "module-update",
		"location":              "NAM4",
		"autoclass":             map[string]any{"enabled": false},
		"hierarchicalNamespace": map[string]any{"enabled": true},
		"customPlacementConfig": map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}},
	})

	// Flip autoclass on and set the optional blocks.
	patched := patchBucket(t, p, "module-update", map[string]any{
		"autoclass": map[string]any{"enabled": true, "terminalStorageClass": "NEARLINE"},
		"website":   map[string]any{"mainPageSuffix": "index.html", "notFoundPage": "404.html"},
		"logging":   map[string]any{"logBucket": "module-update-logs", "logObjectPrefix": "logs/"},
		"ipFilter":  map[string]any{"mode": "Enabled", "allowAllServiceAgentAccess": true},
	})
	present, enabled, terminal := bucketAutoclass(t, patched)
	if !present || !enabled || terminal != "NEARLINE" {
		t.Fatalf("patched autoclass = present %v / enabled %v / terminal %q, want present/true/NEARLINE",
			present, enabled, terminal)
	}
	assertJSONEqual(t, "patched website", patched["website"],
		map[string]any{"mainPageSuffix": "index.html", "notFoundPage": "404.html"})
	assertJSONEqual(t, "patched logging", patched["logging"],
		map[string]any{"logBucket": "module-update-logs", "logObjectPrefix": "logs/"})
	assertJSONEqual(t, "patched ipFilter", patched["ipFilter"],
		map[string]any{"mode": "Enabled", "allowAllServiceAgentAccess": true})
	// An update never rewrites the create-only blocks.
	assertJSONEqual(t, "patched hierarchicalNamespace", patched["hierarchicalNamespace"], map[string]any{"enabled": true})
	assertJSONEqual(t, "patched customPlacementConfig", patched["customPlacementConfig"],
		map[string]any{"dataLocations": []any{"US-CENTRAL1", "US-EAST1"}})

	// Clear-back: the provider sends autoclass{enabled:false}, an explicit JSON
	// null for website/logging (its NullFields path), and ipFilter{mode:
	// Disabled} — the shape its DiffSuppressFunc treats as removed.
	cleared := patchBucket(t, p, "module-update", map[string]any{
		"autoclass": map[string]any{"enabled": false},
		"website":   nil,
		"logging":   nil,
		"ipFilter":  map[string]any{"mode": "Disabled"},
	})
	if present, enabled, _ = bucketAutoclass(t, cleared); !present || enabled {
		t.Errorf("cleared autoclass = present %v / enabled %v, want present/false", present, enabled)
	}
	if _, ok := cleared["website"]; ok {
		t.Errorf("website = %#v after clearing, want absent", cleared["website"])
	}
	if _, ok := cleared["logging"]; ok {
		t.Errorf("logging = %#v after clearing, want absent", cleared["logging"])
	}
	assertJSONEqual(t, "cleared ipFilter", cleared["ipFilter"], map[string]any{"mode": "Disabled"})
	assertJSONEqual(t, "cleared hierarchicalNamespace", cleared["hierarchicalNamespace"], map[string]any{"enabled": true})

	// A PATCH that carries none of the four leaves all of them alone (body-driven,
	// like every other bucket field).
	untouched := patchBucket(t, p, "module-update", map[string]any{"labels": map[string]any{"env": "test"}})
	assertJSONEqual(t, "partial patch ipFilter", untouched["ipFilter"], map[string]any{"mode": "Disabled"})
}

// TestBucketInsertFabricatesNoModuleFields pins the "store what was sent"
// rule: a bucket created without these blocks must not read back synthesized
// defaults for them.
func TestBucketInsertFabricatesNoModuleFields(t *testing.T) {
	p := newTestProvider()
	insertBucket(t, p, map[string]any{"name": "plain-module"})

	data := getBucket(t, p, "plain-module")
	for _, key := range []string{"autoclass", "hierarchicalNamespace", "website", "logging", "customPlacementConfig", "ipFilter"} {
		if v, ok := data[key]; ok {
			t.Errorf("GET reported %s = %#v for a bucket created without it", key, v)
		}
	}

	// getStorageLayout still answers the SDK read, with HNS disabled.
	nr := bucketParams()
	nr.Params["bucket"] = "plain-module"
	layout, err := p.BucketsGetStorageLayout(context.Background(), nr)
	if err != nil {
		t.Fatalf("getStorageLayout: %v", err)
	}
	assertJSONEqual(t, "storageLayout.hierarchicalNamespace", layout.Data["hierarchicalNamespace"],
		map[string]any{"enabled": false})
}
