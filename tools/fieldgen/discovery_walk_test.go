//go:build gcp_conformance

package main

import (
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

// testZoneDoc is a small Discovery fixture: scalar fields, a readOnly output
// field, a nested $ref, an array of $ref, an inline object and a self-reference
// that must be bounded by the depth cap.
func testZoneDoc() *conf.DiscoveryDoc {
	return &conf.DiscoveryDoc{
		Name:    "test",
		Version: "v1",
		Schemas: map[string]*conf.Schema{
			"Zone": {
				Properties: map[string]*conf.Schema{
					"name":         {Type: "string"},
					"description":  {Type: "string"},
					"kind":         {Type: "string", ReadOnly: true},
					"creationTime": {Type: "string", ReadOnly: true},
					"dnssecConfig": {Ref: "DNSSECConfig"},
					"networks":     {Type: "array", Items: &conf.Schema{Ref: "Network"}},
					"inline":       {Type: "object", Properties: map[string]*conf.Schema{"value": {Type: "string"}}},
					"child":        {Ref: "Zone"},
				},
			},
			"DNSSECConfig": {
				Properties: map[string]*conf.Schema{
					"state": {Type: "string"},
					"kind":  {Type: "string", ReadOnly: true},
				},
			},
			"Network": {
				Properties: map[string]*conf.Schema{
					"networkUrl": {Type: "string"},
				},
			},
		},
	}
}

func TestWalkDiscoveryFields(t *testing.T) {
	fields, err := WalkDiscovery("clouddns", "rest", "ManagedZone", testZoneDoc(), "Zone", 2)
	if err != nil {
		t.Fatalf("WalkDiscovery: %v", err)
	}
	got := map[string]string{}
	for _, f := range fields {
		got[f.Name] = f.Behavior
	}
	want := map[string]string{
		"name":                "",
		"description":         "",
		"kind":                "read_only",
		"creationTime":        "read_only",
		"dnssecConfig":        "",
		"dnssecConfig.state":  "",
		"dnssecConfig.kind":   "read_only",
		"networks":            "",
		"networks.networkUrl": "",
		"inline":              "",
		"inline.value":        "",
		"child":               "",
		"child.name":          "",
	}
	for name, behavior := range want {
		b, ok := got[name]
		if !ok {
			t.Errorf("missing field %q", name)
			continue
		}
		if b != behavior {
			t.Errorf("%s behavior = %q, want %q", name, b, behavior)
		}
	}
	for name := range got {
		// Depth 2 means the root's own properties plus one nested level.
		if name == "child.dnssecConfig.state" || name == "child.child.child" {
			t.Errorf("depth 2 walked past one nested level: %s", name)
		}
	}
}

func TestWalkDiscoveryUnknownSchema(t *testing.T) {
	if _, err := WalkDiscovery("clouddns", "rest", "ManagedZone", testZoneDoc(), "Nope", 2); err == nil {
		t.Fatal("WalkDiscovery(unknown schema) = nil error, want an error")
	}
}

func TestWalkDiscoveryDepthOne(t *testing.T) {
	fields, err := WalkDiscovery("clouddns", "rest", "ManagedZone", testZoneDoc(), "Zone", 1)
	if err != nil {
		t.Fatalf("WalkDiscovery: %v", err)
	}
	for _, f := range fields {
		if f.Name == "dnssecConfig.state" {
			t.Fatalf("depth 1 walked nested field %q", f.Name)
		}
	}
}
