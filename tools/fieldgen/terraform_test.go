//go:build gcp_conformance

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadProviderSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.json")
	const doc = `{
	  "format_version": "1.0",
	  "provider_schemas": {
	    "registry.terraform.io/hashicorp/google": {
	      "resource_schemas": {
	        "google_storage_bucket": {
	          "version": 2,
	          "block": {
	            "attributes": {"name": {}, "location": {}},
	            "block_types": {
	              "versioning": {
	                "nesting_mode": "list",
	                "block": {"attributes": {"enabled": {}}}
	              }
	            }
	          }
	        }
	      }
	    }
	  }
	}`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	schema, err := LoadProviderSchema(path)
	if err != nil {
		t.Fatalf("LoadProviderSchema: %v", err)
	}
	if schema.ResourceCount() != 1 {
		t.Fatalf("ResourceCount = %d, want 1", schema.ResourceCount())
	}
	got := schema.Attributes("google_storage_bucket")
	want := []string{"location", "name", "versioning.enabled"}
	if len(got) != len(want) {
		t.Fatalf("Attributes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Attributes = %v, want %v", got, want)
		}
	}
	if _, err := LoadProviderSchema(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("LoadProviderSchema(missing) = nil error")
	}
}
