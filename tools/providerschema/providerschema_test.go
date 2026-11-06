package providerschema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseKeepsAttributeFlags(t *testing.T) {
	const doc = `{
	  "format_version": "1.0",
	  "provider_schemas": {
	    "registry.terraform.io/hashicorp/google": {
	      "resource_schemas": {
	        "google_storage_bucket": {
	          "block": {
	            "attributes": {
	              "name":     {"required": true},
	              "location": {"optional": true},
	              "id":       {"computed": true}
	            },
	            "block_types": {
	              "versioning": {"block": {"attributes": {"enabled": {"optional": true}}}}
	            }
	          }
	        }
	      }
	    }
	  }
	}`
	schema, err := Parse([]byte(doc), "fixture")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := schema.Attributes("google_storage_bucket")
	want := []string{"id", "location", "name", "versioning.enabled"}
	if len(got) != len(want) {
		t.Fatalf("Attributes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Attributes = %v, want %v", got, want)
		}
	}
	flags := map[string]Attribute{}
	for _, a := range schema.AttributeSpecs("google_storage_bucket") {
		flags[a.Path] = a
	}
	if !flags["name"].Required || !flags["location"].Optional || !flags["id"].Computed {
		t.Fatalf("flags = %+v, want name required / location optional / id computed", flags)
	}
	if !flags["versioning.enabled"].Optional {
		t.Fatalf("nested attribute flags = %+v, want optional", flags["versioning.enabled"])
	}
}

func TestParseRejectsNonSchemaInput(t *testing.T) {
	for name, doc := range map[string]string{
		"empty":        `{}`,
		"no-resources": `{"format_version":"1.0","provider_schemas":{"p":{}}}`,
		"not-json":     `not json`,
	} {
		if _, err := Parse([]byte(doc), name); err == nil {
			t.Errorf("Parse(%s) = nil error, want failure", name)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("Load(missing) = nil error")
	}
}

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.json")
	const doc = `{"format_version":"1.0","provider_schemas":{"p":{"resource_schemas":{"google_pubsub_topic":{"block":{"attributes":{"name":{}}}}}}}}`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	schema, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if schema.ResourceCount() != 1 {
		t.Fatalf("ResourceCount = %d, want 1", schema.ResourceCount())
	}
}

func TestCompactKeepsOnlyRequestedResourcesAndFlags(t *testing.T) {
	const doc = `{"format_version":"1.0","provider_schemas":{"registry.terraform.io/hashicorp/google":{"resource_schemas":{
	  "google_storage_bucket":{"block":{"attributes":{"name":{"required":true},"id":{"computed":true}},"block_types":{"versioning":{"block":{"attributes":{"enabled":{"optional":true}}}}}}},
	  "google_pubsub_topic":{"block":{"attributes":{"name":{}}}}
	}}}}`
	out, err := Compact([]byte(doc), []string{"google_storage_bucket"})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	schema, err := Parse(out, "snapshot")
	if err != nil {
		t.Fatalf("Parse(compact): %v", err)
	}
	if schema.ResourceCount() != 1 {
		t.Fatalf("ResourceCount = %d, want only the requested resource", schema.ResourceCount())
	}
	flags := map[string]Attribute{}
	for _, a := range schema.AttributeSpecs("google_storage_bucket") {
		flags[a.Path] = a
	}
	if !flags["name"].Required || !flags["id"].Computed || !flags["versioning.enabled"].Optional {
		t.Fatalf("compacted flags lost: %+v", flags)
	}
}
