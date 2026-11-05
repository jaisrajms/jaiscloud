//go:build gcp_conformance

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// FC6 hook — Terraform provider-schema input.
//
// The third authoritative source for field coverage is the Terraform provider's
// own schema (`terraform providers schema -json`): a provider attribute the
// module sends but the emulator does not serve is the TGM15/TGM1-3 class seen
// from the client side. FC6 wires the module gates to capture outgoing request
// bodies and joins the provider schema to docs/fidelity/field-coverage.csv,
// failing on a sent-but-unsupported attribute.
//
// FC1/FC2 only need the input format to be stable, so this file parses the
// schema and exposes the attributes per resource type. It deliberately does NOT
// join them to the inventory yet: the provider-resource -> GCP-resource mapping
// and the "client-side (never sent)" classification are FC6 deliverables, and
// this PR is behaviour-neutral.
//
// Expected shape (abridged):
//
//	{
//	  "format_version": "1.0",
//	  "provider_schemas": {
//	    "registry.terraform.io/hashicorp/google": {
//	      "resource_schemas": {
//	        "google_storage_bucket": {
//	          "block": {"attributes": {"name": {...}, "location": {...}},
//	                    "block_types": {"lifecycle_rule": {...}}}
//	        }
//	      }
//	    }
//	  }
//	}

// ProviderSchema is a loaded Terraform provider schema, reduced to the parts
// FC6 needs: resource type -> attribute names (nested block attributes are
// prefixed with the block path, e.g. "versioning.enabled").
type ProviderSchema struct {
	FormatVersion string
	Resources     map[string][]string
}

// ResourceCount is the number of provider resource types in the schema.
func (p *ProviderSchema) ResourceCount() int { return len(p.Resources) }

// Attributes returns the sorted attribute paths declared for a provider
// resource type.
func (p *ProviderSchema) Attributes(resourceType string) []string {
	return p.Resources[resourceType]
}

type providerSchemaFile struct {
	FormatVersion   string `json:"format_version"`
	ProviderSchemas map[string]struct {
		ResourceSchemas map[string]struct {
			Block providerBlock `json:"block"`
		} `json:"resource_schemas"`
	} `json:"provider_schemas"`
}

type providerBlock struct {
	Attributes map[string]json.RawMessage `json:"attributes"`
	BlockTypes map[string]struct {
		Block providerBlock `json:"block"`
	} `json:"block_types"`
}

// LoadProviderSchema reads a `terraform providers schema -json` dump.
func LoadProviderSchema(path string) (*ProviderSchema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read terraform provider schema (%s): %w", path, err)
	}
	var f providerSchemaFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse terraform provider schema (%s): %w", path, err)
	}
	if len(f.ProviderSchemas) == 0 {
		return nil, fmt.Errorf("terraform provider schema (%s): no provider_schemas (is this `terraform providers schema -json`?)", path)
	}
	out := &ProviderSchema{FormatVersion: f.FormatVersion, Resources: map[string][]string{}}
	for _, provider := range f.ProviderSchemas {
		for name, res := range provider.ResourceSchemas {
			attrs := map[string]bool{}
			collectProviderAttributes("", res.Block, attrs)
			names := make([]string, 0, len(attrs))
			for a := range attrs {
				names = append(names, a)
			}
			sort.Strings(names)
			out.Resources[name] = names
		}
	}
	if len(out.Resources) == 0 {
		return nil, fmt.Errorf("terraform provider schema (%s): no resource_schemas", path)
	}
	return out, nil
}

func collectProviderAttributes(prefix string, block providerBlock, out map[string]bool) {
	for name := range block.Attributes {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		out[path] = true
	}
	for name, nested := range block.BlockTypes {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		collectProviderAttributes(path, nested.Block, out)
	}
}
