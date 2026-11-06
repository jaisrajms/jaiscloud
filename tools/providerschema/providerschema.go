// Package providerschema parses a `terraform providers schema -json` dump into
// the parts the GCP coverage tooling needs: per resource type, the sorted
// attribute paths and their schema flags (computed / optional / required).
//
// It is shared by tools/fieldgen (the FC1 field inventory's Terraform hook) and
// tools/tfcapture (FC6's sent-vs-declared join), so both read one definition
// and a provider upgrade cannot make them disagree.
package providerschema

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Attribute is one provider attribute path (nested blocks are joined with a
// dot, e.g. "versioning.enabled") and the flags the provider declares.
type Attribute struct {
	Path     string
	Computed bool
	Optional bool
	Required bool
}

// ProviderSchema is a loaded Terraform provider schema reduced to resource type
// -> attributes.
type ProviderSchema struct {
	FormatVersion string
	Resources     map[string][]Attribute
}

// ResourceCount is the number of provider resource types in the schema.
func (p *ProviderSchema) ResourceCount() int { return len(p.Resources) }

// Attributes returns the sorted attribute paths declared for a provider
// resource type.
func (p *ProviderSchema) Attributes(resourceType string) []string {
	specs := p.Resources[resourceType]
	out := make([]string, 0, len(specs))
	for _, a := range specs {
		out = append(out, a.Path)
	}
	sort.Strings(out)
	return out
}

// AttributeSpecs returns the attributes of one resource type sorted by path.
func (p *ProviderSchema) AttributeSpecs(resourceType string) []Attribute {
	return p.Resources[resourceType]
}

type file struct {
	FormatVersion   string `json:"format_version"`
	ProviderSchemas map[string]struct {
		ResourceSchemas map[string]struct {
			Block block `json:"block"`
		} `json:"resource_schemas"`
	} `json:"provider_schemas"`
}

type block struct {
	Attributes map[string]struct {
		Computed bool `json:"computed"`
		Optional bool `json:"optional"`
		Required bool `json:"required"`
	} `json:"attributes"`
	BlockTypes map[string]struct {
		Block block `json:"block"`
	} `json:"block_types"`
}

// Load reads and parses a `terraform providers schema -json` dump.
func Load(path string) (*ProviderSchema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read terraform provider schema (%s): %w", path, err)
	}
	return Parse(raw, path)
}

// Parse parses a dump already read into memory.
func Parse(raw []byte, path string) (*ProviderSchema, error) {
	var f file
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse terraform provider schema (%s): %w", path, err)
	}
	if len(f.ProviderSchemas) == 0 {
		return nil, fmt.Errorf("terraform provider schema (%s): no provider_schemas (is this `terraform providers schema -json`?)", path)
	}
	out := &ProviderSchema{FormatVersion: f.FormatVersion, Resources: map[string][]Attribute{}}
	for _, provider := range f.ProviderSchemas {
		for name, res := range provider.ResourceSchemas {
			attrs := map[string]Attribute{}
			collect("", res.Block, attrs)
			list := make([]Attribute, 0, len(attrs))
			for _, a := range attrs {
				list = append(list, a)
			}
			sort.Slice(list, func(i, j int) bool { return list[i].Path < list[j].Path })
			out.Resources[name] = list
		}
	}
	if len(out.Resources) == 0 {
		return nil, fmt.Errorf("terraform provider schema (%s): no resource_schemas", path)
	}
	return out, nil
}

func collect(prefix string, b block, out map[string]Attribute) {
	for name, spec := range b.Attributes {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		out[path] = Attribute{Path: path, Computed: spec.Computed, Optional: spec.Optional, Required: spec.Required}
	}
	for name, nested := range b.BlockTypes {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		collect(path, nested.Block, out)
	}
}

// Compact rewrites a `terraform providers schema -json` dump to a small,
// deterministic snapshot of the named resource types (attribute flags and
// nesting only, no descriptions), suitable for committing and diffing.
func Compact(raw []byte, resources []string) ([]byte, error) {
	var f file
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse terraform provider schema: %w", err)
	}
	if len(f.ProviderSchemas) == 0 {
		return nil, fmt.Errorf("terraform provider schema: no provider_schemas")
	}
	keep := map[string]bool{}
	for _, r := range resources {
		if r = strings.TrimSpace(r); r != "" {
			keep[r] = true
		}
	}
	type compactBlock struct {
		Attributes map[string]map[string]bool `json:"attributes,omitempty"`
		BlockTypes map[string]any             `json:"block_types,omitempty"`
	}
	var compact func(b block) compactBlock
	compact = func(b block) compactBlock {
		out := compactBlock{}
		if len(b.Attributes) > 0 {
			out.Attributes = map[string]map[string]bool{}
			for name, spec := range b.Attributes {
				flags := map[string]bool{}
				for k, v := range map[string]bool{"computed": spec.Computed, "optional": spec.Optional, "required": spec.Required} {
					if v {
						flags[k] = true
					}
				}
				out.Attributes[name] = flags
			}
		}
		if len(b.BlockTypes) > 0 {
			out.BlockTypes = map[string]any{}
			for name, nested := range b.BlockTypes {
				out.BlockTypes[name] = map[string]any{"block": compact(nested.Block)}
			}
		}
		return out
	}
	type providerOut struct {
		ResourceSchemas map[string]struct {
			Block compactBlock `json:"block"`
		} `json:"resource_schemas"`
	}
	out := struct {
		FormatVersion   string                 `json:"format_version"`
		ProviderSchemas map[string]providerOut `json:"provider_schemas"`
	}{FormatVersion: f.FormatVersion, ProviderSchemas: map[string]providerOut{}}
	for providerName, provider := range f.ProviderSchemas {
		p := providerOut{ResourceSchemas: map[string]struct {
			Block compactBlock `json:"block"`
		}{}}
		for name, res := range provider.ResourceSchemas {
			if !keep[name] {
				continue
			}
			p.ResourceSchemas[name] = struct {
				Block compactBlock `json:"block"`
			}{Block: compact(res.Block)}
		}
		out.ProviderSchemas[providerName] = p
	}
	return json.MarshalIndent(out, "", " ")
}
