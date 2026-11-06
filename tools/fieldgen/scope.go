//go:build gcp_conformance

package main

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"

	conf "jaiscloud/tests/gcpconformance"
)

// Scope is the curated list of resources whose fields enter the inventory. It
// is intentionally explicit: the field universe must be bounded by a reviewable
// list so a proto module upgrade cannot silently demand thousands of unrelated
// classifications. Services enter the scope when their definition is vendored
// and their pilot waves schedule them (see plan_docs/gcp-defect-coverage-wave-plan.md).
type Scope struct {
	Sources []ScopeSource `yaml:"sources"`
	// References names messages whose google.api.resource_reference fields are
	// inventoried even though the resource itself is outside the field-coverage
	// pilot (FC4: Pub/Sub, Dataproc, Metastore, Logging, Workflows, ...).
	References []ScopeReference `yaml:"references"`
	// DocumentedReferences lists fields a Discovery document documents as a
	// reference but cannot annotate (Discovery has no resource_reference).
	DocumentedReferences []DocumentedReference `yaml:"documented_references"`
}

// ScopeReference is one message walked for reference annotations.
type ScopeReference struct {
	Service  string `yaml:"service"`
	Resource string `yaml:"resource"`
	Message  string `yaml:"message"`
	Depth    int    `yaml:"depth"`
}

// DocumentedReference is a hand-declared reference documented by a Discovery
// description.
type DocumentedReference struct {
	Service  string `yaml:"service"`
	Resource string `yaml:"resource"`
	Field    string `yaml:"field"`
	Target   string `yaml:"target"`
	Reason   string `yaml:"reason"`
}

// ScopeSource is one resource and the definitions that describe it per
// transport.
type ScopeSource struct {
	Service    string                    `yaml:"service"`
	Resource   string                    `yaml:"resource"`
	Transports map[string]ScopeTransport `yaml:"transports"`
}

// ScopeTransport names the authoritative definition for one transport:
// a proto message (compiled descriptor) and/or a Discovery schema. Both may be
// set for the same transport; `message` alone means the transport's JSON is
// protojson for that message (Functions v2, whose only vendored Discovery
// document is v1).
type ScopeTransport struct {
	Message   string `yaml:"message"`
	Discovery string `yaml:"discovery"`
	Schema    string `yaml:"schema"`
	Depth     int    `yaml:"depth"`
}

// Universe is the definition-derived field set, keyed by (service, transport,
// resource, field).
type Universe struct {
	Fields []Field
}

// Field is one definition field on one transport.
//
// Name/Behavior drive the inventory (FC1/FC2). Type, Repeated, ElemType,
// EnumValues and Segments drive the generated round-trip scenarios (FC3): they
// are the definition's wire type, which is what lets a scenario set a
// type-driven non-default value without a hand-written per-field table.
type Field struct {
	Service   string
	Transport string
	Resource  string
	Name      string
	Behavior  string

	// Type is the field's wire type token: string, int32, int64, uint32,
	// uint64, bool, double, float, bytes, enum, duration, timestamp, map,
	// message, struct or any (a google.protobuf.Any).
	Type string
	// Repeated is true for a list or a map: the scenario writes an array/object.
	Repeated bool
	// ElemType is the element type for a repeated field or map value.
	ElemType string
	// EnumValues lists the definition's enum value names, in definition order.
	EnumValues []string
	// Segments is the full path from the resource root, with per-segment
	// repetition, so a generated value can be placed in the right shape.
	Segments []PathSegment
}

// PathSegment is one level of a definition field's path.
type PathSegment struct {
	Name     string
	Repeated bool
}

// Key is the CSV row identity. Transport is part of the key: the REST JSON and
// gRPC proto surfaces of a resource are different wire contracts and are
// classified separately (a field can round-trip over one transport only).
func (f Field) Key() string { return RowKey(f.Service, f.Transport, f.Resource, f.Name) }

// LoadScope reads the scope manifest.
func LoadScope(path string) (*Scope, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scope (%s): %w", path, err)
	}
	var s Scope
	if err := yaml.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("parse scope (%s): %w", path, err)
	}
	if len(s.Sources) == 0 {
		return nil, fmt.Errorf("scope (%s): no sources declared", path)
	}
	seen := map[string]bool{}
	for i, src := range s.Sources {
		if src.Service == "" || src.Resource == "" {
			return nil, fmt.Errorf("scope (%s): source %d is missing service or resource", path, i+1)
		}
		if len(src.Transports) == 0 {
			return nil, fmt.Errorf("scope (%s): %s/%s declares no transports", path, src.Service, src.Resource)
		}
		for transport, t := range src.Transports {
			if transport != "rest" && transport != "grpc" {
				return nil, fmt.Errorf("scope (%s): %s/%s has unknown transport %q (want rest or grpc)", path, src.Service, src.Resource, transport)
			}
			if t.Message == "" && t.Discovery == "" {
				return nil, fmt.Errorf("scope (%s): %s/%s/%s needs message and/or discovery", path, src.Service, src.Resource, transport)
			}
			if (t.Discovery == "") != (t.Schema == "") {
				return nil, fmt.Errorf("scope (%s): %s/%s/%s sets only one of discovery/schema; both are required together", path, src.Service, src.Resource, transport)
			}
			if t.Depth < 1 {
				return nil, fmt.Errorf("scope (%s): %s/%s/%s needs depth >= 1", path, src.Service, src.Resource, transport)
			}
			k := src.Service + "/" + src.Resource + "/" + transport
			if seen[k] {
				return nil, fmt.Errorf("scope (%s): duplicate source %s", path, k)
			}
			seen[k] = true
		}
	}
	for i, r := range s.References {
		if r.Service == "" || r.Resource == "" || r.Message == "" {
			return nil, fmt.Errorf("scope (%s): reference %d is missing service, resource or message", path, i+1)
		}
		if r.Depth < 1 {
			return nil, fmt.Errorf("scope (%s): reference %s/%s needs depth >= 1", path, r.Service, r.Resource)
		}
	}
	for i, d := range s.DocumentedReferences {
		if d.Service == "" || d.Resource == "" || d.Field == "" || d.Target == "" || d.Reason == "" {
			return nil, fmt.Errorf("scope (%s): documented reference %d needs service, resource, field, target and reason", path, i+1)
		}
	}
	return &s, nil
}

// BuildUniverse expands the scope into the definition-derived field list. The
// result is sorted so generation and checking are deterministic.
func BuildUniverse(scope *Scope, docs map[string]*conf.DiscoveryDoc) (*Universe, error) {
	u := &Universe{}
	seen := map[string]bool{}
	add := func(fields []Field) error {
		for _, f := range fields {
			k := f.Key()
			if seen[k] {
				return fmt.Errorf("duplicate definition field %s", k)
			}
			seen[k] = true
			u.Fields = append(u.Fields, f)
		}
		return nil
	}

	for _, src := range scope.Sources {
		transports := make([]string, 0, len(src.Transports))
		for t := range src.Transports {
			transports = append(transports, t)
		}
		sort.Strings(transports)
		for _, transport := range transports {
			t := src.Transports[transport]
			if t.Message != "" {
				fields, err := WalkProto(src.Service, transport, src.Resource, t.Message, t.Depth)
				if err != nil {
					return nil, err
				}
				if err := add(fields); err != nil {
					return nil, err
				}
			}
			if t.Discovery != "" {
				doc, ok := docs[t.Discovery]
				if !ok {
					return nil, fmt.Errorf("discovery document %q is not vendored (schema %s/%s/%s)", t.Discovery, src.Service, src.Resource, t.Schema)
				}
				fields, err := WalkDiscovery(src.Service, transport, src.Resource, doc, t.Schema, t.Depth)
				if err != nil {
					return nil, err
				}
				if err := add(fields); err != nil {
					return nil, err
				}
			}
		}
	}
	sort.Slice(u.Fields, func(i, j int) bool { return u.Fields[i].Key() < u.Fields[j].Key() })
	return u, nil
}
