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
type Field struct {
	Service   string
	Transport string
	Resource  string
	Name      string
	Behavior  string
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
			if t.Message == "" && (t.Discovery == "" || t.Schema == "") {
				return nil, fmt.Errorf("scope (%s): %s/%s/%s needs message or discovery+schema", path, src.Service, src.Resource, transport)
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
