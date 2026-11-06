//go:build gcp_conformance

package main

import (
	"fmt"
	"sort"

	conf "jaiscloud/tests/gcpconformance"
)

// WalkDiscovery enumerates the fields of a Discovery schema, following $ref and
// array item schemas into nested objects up to depth levels (depth 1 = the
// document's top-level properties). Field names are the Discovery JSON names
// (camelCase) — the actual REST wire names.
func WalkDiscovery(service, transport, resource string, doc *conf.DiscoveryDoc, schemaName string, depth int) ([]Field, error) {
	if depth < 1 {
		return nil, fmt.Errorf("discovery %s/%s: depth must be >= 1", doc.Name, schemaName)
	}
	schema, ok := doc.Schemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("discovery %s v%s has no schema %q", doc.Name, doc.Version, schemaName)
	}
	var out []Field
	walkDiscoverySchema(service, transport, resource, doc, schema, "", depth, &out)
	sortFields(out)
	return out, nil
}

func walkDiscoverySchema(service, transport, resource string, doc *conf.DiscoveryDoc, schema *conf.Schema, prefix string, depth int, out *[]Field) {
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		prop := schema.Properties[name]
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		*out = append(*out, Field{
			Service:   service,
			Transport: transport,
			Resource:  resource,
			Name:      path,
			Behavior:  discoveryBehavior(prop),
		})
		if depth <= 1 {
			continue
		}
		child, ok := resolveDiscoveryObject(doc, prop)
		if !ok {
			continue
		}
		walkDiscoverySchema(service, transport, resource, doc, child, path, depth-1, out)
	}
}

// resolveDiscoveryObject returns the object schema a property points at, either
// through a $ref, through an array's items.$ref/items.properties, or because the
// property itself declares properties. Scalar properties resolve to nothing.
func resolveDiscoveryObject(doc *conf.DiscoveryDoc, prop *conf.Schema) (*conf.Schema, bool) {
	if prop == nil {
		return nil, false
	}
	if prop.Ref != "" {
		return doc.ResolveRef(prop.Ref)
	}
	if prop.Items != nil {
		if prop.Items.Ref != "" {
			return doc.ResolveRef(prop.Items.Ref)
		}
		if len(prop.Items.Properties) > 0 {
			return prop.Items, true
		}
	}
	if len(prop.Properties) > 0 {
		return prop, true
	}
	return nil, false
}

// discoveryBehavior renders the Discovery readOnly flag as the proto
// field_behavior equivalent (read_only). Discovery has no IMMUTABLE or
// REQUIRED-field analog beyond the schema's `required` list, which is recorded
// by the definition itself and not a per-field runtime behavior.
func discoveryBehavior(s *conf.Schema) string {
	if s != nil && s.ReadOnly {
		return "read_only"
	}
	return ""
}
