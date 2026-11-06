//go:build gcp_conformance

package main

import (
	"bytes"
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
	walkDiscoverySchema(service, transport, resource, doc, schema, nil, depth, &out)
	sortFields(out)
	return out, nil
}

func walkDiscoverySchema(service, transport, resource string, doc *conf.DiscoveryDoc, schema *conf.Schema, segs []PathSegment, depth int, out *[]Field) {
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		prop := schema.Properties[name]
		typ, repeated, elemType, enumValues := discoveryFieldType(prop)
		path := append(append([]PathSegment(nil), segs...),
			PathSegment{Name: name, Repeated: prop != nil && (prop.Type == "array" || prop.Items != nil)})
		fieldName := ""
		for i, s := range path {
			if i > 0 {
				fieldName += "."
			}
			fieldName += s.Name
		}
		*out = append(*out, Field{
			Service:    service,
			Transport:  transport,
			Resource:   resource,
			Name:       fieldName,
			Behavior:   discoveryBehavior(prop),
			Type:       typ,
			Repeated:   repeated,
			ElemType:   elemType,
			EnumValues: enumValues,
			Segments:   path,
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

// discoveryFieldType maps a Discovery property onto the scenario generator's
// type tokens. Discovery has no message/enum distinction beyond these fields,
// and its formats carry the well-known types (google-datetime/duration).
func discoveryFieldType(s *conf.Schema) (typ string, repeated bool, elem string, enums []string) {
	if s == nil {
		return "any", false, "", nil
	}
	if s.Type == "array" || s.Items != nil {
		t, _, en := discoveryScalarType(s.Items)
		return t, true, t, en
	}
	if s.Ref != "" || len(s.Properties) > 0 {
		return "message", false, "", nil
	}
	if len(s.AdditionalProperties) > 0 && string(bytes.TrimSpace(s.AdditionalProperties)) != "false" {
		return "map", true, "string", nil
	}
	t, _, en := discoveryScalarType(s)
	return t, false, "", en
}

// discoveryScalarType maps one non-repeated Discovery schema onto a type token.
func discoveryScalarType(s *conf.Schema) (typ, format string, enums []string) {
	if s == nil {
		return "any", "", nil
	}
	if s.Ref != "" || len(s.Properties) > 0 {
		return "message", "", nil
	}
	if len(s.Enum) > 0 {
		return "enum", s.Format, s.Enum
	}
	switch s.Format {
	case "google-datetime", "date-time":
		return "timestamp", s.Format, nil
	case "google-duration":
		return "duration", s.Format, nil
	case "int32", "uint32", "int64", "uint64", "double", "float":
		return s.Format, s.Format, nil
	case "byte", "bytes", "base64":
		return "bytes", s.Format, nil
	}
	switch s.Type {
	case "boolean":
		return "bool", "", nil
	case "integer":
		return "int32", "", nil
	case "number":
		return "double", "", nil
	case "string":
		return "string", "", nil
	case "object":
		return "message", "", nil
	case "any":
		return "any", "", nil
	}
	return "any", "", nil
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
