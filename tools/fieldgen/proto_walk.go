//go:build gcp_conformance

package main

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// wellKnownMessages are proto types whose internal fields are never part of a
// resource's own wire surface: a google.protobuf.Timestamp is one scalar to the
// API even though it has seconds/nanos fields.
var wellKnownMessages = map[protoreflect.FullName]bool{
	"google.protobuf.Timestamp":   true,
	"google.protobuf.Duration":    true,
	"google.protobuf.FieldMask":   true,
	"google.protobuf.Any":         true,
	"google.protobuf.Struct":      true,
	"google.protobuf.Value":       true,
	"google.protobuf.ListValue":   true,
	"google.protobuf.Empty":       true,
	"google.protobuf.Int64Value":  true,
	"google.protobuf.UInt64Value": true,
	"google.protobuf.StringValue": true,
	"google.protobuf.BoolValue":   true,
	"google.protobuf.DoubleValue": true,
	"google.protobuf.FloatValue":  true,
	"google.protobuf.Int32Value":  true,
	"google.protobuf.UInt32Value": true,
	"google.protobuf.BytesValue":  true,
}

// WalkProto enumerates the fields of a compiled proto message, recursing into
// nested message fields up to depth levels (depth 1 = the message's own fields).
//
// Field names are the proto (snake_case) names on gRPC and the protojson
// (camelCase) names on a REST surface defined by a proto. Nested fields use a dotted path
// (retention_policy.retention_period). Map fields are recorded as the map field
// itself and not expanded: Discovery models labels/annotations as additional
// properties, and their keys are caller data, not API fields.
func WalkProto(service, transport, resource, message string, depth int) ([]Field, error) {
	md, err := messageDescriptor(message)
	if err != nil {
		return nil, err
	}
	return walkMessage(service, transport, resource, md, depth)
}

// walkMessage enumerates an already-resolved message descriptor. Tests build a
// fixture descriptor in memory and call this directly.
func walkMessage(service, transport, resource string, md protoreflect.MessageDescriptor, depth int) ([]Field, error) {
	if depth < 1 {
		return nil, fmt.Errorf("proto %s: depth must be >= 1", md.FullName())
	}
	var out []Field
	walkProtoFields(service, transport, resource, md, nil, depth, &out)
	sortFields(out)
	return out, nil
}

func messageDescriptor(name string) (protoreflect.MessageDescriptor, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, fmt.Errorf("proto message %s is not linked into this tool: %w", name, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("proto symbol %s is not a message", name)
	}
	return md, nil
}

// walkProtoFields enumerates md's fields. prefix is the transport-native dotted
// path so far and segs the same path as structured segments (the CSV name and
// the generated scenario path share one origin).
func walkProtoFields(service, transport, resource string, md protoreflect.MessageDescriptor, segs []PathSegment, depth int, out *[]Field) {
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		// gRPC/protojson uses the proto name; a REST JSON surface (protojson)
		// uses the declared json_name, which is camelCase for these APIs.
		name := string(fd.Name())
		if transport == "rest" {
			name = string(fd.JSONName())
		}
		typ, repeated, elemType, enumValues := protoFieldType(fd)
		seg := PathSegment{Name: name, Repeated: fd.IsList() || fd.IsMap()}
		path := append(append([]PathSegment(nil), segs...), seg)
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
			Behavior:   protoBehavior(fd),
			Type:       typ,
			Repeated:   repeated,
			ElemType:   elemType,
			EnumValues: enumValues,
			Segments:   path,
		})
		if depth <= 1 || fd.IsMap() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		child := fd.Message()
		if child == nil || wellKnownMessages[child.FullName()] {
			continue
		}
		walkProtoFields(service, transport, resource, child, path, depth-1, out)
	}
}

// protoFieldType returns the field's wire type token, whether it repeats, the
// element type, and the enum values when it is an enum.
func protoFieldType(fd protoreflect.FieldDescriptor) (typ string, repeated bool, elem string, enums []string) {
	if fd.IsMap() {
		return "map", true, protoScalarType(fd.MapValue()), nil
	}
	typ = protoScalarType(fd)
	if fd.IsList() {
		return typ, true, typ, protoEnumValues(fd)
	}
	return typ, false, "", protoEnumValues(fd)
}

// protoScalarType maps a descriptor's kind onto the type token the scenario
// generator understands. Well-known message types are flattened to the value
// they represent on the wire (a Timestamp is one RFC3339 string, not a message).
func protoScalarType(fd protoreflect.FieldDescriptor) string {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.EnumKind:
		return "enum"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if md := fd.Message(); md != nil {
			switch md.FullName() {
			case "google.protobuf.Timestamp":
				return "timestamp"
			case "google.protobuf.Duration":
				return "duration"
			case "google.protobuf.Struct":
				return "struct"
			case "google.protobuf.Any":
				return "any"
			case "google.protobuf.StringValue":
				return "string"
			case "google.protobuf.BoolValue":
				return "bool"
			case "google.protobuf.Int32Value":
				return "int32"
			case "google.protobuf.UInt32Value":
				return "uint32"
			case "google.protobuf.Int64Value":
				return "int64"
			case "google.protobuf.UInt64Value":
				return "uint64"
			case "google.protobuf.FloatValue":
				return "float"
			case "google.protobuf.DoubleValue":
				return "double"
			case "google.protobuf.BytesValue":
				return "bytes"
			}
		}
		return "message"
	}
	return "any"
}

// protoEnumValues lists the field's enum value names in definition order.
func protoEnumValues(fd protoreflect.FieldDescriptor) []string {
	if fd.Kind() != protoreflect.EnumKind || fd.Enum() == nil {
		return nil
	}
	values := fd.Enum().Values()
	out := make([]string, 0, values.Len())
	for i := 0; i < values.Len(); i++ {
		out = append(out, string(values.Get(i).Name()))
	}
	return out
}

// protoBehavior renders a field's definition annotations as a stable token
// list: the google.api.field_behavior enum names lowercased, followed by the
// google.api.resource_reference target type (ref:<type>), which is the
// cross-service reference FC4 later validates. Empty means the definition
// declares no behavior (most optional fields).
func protoBehavior(fd protoreflect.FieldDescriptor) string {
	var tokens []string
	if proto.HasExtension(fd.Options(), annotations.E_FieldBehavior) {
		if behaviors, ok := proto.GetExtension(fd.Options(), annotations.E_FieldBehavior).([]annotations.FieldBehavior); ok {
			for _, b := range behaviors {
				tokens = append(tokens, strings.ToLower(strings.TrimPrefix(b.String(), "FIELD_BEHAVIOR_")))
			}
		}
	}
	if proto.HasExtension(fd.Options(), annotations.E_ResourceReference) {
		if ref, ok := proto.GetExtension(fd.Options(), annotations.E_ResourceReference).(*annotations.ResourceReference); ok && ref != nil {
			target := ref.GetType()
			if target == "" {
				target = ref.GetChildType()
			}
			if target == "" {
				target = "*"
			}
			tokens = append(tokens, "ref:"+target)
		}
	}
	return strings.Join(dedupe(tokens), "|")
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := in[:0]
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func sortFields(fields []Field) {
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Name != fields[j].Name {
			return fields[i].Name < fields[j].Name
		}
		return fields[i].Transport < fields[j].Transport
	})
}
