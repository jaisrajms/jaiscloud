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
// Field names are the proto (snake_case) names, which is also what the gRPC
// wire and protojson use. Nested fields use a dotted path
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
	walkProtoFields(service, transport, resource, md, "", depth, &out)
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

func walkProtoFields(service, transport, resource string, md protoreflect.MessageDescriptor, prefix string, depth int, out *[]Field) {
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		if prefix != "" {
			name = prefix + "." + name
		}
		*out = append(*out, Field{
			Service:   service,
			Transport: transport,
			Resource:  resource,
			Name:      name,
			Behavior:  protoBehavior(fd),
		})
		if depth <= 1 || fd.IsMap() || fd.Kind() != protoreflect.MessageKind {
			continue
		}
		child := fd.Message()
		if child == nil || wellKnownMessages[child.FullName()] {
			continue
		}
		walkProtoFields(service, transport, resource, child, name, depth-1, out)
	}
}

// protoBehavior renders a field's definition annotations as a stable token
// list: the google.api.field_behavior enum names lowercased, followed by the
// google.api.resource_reference target type (ref:<type>), which is the
// cross-service reference FC4 later validates. Empty means the definition
// declares no behavior (most optional fields).
func protoBehavior(fd protoreflect.FieldDescriptor) string {
	var tokens []string
	if proto.HasExtension(fd.Options(), annotations.E_FieldBehavior) {
		behaviors, _ := proto.GetExtension(fd.Options(), annotations.E_FieldBehavior).([]annotations.FieldBehavior)
		for _, b := range behaviors {
			tokens = append(tokens, strings.ToLower(strings.TrimPrefix(b.String(), "FIELD_BEHAVIOR_")))
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
