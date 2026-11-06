//go:build gcp_conformance

package main

import (
	"testing"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// testWidgetProto builds a small proto fixture in memory: no protoc, no
// generated code, just the descriptor the walker consumes.
func testWidgetProto(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED

	field := func(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label, typeName string) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(name),
			Number: proto.Int32(num),
			Type:   typ.Enum(),
			Label:  label.Enum(),
		}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	withBehavior := func(f *descriptorpb.FieldDescriptorProto, behaviors ...annotations.FieldBehavior) *descriptorpb.FieldDescriptorProto {
		f.Options = &descriptorpb.FieldOptions{}
		proto.SetExtension(f.Options, annotations.E_FieldBehavior, behaviors)
		return f
	}
	withRef := func(f *descriptorpb.FieldDescriptorProto, refType string) *descriptorpb.FieldDescriptorProto {
		if f.Options == nil {
			f.Options = &descriptorpb.FieldOptions{}
		}
		proto.SetExtension(f.Options, annotations.E_ResourceReference, &annotations.ResourceReference{Type: refType})
		return f
	}

	name := withRef(withBehavior(field("name", 1, str, opt, ""), annotations.FieldBehavior_REQUIRED), "test.googleapis.com/Widget")
	displayName := field("display_name", 6, str, opt, "")
	createTime := withBehavior(field("create_time", 2, msg, opt, ".google.protobuf.Timestamp"), annotations.FieldBehavior_OUTPUT_ONLY)
	settings := field("settings", 3, msg, opt, ".test.v1.Widget.Settings")
	items := field("items", 4, msg, rep, ".test.v1.Widget.Item")
	// A proto3 map field: the entry message is an implementation detail.
	mapEntry := field("labels", 5, msg, rep, ".test.v1.Widget.LabelsEntry")

	settingsMsg := &descriptorpb.DescriptorProto{
		Name: proto.String("Settings"),
		Field: []*descriptorpb.FieldDescriptorProto{
			withBehavior(field("mode", 1, str, opt, ""), annotations.FieldBehavior_OUTPUT_ONLY),
			field("level", 2, str, opt, ""),
		},
	}
	itemMsg := &descriptorpb.DescriptorProto{
		Name: proto.String("Item"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("label", 1, str, opt, ""),
		},
	}
	labelsEntry := &descriptorpb.DescriptorProto{
		Name:    proto.String("LabelsEntry"),
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("key"), Number: proto.Int32(1), Type: str.Enum(), Label: opt.Enum()},
			{Name: proto.String("value"), Number: proto.Int32(2), Type: str.Enum(), Label: opt.Enum()},
		},
	}
	widget := &descriptorpb.DescriptorProto{
		Name:  proto.String("Widget"),
		Field: []*descriptorpb.FieldDescriptorProto{name, createTime, settings, items, mapEntry, displayName},
		NestedType: []*descriptorpb.DescriptorProto{
			settingsMsg, itemMsg, labelsEntry,
		},
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("test/v1/widget.proto"),
		Package:     proto.String("test.v1"),
		Syntax:      proto.String("proto3"),
		Dependency:  []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{widget},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build fixture descriptor: %v", err)
	}
	return fd.Messages().ByName("Widget")
}

func TestWalkProtoFields(t *testing.T) {
	md := testWidgetProto(t)
	fields, err := walkMessage("test", "grpc", "Widget", md, 2)
	if err != nil {
		t.Fatalf("walkMessage: %v", err)
	}
	got := map[string]string{}
	for _, f := range fields {
		got[f.Name] = f.Behavior
	}
	want := map[string]string{
		"name":           "required|ref:test.googleapis.com/Widget",
		"create_time":    "output_only",
		"settings":       "",
		"settings.mode":  "output_only",
		"settings.level": "",
		"items":          "",
		"items.label":    "",
		"labels":         "",
		"display_name":   "",
	}
	for name, behavior := range want {
		b, ok := got[name]
		if !ok {
			t.Errorf("missing field %q (walked: %v)", name, keys(got))
			continue
		}
		if b != behavior {
			t.Errorf("%s behavior = %q, want %q", name, b, behavior)
		}
	}
	if len(fields) != len(want) {
		t.Errorf("walked %d fields, want %d: %v", len(fields), len(want), keys(got))
	}
	// Map entries and well-known types are one field, not their internals.
	for name := range got {
		if name == "labels.key" || name == "labels.value" || name == "create_time.seconds" {
			t.Errorf("walk descended into an implementation-only message: %s", name)
		}
	}
}

// TestWalkProtoRESTUsesJSONNames pins the REST surface's field names: a REST
// JSON API defined by a proto uses protojson's json_name (camelCase), not the
// proto name.
func TestWalkProtoRESTUsesJSONNames(t *testing.T) {
	md := testWidgetProto(t)
	fields, err := walkMessage("test", "rest", "Widget", md, 2)
	if err != nil {
		t.Fatalf("walkMessage: %v", err)
	}
	got := map[string]string{}
	for _, f := range fields {
		got[f.Name] = f.Behavior
	}
	if _, ok := got["displayName"]; !ok {
		t.Fatalf("REST walk did not use json_name: %v", keys(got))
	}
	if _, ok := got["display_name"]; ok {
		t.Fatalf("REST walk kept the proto name: %v", keys(got))
	}
	if got["createTime"] != "output_only" {
		t.Fatalf("createTime behavior = %q, want output_only", got["createTime"])
	}
}

func TestWalkProtoDepthLimit(t *testing.T) {
	md := testWidgetProto(t)
	fields, err := walkMessage("test", "grpc", "Widget", md, 1)
	if err != nil {
		t.Fatalf("walkMessage: %v", err)
	}
	for _, f := range fields {
		if f.Name != "name" && f.Name != "create_time" && f.Name != "settings" && f.Name != "items" && f.Name != "labels" && f.Name != "display_name" {
			t.Errorf("depth 1 walked nested field %q", f.Name)
		}
	}
}

func TestWalkProtoUnknownMessage(t *testing.T) {
	if _, err := WalkProto("test", "grpc", "Widget", "test.v1.DoesNotExist", 1); err == nil {
		t.Fatal("WalkProto(unknown) = nil error, want a resolution error")
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
