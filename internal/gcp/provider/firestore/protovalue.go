package firestore

import (
	"fmt"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// Transport-neutral decoders from the pinned `firestorepb` message types to the
// internal store Value. They live in the provider package (not the gRPC
// transport) because the shared pipeline engine (pipeline.go) decodes stage
// argument Values, and the REST ExecutePipeline handler feeds it protojson-decoded
// message trees. The gRPC transport delegates here too, so both transports map
// the protobuf Value the one way.

// ValueFromProto converts a protobuf Value into the internal store Value. A nil
// message yields a nil value; an unsupported oneof variant (a function,
// field-reference or sub-pipeline expression) is an error.
func ValueFromProto(pb *firestorepb.Value) (*firestorestore.Value, error) {
	if pb == nil {
		return nil, nil
	}
	switch tv := pb.ValueType.(type) {
	case *firestorepb.Value_NullValue:
		return firestorestore.NullVal(), nil
	case *firestorepb.Value_BooleanValue:
		return firestorestore.BoolVal(tv.BooleanValue), nil
	case *firestorepb.Value_IntegerValue:
		return firestorestore.IntVal(tv.IntegerValue), nil
	case *firestorepb.Value_DoubleValue:
		return firestorestore.DoubleVal(tv.DoubleValue), nil
	case *firestorepb.Value_TimestampValue:
		return firestorestore.TimestampVal(tv.TimestampValue.AsTime()), nil
	case *firestorepb.Value_StringValue:
		return firestorestore.StringVal(tv.StringValue), nil
	case *firestorepb.Value_BytesValue:
		return firestorestore.BytesVal(tv.BytesValue), nil
	case *firestorepb.Value_ReferenceValue:
		return firestorestore.ReferenceVal(tv.ReferenceValue), nil
	case *firestorepb.Value_GeoPointValue:
		return firestorestore.GeoPointVal(tv.GeoPointValue.GetLatitude(), tv.GeoPointValue.GetLongitude()), nil
	case *firestorepb.Value_ArrayValue:
		vals := make([]*firestorestore.Value, 0, len(tv.ArrayValue.GetValues()))
		for _, e := range tv.ArrayValue.GetValues() {
			v, err := ValueFromProto(e)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		return firestorestore.ArrayVal(vals...), nil
	case *firestorepb.Value_MapValue:
		fields := make(map[string]*firestorestore.Value, len(tv.MapValue.GetFields()))
		for k, e := range tv.MapValue.GetFields() {
			v, err := ValueFromProto(e)
			if err != nil {
				return nil, err
			}
			fields[k] = v
		}
		return firestorestore.MapVal(fields), nil
	default:
		return nil, fmt.Errorf("unsupported Firestore value type %T", pb.ValueType)
	}
}

// ArrayValueFromProto converts a protobuf ArrayValue into the internal store
// ArrayValue. A nil message yields an empty array value.
func ArrayValueFromProto(pb *firestorepb.ArrayValue) (*firestorestore.ArrayValue, error) {
	av := &firestorestore.ArrayValue{}
	if pb == nil {
		return av, nil
	}
	for _, v := range pb.GetValues() {
		dv, err := ValueFromProto(v)
		if err != nil {
			return nil, err
		}
		av.Values = append(av.Values, dv)
	}
	return av, nil
}

// FieldsFromProto converts a protobuf Document's field map into internal Values.
func FieldsFromProto(pb *firestorepb.Document) (map[string]*firestorestore.Value, error) {
	if pb == nil {
		return map[string]*firestorestore.Value{}, nil
	}
	fields := make(map[string]*firestorestore.Value, len(pb.GetFields()))
	for k, v := range pb.GetFields() {
		fv, err := ValueFromProto(v)
		if err != nil {
			return nil, err
		}
		fields[k] = fv
	}
	return fields, nil
}
