// Package firestore implements the Firestore gRPC service over the shared
// transport-agnostic provider Service.
//
// # Listen fidelity
//
// Listen delta replay uses the shared change-feed's monotonic sequence as the
// resume position. A per-target TargetChange_CURRENT token is a versioned,
// scope-bound token: "FTK1" + sequence + an opaque fingerprint of the target's
// query scope (parent plus structured query, or its document list). The legacy
// 8-byte bare sequence is still accepted and is still what the stream-wide
// empty-target_ids NO_CHANGE frame carries. Deliberate approximations of real
// Firestore are documented here:
//
//   - Bounded history. The change-feed retains the most recent
//     changeLogRetention events and drops older ones, advancing a floor. A resume
//     token below the floor (or from before a Reset), malformed, or of zero, is
//     treated as expired: the target is reset (TargetChange_RESET) and receives a
//     fresh snapshot, matching real Firestore's behavior for an un-honorable
//     resume token. A token of 0 is likewise not a resumable position, because the
//     log records writes rather than a full document history.
//   - Scope-bound tokens. A per-target token identifies the target scope it was
//     minted for. Replaying it against a different query would deliver deltas for
//     documents the new query does not select, so that target is reset before its
//     fresh snapshot instead. The stream-wide NO_CHANGE token cannot name one
//     target's identity (it covers every target on the stream), so it stays
//     unbound: resuming from it replays against the added target with that
//     target's query filtering the catch-up, but cannot trigger a scope reset.
//   - Per-target cursors. Each target tracks the last sequence delivered to it.
//     Its TargetChange_CURRENT frame carries that target's own sequence. The
//     empty-target_ids NO_CHANGE frame's token must cover all targets at once; it
//     carries the minimum of the live cursors — resuming from there can replay a
//     duplicate delta but never skip one. Real Firestore pairs each resume token
//     with its own target_ids, which a single "all targets" token cannot express.
//   - Exactly-once delivery per target. A live change whose sequence is at or
//     below the target's delivered cursor is dropped, so a write that races the
//     snapshot/replay is not delivered twice. The stream-wide NO_CHANGE token
//     above remains the one place a duplicate can surface: resuming from the
//     min-of-cursors token may replay a delta another target already observed.
//   - Read position. A resume token is a position in the write feed, not a
//     query-range read position: an incremental resume replays matching deltas
//     only and does not re-stream the pre-existing result set, so a client must
//     already hold the snapshot it is resuming from. Snapshots are computed from
//     current document state rather than a consistent read-time index.
//   - Replay membership. A query target's catch-up is a net reconciliation:
//     documents the feed touched and the query now selects are sent as
//     DocumentChange, and touched documents it no longer selects are removed or
//     deleted. Membership comes from re-running the query, so where/order_by/
//     limit/offset apply exactly as they do to live deltas. Whether a removed
//     document was in the view at the resume position is read from its retained
//     pre-resume change-log body; when that body has already been evicted (the
//     feed's history window is bounded), no removal is invented for it and no
//     order/limit shift caused solely by an untouched document is replayed.
package firestore

import (
	"fmt"
	"time"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
	firestoreprovider "jaiscloud/internal/gcp/provider/firestore"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/gcp/timestamp"

	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ─── Value ────────────────────────────────────────────────────────────────────

// decodeValue converts a protobuf Value into the internal store Value. The one
// implementation lives in the provider package (shared with the REST
// ExecutePipeline engine); this delegates so both transports map it identically.
func decodeValue(pb *firestorepb.Value) (*firestorestore.Value, error) {
	return firestoreprovider.ValueFromProto(pb)
}

// encodeValue converts an internal store Value into a protobuf Value.
func encodeValue(v *firestorestore.Value) *firestorepb.Value {
	if v == nil {
		return &firestorepb.Value{}
	}
	switch {
	case v.NullValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{}}
	case v.BooleanValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_BooleanValue{BooleanValue: *v.BooleanValue}}
	case v.IntegerValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: *v.IntegerValue}}
	case v.DoubleValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.TimestampValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_TimestampValue{TimestampValue: timestamppb.New(*v.TimestampValue)}}
	case v.StringValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_StringValue{StringValue: *v.StringValue}}
	case v.BytesValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_BytesValue{BytesValue: v.BytesValue}}
	case v.ReferenceValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: *v.ReferenceValue}}
	case v.GeoPointValue != nil:
		return &firestorepb.Value{ValueType: &firestorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{
			Latitude:  v.GeoPointValue.Latitude,
			Longitude: v.GeoPointValue.Longitude,
		}}}
	case v.ArrayValue != nil:
		vals := make([]*firestorepb.Value, 0, len(v.ArrayValue.Values))
		for _, e := range v.ArrayValue.Values {
			vals = append(vals, encodeValue(e))
		}
		return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: vals}}}
	case v.MapValue != nil:
		fields := make(map[string]*firestorepb.Value, len(v.MapValue.Fields))
		for k, e := range v.MapValue.Fields {
			fields[k] = encodeValue(e)
		}
		return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{MapValue: &firestorepb.MapValue{Fields: fields}}}
	}
	return &firestorepb.Value{}
}

// ─── Document ─────────────────────────────────────────────────────────────────

// decodeFields converts a protobuf Document's field map into internal Values,
// delegating to the shared provider decoder.
func decodeFields(pb *firestorepb.Document) (map[string]*firestorestore.Value, error) {
	return firestoreprovider.FieldsFromProto(pb)
}

// encodeDocument converts an internal store Document into a protobuf Document.
func encodeDocument(d firestorestore.Document) *firestorepb.Document {
	doc := &firestorepb.Document{Name: d.Name}
	if len(d.Fields) > 0 {
		doc.Fields = make(map[string]*firestorepb.Value, len(d.Fields))
		for k, v := range d.Fields {
			doc.Fields[k] = encodeValue(v)
		}
	}
	if !d.CreateTime.IsZero() {
		doc.CreateTime = timestamp.Proto(d.CreateTime)
	}
	if !d.UpdateTime.IsZero() {
		doc.UpdateTime = timestamp.Proto(d.UpdateTime)
	}
	return doc
}

func encodeDocuments(docs []firestorestore.Document) []*firestorepb.Document {
	out := make([]*firestorepb.Document, 0, len(docs))
	for _, d := range docs {
		out = append(out, encodeDocument(d))
	}
	return out
}

// ─── StructuredQuery ──────────────────────────────────────────────────────────

// decodeStructuredQuery converts a protobuf StructuredQuery into the internal
// structuredQuery wire struct.
func decodeStructuredQuery(pb *firestorepb.StructuredQuery) (*firestoreprovider.StructuredQuery, error) {
	if pb == nil {
		return nil, fmt.Errorf("structuredQuery is required")
	}
	q := &firestoreprovider.StructuredQuery{}
	if sel := pb.GetSelect(); sel != nil {
		proj := &firestoreprovider.Projection{}
		for _, f := range sel.GetFields() {
			proj.Fields = append(proj.Fields, firestoreprovider.FieldReference{FieldPath: f.GetFieldPath()})
		}
		q.Select = proj
	}
	for _, cs := range pb.GetFrom() {
		q.From = append(q.From, firestoreprovider.CollectionSelector{
			CollectionID:   cs.GetCollectionId(),
			AllDescendants: cs.GetAllDescendants(),
		})
	}
	if w := pb.GetWhere(); w != nil {
		f, err := decodeFilter(w)
		if err != nil {
			return nil, err
		}
		q.Where = f
	}
	for _, o := range pb.GetOrderBy() {
		q.OrderBy = append(q.OrderBy, firestoreprovider.Order{
			Field:     firestoreprovider.FieldReference{FieldPath: o.GetField().GetFieldPath()},
			Direction: directionToString(o.GetDirection()),
		})
	}
	if sa := pb.GetStartAt(); sa != nil {
		c, err := decodeCursor(sa)
		if err != nil {
			return nil, err
		}
		q.StartAt = c
	}
	if ea := pb.GetEndAt(); ea != nil {
		c, err := decodeCursor(ea)
		if err != nil {
			return nil, err
		}
		q.EndAt = c
	}
	q.Offset = int64(pb.GetOffset())
	if l := pb.GetLimit(); l != nil {
		q.Limit = int64(l.GetValue())
	}
	return q, nil
}

func decodeFilter(pb *firestorepb.StructuredQuery_Filter) (*firestoreprovider.Filter, error) {
	switch f := pb.FilterType.(type) {
	case *firestorepb.StructuredQuery_Filter_CompositeFilter:
		cf := &firestoreprovider.CompositeFilter{Op: compositeOpToString(f.CompositeFilter.GetOp())}
		for _, c := range f.CompositeFilter.GetFilters() {
			sub, err := decodeFilter(c)
			if err != nil {
				return nil, err
			}
			cf.Filters = append(cf.Filters, sub)
		}
		return &firestoreprovider.Filter{CompositeFilter: cf}, nil
	case *firestorepb.StructuredQuery_Filter_FieldFilter:
		ff := &firestoreprovider.FieldFilter{
			Field: firestoreprovider.FieldReference{FieldPath: f.FieldFilter.GetField().GetFieldPath()},
			Op:    fieldFilterOpToString(f.FieldFilter.GetOp()),
		}
		if v := f.FieldFilter.GetValue(); v != nil {
			fv, err := decodeValue(v)
			if err != nil {
				return nil, err
			}
			ff.Value = fv
		}
		return &firestoreprovider.Filter{FieldFilter: ff}, nil
	case *firestorepb.StructuredQuery_Filter_UnaryFilter:
		uf := &firestoreprovider.UnaryFilter{
			Op:    unaryOpToString(f.UnaryFilter.GetOp()),
			Field: firestoreprovider.FieldReference{FieldPath: f.UnaryFilter.GetField().GetFieldPath()},
		}
		return &firestoreprovider.Filter{UnaryFilter: uf}, nil
	}
	return nil, fmt.Errorf("unsupported filter type %T", pb.FilterType)
}

func decodeCursor(c *firestorepb.Cursor) (*firestoreprovider.Cursor, error) {
	cur := &firestoreprovider.Cursor{Before: c.GetBefore()}
	for _, v := range c.GetValues() {
		dv, err := decodeValue(v)
		if err != nil {
			return nil, err
		}
		cur.Values = append(cur.Values, dv)
	}
	return cur, nil
}

// ─── Write / transform / precondition ────────────────────────────────────────

// decodeWrites converts protobuf Writes into internal writeWire structs.
func decodeWrites(pbs []*firestorepb.Write) ([]*firestoreprovider.WriteWire, error) {
	out := make([]*firestoreprovider.WriteWire, 0, len(pbs))
	for _, w := range pbs {
		dw, err := decodeWrite(w)
		if err != nil {
			return nil, err
		}
		out = append(out, dw)
	}
	return out, nil
}

func decodeWrite(pb *firestorepb.Write) (*firestoreprovider.WriteWire, error) {
	w := &firestoreprovider.WriteWire{}
	if pb.GetCurrentDocument() != nil {
		pre, err := decodePrecondition(pb.GetCurrentDocument())
		if err != nil {
			return nil, err
		}
		w.CurrentDocument = pre
	}
	if pb.GetUpdateMask() != nil {
		w.UpdateMask = &firestoreprovider.DocumentMaskWire{FieldPaths: pb.GetUpdateMask().GetFieldPaths()}
	}
	for _, ft := range pb.GetUpdateTransforms() {
		t, err := decodeFieldTransform(ft)
		if err != nil {
			return nil, err
		}
		w.UpdateTransforms = append(w.UpdateTransforms, t)
	}
	switch op := pb.Operation.(type) {
	case *firestorepb.Write_Update:
		fields, err := decodeFields(op.Update)
		if err != nil {
			return nil, err
		}
		w.Update = &firestoreprovider.DocumentWire{Name: op.Update.GetName(), Fields: fields}
	case *firestorepb.Write_Delete:
		w.Delete = op.Delete
	case *firestorepb.Write_Transform:
		tw := &firestoreprovider.DocumentTransformWire{Document: op.Transform.GetDocument()}
		for _, ft := range op.Transform.GetFieldTransforms() {
			t, err := decodeFieldTransform(ft)
			if err != nil {
				return nil, err
			}
			tw.FieldTransforms = append(tw.FieldTransforms, t)
		}
		w.Transform = tw
	default:
		return nil, fmt.Errorf("write must specify update, delete, or transform")
	}
	return w, nil
}

func decodeFieldTransform(pb *firestorepb.DocumentTransform_FieldTransform) (firestoreprovider.FieldTransformWire, error) {
	t := firestoreprovider.FieldTransformWire{FieldPath: pb.GetFieldPath()}
	switch tv := pb.TransformType.(type) {
	case *firestorepb.DocumentTransform_FieldTransform_SetToServerValue:
		t.SetToServerValue = serverValueToString(tv.SetToServerValue)
	case *firestorepb.DocumentTransform_FieldTransform_Increment:
		v, err := decodeValue(tv.Increment)
		if err != nil {
			return t, err
		}
		t.Increment = v
	case *firestorepb.DocumentTransform_FieldTransform_Maximum:
		v, err := decodeValue(tv.Maximum)
		if err != nil {
			return t, err
		}
		t.Maximum = v
	case *firestorepb.DocumentTransform_FieldTransform_Minimum:
		v, err := decodeValue(tv.Minimum)
		if err != nil {
			return t, err
		}
		t.Minimum = v
	case *firestorepb.DocumentTransform_FieldTransform_AppendMissingElements:
		av, err := decodeArrayValue(tv.AppendMissingElements)
		if err != nil {
			return t, err
		}
		t.AppendMissingElements = av
	case *firestorepb.DocumentTransform_FieldTransform_RemoveAllFromArray:
		av, err := decodeArrayValue(tv.RemoveAllFromArray)
		if err != nil {
			return t, err
		}
		t.RemoveAllFromArray = av
	default:
		return t, fmt.Errorf("transform must specify an operation")
	}
	return t, nil
}

// decodeArrayValue converts a protobuf ArrayValue into the internal store
// ArrayValue, delegating to the shared provider decoder.
func decodeArrayValue(pb *firestorepb.ArrayValue) (*firestorestore.ArrayValue, error) {
	return firestoreprovider.ArrayValueFromProto(pb)
}

func decodePrecondition(pb *firestorepb.Precondition) (*firestoreprovider.PreconditionWire, error) {
	if pb == nil {
		return nil, nil
	}
	pre := &firestoreprovider.PreconditionWire{}
	switch c := pb.ConditionType.(type) {
	case *firestorepb.Precondition_Exists:
		b := c.Exists
		pre.Exists = &b
	case *firestorepb.Precondition_UpdateTime:
		pre.UpdateTime = c.UpdateTime.AsTime().Format(time.RFC3339Nano)
	}
	return pre, nil
}

// decodeStorePrecondition converts a request-level protobuf Precondition into
// the store Precondition used by the Service's document methods (PatchDocument /
// DeleteDocument).
func decodeStorePrecondition(pb *firestorepb.Precondition) (*firestorestore.Precondition, error) {
	if pb == nil {
		return nil, nil
	}
	pre := &firestorestore.Precondition{}
	switch c := pb.ConditionType.(type) {
	case *firestorepb.Precondition_Exists:
		b := c.Exists
		pre.Exists = &b
	case *firestorepb.Precondition_UpdateTime:
		t := c.UpdateTime.AsTime()
		pre.UpdateTime = &t
	}
	return pre, nil
}

// ─── enum mappings (proto int → internal JSON string) ────────────────────────

func fieldFilterOpToString(op firestorepb.StructuredQuery_FieldFilter_Operator) string {
	switch op {
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN:
		return "LESS_THAN"
	case firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
		return "LESS_THAN_OR_EQUAL"
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN:
		return "GREATER_THAN"
	case firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		return "GREATER_THAN_OR_EQUAL"
	case firestorepb.StructuredQuery_FieldFilter_EQUAL:
		return "EQUAL"
	case firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL:
		return "NOT_EQUAL"
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		return "ARRAY_CONTAINS"
	case firestorepb.StructuredQuery_FieldFilter_IN:
		return "IN"
	case firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		return "ARRAY_CONTAINS_ANY"
	case firestorepb.StructuredQuery_FieldFilter_NOT_IN:
		return "NOT_IN"
	}
	return ""
}

func fieldFilterOpFromString(s string) firestorepb.StructuredQuery_FieldFilter_Operator {
	switch s {
	case "LESS_THAN":
		return firestorepb.StructuredQuery_FieldFilter_LESS_THAN
	case "LESS_THAN_OR_EQUAL":
		return firestorepb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL
	case "GREATER_THAN":
		return firestorepb.StructuredQuery_FieldFilter_GREATER_THAN
	case "GREATER_THAN_OR_EQUAL":
		return firestorepb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL
	case "EQUAL":
		return firestorepb.StructuredQuery_FieldFilter_EQUAL
	case "NOT_EQUAL":
		return firestorepb.StructuredQuery_FieldFilter_NOT_EQUAL
	case "ARRAY_CONTAINS":
		return firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS
	case "IN":
		return firestorepb.StructuredQuery_FieldFilter_IN
	case "ARRAY_CONTAINS_ANY":
		return firestorepb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY
	case "NOT_IN":
		return firestorepb.StructuredQuery_FieldFilter_NOT_IN
	}
	return firestorepb.StructuredQuery_FieldFilter_OPERATOR_UNSPECIFIED
}

func directionToString(d firestorepb.StructuredQuery_Direction) string {
	switch d {
	case firestorepb.StructuredQuery_DESCENDING:
		return "DESCENDING"
	default:
		return "ASCENDING"
	}
}

func directionFromString(s string) firestorepb.StructuredQuery_Direction {
	switch s {
	case "DESCENDING":
		return firestorepb.StructuredQuery_DESCENDING
	case "ASCENDING":
		return firestorepb.StructuredQuery_ASCENDING
	}
	return firestorepb.StructuredQuery_DIRECTION_UNSPECIFIED
}

func compositeOpToString(op firestorepb.StructuredQuery_CompositeFilter_Operator) string {
	switch op {
	case firestorepb.StructuredQuery_CompositeFilter_OR:
		return "OR"
	default:
		return "AND"
	}
}

func compositeOpFromString(s string) firestorepb.StructuredQuery_CompositeFilter_Operator {
	switch s {
	case "OR":
		return firestorepb.StructuredQuery_CompositeFilter_OR
	case "AND":
		return firestorepb.StructuredQuery_CompositeFilter_AND
	}
	return firestorepb.StructuredQuery_CompositeFilter_OPERATOR_UNSPECIFIED
}

func unaryOpToString(op firestorepb.StructuredQuery_UnaryFilter_Operator) string {
	switch op {
	case firestorepb.StructuredQuery_UnaryFilter_IS_NAN:
		return "IS_NAN"
	case firestorepb.StructuredQuery_UnaryFilter_IS_NULL:
		return "IS_NULL"
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		return "IS_NOT_NAN"
	case firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		return "IS_NOT_NULL"
	}
	return ""
}

func unaryOpFromString(s string) firestorepb.StructuredQuery_UnaryFilter_Operator {
	switch s {
	case "IS_NAN":
		return firestorepb.StructuredQuery_UnaryFilter_IS_NAN
	case "IS_NULL":
		return firestorepb.StructuredQuery_UnaryFilter_IS_NULL
	case "IS_NOT_NAN":
		return firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NAN
	case "IS_NOT_NULL":
		return firestorepb.StructuredQuery_UnaryFilter_IS_NOT_NULL
	}
	return firestorepb.StructuredQuery_UnaryFilter_OPERATOR_UNSPECIFIED
}

func serverValueToString(v firestorepb.DocumentTransform_FieldTransform_ServerValue) string {
	switch v {
	case firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME:
		return "REQUEST_TIME"
	}
	return ""
}

func serverValueFromString(s string) firestorepb.DocumentTransform_FieldTransform_ServerValue {
	switch s {
	case "REQUEST_TIME":
		return firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME
	}
	return firestorepb.DocumentTransform_FieldTransform_SERVER_VALUE_UNSPECIFIED
}
