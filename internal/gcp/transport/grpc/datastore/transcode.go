package datastore

import (
	"time"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/model"

	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"

	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ─── error helpers ────────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// ─── key transcoding ──────────────────────────────────────────────────────────

// keyFromProto converts a proto Key to the transport-neutral Key. The full path
// (ancestors followed by the final element) and the partition (namespace +
// database) are preserved. An incomplete final element (no id/name) is returned
// with Kind set and Complete()==false.
func keyFromProto(k *datastorepb.Key) (core.Key, error) {
	if k == nil {
		return core.Key{}, nil
	}
	out := core.Key{}
	if pid := k.GetPartitionId(); pid != nil {
		out.Namespace = pid.GetNamespaceId()
		out.Database = pid.GetDatabaseId()
	}
	path := k.GetPath()
	if len(path) == 0 {
		return out, nil
	}
	els := make([]dsstore.PathElement, 0, len(path))
	for _, el := range path {
		pe := dsstore.PathElement{Kind: el.GetKind()}
		if id, ok := el.GetIdType().(*datastorepb.Key_PathElement_Id); ok {
			pe.ID, pe.HasID = id.Id, true
		} else if name, ok := el.GetIdType().(*datastorepb.Key_PathElement_Name); ok {
			pe.Name, pe.HasName = name.Name, true
		}
		els = append(els, pe)
	}
	if err := core.ValidateKeyPath(els); err != nil {
		return core.Key{}, err
	}
	final := els[len(els)-1]
	out.Kind, out.ID, out.Name, out.HasID, out.HasName =
		final.Kind, final.ID, final.Name, final.HasID, final.HasName
	if len(els) > 1 {
		out.Ancestors = els[:len(els)-1]
	}
	return out, nil
}

// keyToProto reconstructs a proto Key (full path + partition) from a neutral
// key, tagging the partition with the owning project.
func keyToProto(k core.Key, project string) *datastorepb.Key {
	pid := &datastorepb.PartitionId{ProjectId: project}
	if k.Database != "" {
		pid.DatabaseId = k.Database
	}
	if k.Namespace != "" {
		pid.NamespaceId = k.Namespace
	}
	out := &datastorepb.Key{PartitionId: pid}
	for _, e := range k.PathElements() {
		el := &datastorepb.Key_PathElement{Kind: e.Kind}
		switch {
		case e.HasID:
			el.IdType = &datastorepb.Key_PathElement_Id{Id: e.ID}
		case e.HasName:
			el.IdType = &datastorepb.Key_PathElement_Name{Name: e.Name}
		default:
			// Incomplete key: emit the kind only (partition still tagged).
		}
		out.Path = append(out.Path, el)
	}
	return out
}

// ─── entity transcoding ───────────────────────────────────────────────────────

func entityFromProto(p *datastorepb.Entity) (dsstore.Entity, error) {
	e := dsstore.Entity{Properties: map[string]dsstore.Value{}}
	if p == nil {
		return e, nil
	}
	if k := p.GetKey(); k != nil {
		key, err := keyFromProto(k)
		if err != nil {
			return e, err
		}
		e.Kind = key.Kind
		if key.Complete() {
			e.Key = core.CanonicalKey(key)
		}
	}
	for name, v := range p.GetProperties() {
		sv, err := valueFromProto(v)
		if err != nil {
			return e, err
		}
		e.Properties[name] = sv
	}
	return e, nil
}

func entityToProto(e dsstore.Entity, project string) *datastorepb.Entity {
	props := make(map[string]*datastorepb.Value, len(e.Properties))
	for name, v := range e.Properties {
		props[name] = valueToProto(v, project)
	}
	out := &datastorepb.Entity{Properties: props}
	if e.Key != "" {
		if k, ok := core.KeyFromCanonical(e.Key); ok {
			out.Key = keyToProto(k, project)
		}
	}
	return out
}

// ─── value transcoding ────────────────────────────────────────────────────────

func valueFromProto(p *datastorepb.Value) (dsstore.Value, error) {
	if p == nil {
		return dsstore.Value{}, nil
	}
	switch v := p.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		s := "NULL_VALUE"
		return dsstore.Value{NullValue: &s}, nil
	case *datastorepb.Value_BooleanValue:
		b := v.BooleanValue
		return dsstore.Value{BooleanValue: &b}, nil
	case *datastorepb.Value_IntegerValue:
		n := v.IntegerValue
		return dsstore.Value{IntegerValue: &n}, nil
	case *datastorepb.Value_DoubleValue:
		f := v.DoubleValue
		return dsstore.Value{DoubleValue: &f}, nil
	case *datastorepb.Value_TimestampValue:
		s := v.TimestampValue.AsTime().UTC().Format(time.RFC3339Nano)
		return dsstore.Value{TimestampValue: &s}, nil
	case *datastorepb.Value_KeyValue:
		key, err := keyFromProto(v.KeyValue)
		if err != nil {
			return dsstore.Value{}, err
		}
		if !key.Complete() {
			return dsstore.Value{}, invalidArgument("key value is incomplete")
		}
		ck := core.CanonicalKey(key)
		return dsstore.Value{KeyValue: &ck}, nil
	case *datastorepb.Value_StringValue:
		s := v.StringValue
		return dsstore.Value{StringValue: &s}, nil
	case *datastorepb.Value_BlobValue:
		return dsstore.Value{BlobValue: v.BlobValue}, nil
	case *datastorepb.Value_GeoPointValue:
		g := dsstore.GeoPoint{
			Latitude:  v.GeoPointValue.GetLatitude(),
			Longitude: v.GeoPointValue.GetLongitude(),
		}
		return dsstore.Value{GeoPointValue: &g}, nil
	case *datastorepb.Value_EntityValue:
		e, err := entityFromProto(v.EntityValue)
		if err != nil {
			return dsstore.Value{}, err
		}
		return dsstore.Value{EntityValue: &e}, nil
	case *datastorepb.Value_ArrayValue:
		arr := dsstore.ArrayValue{Values: make([]dsstore.Value, 0, len(v.ArrayValue.GetValues()))}
		for _, x := range v.ArrayValue.GetValues() {
			xv, err := valueFromProto(x)
			if err != nil {
				return dsstore.Value{}, err
			}
			arr.Values = append(arr.Values, xv)
		}
		return dsstore.Value{ArrayValue: &arr}, nil
	}
	return dsstore.Value{}, nil
}

func valueToProto(v dsstore.Value, project string) *datastorepb.Value {
	switch {
	case v.NullValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
	case v.BooleanValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BooleanValue{BooleanValue: *v.BooleanValue}}
	case v.IntegerValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: *v.IntegerValue}}
	case v.DoubleValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.TimestampValue != nil:
		t, err := time.Parse(time.RFC3339Nano, *v.TimestampValue)
		if err != nil {
			t = time.Time{}
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
	case v.KeyValue != nil:
		if k, ok := core.KeyFromCanonical(*v.KeyValue); ok {
			return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: keyToProto(k, project)}}
		}
		return &datastorepb.Value{}
	case v.StringValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_StringValue{StringValue: *v.StringValue}}
	case v.BlobValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_BlobValue{BlobValue: v.BlobValue}}
	case v.GeoPointValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{
			Latitude:  v.GeoPointValue.Latitude,
			Longitude: v.GeoPointValue.Longitude,
		}}}
	case v.EntityValue != nil:
		return &datastorepb.Value{ValueType: &datastorepb.Value_EntityValue{EntityValue: entityToProto(*v.EntityValue, project)}}
	case v.ArrayValue != nil:
		arr := &datastorepb.ArrayValue{Values: make([]*datastorepb.Value, 0, len(v.ArrayValue.Values))}
		for _, x := range v.ArrayValue.Values {
			arr.Values = append(arr.Values, valueToProto(x, project))
		}
		return &datastorepb.Value{ValueType: &datastorepb.Value_ArrayValue{ArrayValue: arr}}
	}
	return &datastorepb.Value{}
}

// ─── mutation / query transcoding ─────────────────────────────────────────────

// mutationFromProto translates a proto Mutation into a neutral Mutation.
func mutationFromProto(m *datastorepb.Mutation) (core.Mutation, error) {
	out := core.Mutation{Precondition: mutationPrecondition(m)}
	switch op := m.GetOperation().(type) {
	case *datastorepb.Mutation_Insert:
		k, e, err := mutationTarget(op.Insert)
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationInsert, k, e
	case *datastorepb.Mutation_Upsert:
		k, e, err := mutationTarget(op.Upsert)
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationUpsert, k, e
	case *datastorepb.Mutation_Update:
		k, e, err := mutationTarget(op.Update)
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationUpdate, k, e
	case *datastorepb.Mutation_Delete:
		k, err := keyFromProto(op.Delete)
		if err != nil {
			return out, err
		}
		out.Op, out.DeleteKey = core.MutationDelete, k
	default:
		return out, invalidArgument("mutation has no operation")
	}
	return out, nil
}

// withDatabase applies a request's database id to a mutation's keys that do not
// already carry one. The official SDK sets both the request field and each key's
// partition; a raw stub may set only the request field.
func withDatabase(m core.Mutation, database string) core.Mutation {
	if database == "" {
		return m
	}
	if m.Key.Database == "" {
		m.Key.Database = database
	}
	if m.DeleteKey.Database == "" {
		m.DeleteKey.Database = database
	}
	return m
}

// mutationTarget parses an insert/upsert/update entity's neutral key and
// properties. The neutral key carries any ancestors and partition — needed to
// allocate an ID for an incomplete key — while the entity carries the
// properties.
func mutationTarget(p *datastorepb.Entity) (core.Key, dsstore.Entity, error) {
	e, err := entityFromProto(p)
	if err != nil {
		return core.Key{}, dsstore.Entity{}, err
	}
	k, err := keyFromProto(p.GetKey())
	if err != nil {
		return core.Key{}, dsstore.Entity{}, err
	}
	return k, e, nil
}

// mutationPrecondition translates a Mutation's conflict_detection_strategy
// oneof (base_version or update_time) into a store Precondition. Returns nil
// when the mutation carries neither (the common case — no precondition).
func mutationPrecondition(m *datastorepb.Mutation) *dsstore.Precondition {
	switch v := m.GetConflictDetectionStrategy().(type) {
	case *datastorepb.Mutation_BaseVersion:
		bv := v.BaseVersion
		return &dsstore.Precondition{BaseVersion: &bv}
	case *datastorepb.Mutation_UpdateTime:
		ut := v.UpdateTime.AsTime()
		return &dsstore.Precondition{UpdateTime: &ut}
	default:
		return nil
	}
}

// queryFromProto translates a proto Query into a neutral Query. A nil proto
// query yields a neutral zero query (kind "", unbounded), matching the
// emulator's historical treatment of a present-but-unset query field.
func queryFromProto(q *datastorepb.Query) (*core.Query, error) {
	out := &core.Query{Offset: int(q.GetOffset())}
	if len(q.GetKind()) > 0 {
		out.Kind = q.GetKind()[0].GetName()
	}
	if q.GetLimit() != nil {
		l := int(q.GetLimit().GetValue())
		out.Limit = &l
	}
	f, err := filterFromProto(q.GetFilter())
	if err != nil {
		return nil, err
	}
	out.Filter = f
	return out, nil
}

func filterFromProto(f *datastorepb.Filter) (*core.Filter, error) {
	if f == nil {
		return nil, nil
	}
	switch ft := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		pf := ft.PropertyFilter
		if pf == nil || pf.GetProperty() == nil {
			return &core.Filter{Property: &core.PropertyFilter{}}, nil
		}
		val, err := valueFromProto(pf.GetValue())
		if err != nil {
			return nil, err
		}
		return &core.Filter{Property: &core.PropertyFilter{
			Property: pf.GetProperty().GetName(),
			Op:       propertyOpFromProto(pf.GetOp()),
			Value:    val,
		}}, nil
	case *datastorepb.Filter_CompositeFilter:
		cf := ft.CompositeFilter
		if cf == nil {
			// A composite filter with no message matches everything, matching
			// the emulator's historical behavior.
			return nil, nil
		}
		subs := make([]*core.Filter, 0, len(cf.GetFilters()))
		for _, s := range cf.GetFilters() {
			sf, err := filterFromProto(s)
			if err != nil {
				return nil, err
			}
			subs = append(subs, sf)
		}
		return &core.Filter{Composite: &core.CompositeFilter{Op: compositeOpFromProto(cf.GetOp()), Filters: subs}}, nil
	default:
		return &core.Filter{}, nil
	}
}

func propertyOpFromProto(op datastorepb.PropertyFilter_Operator) core.PropertyOp {
	switch op {
	case datastorepb.PropertyFilter_EQUAL:
		return core.PropertyEqual
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return core.PropertyNotEqual
	case datastorepb.PropertyFilter_IN:
		return core.PropertyIn
	case datastorepb.PropertyFilter_LESS_THAN:
		return core.PropertyLessThan
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return core.PropertyLessThanOrEqual
	case datastorepb.PropertyFilter_GREATER_THAN:
		return core.PropertyGreaterThan
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return core.PropertyGreaterThanOrEqual
	case datastorepb.PropertyFilter_HAS_ANCESTOR:
		return core.PropertyHasAncestor
	default:
		return core.PropertyUnspecified
	}
}

func compositeOpFromProto(op datastorepb.CompositeFilter_Operator) core.CompositeOp {
	switch op {
	case datastorepb.CompositeFilter_AND:
		return core.CompositeAnd
	case datastorepb.CompositeFilter_OR:
		return core.CompositeOr
	default:
		return core.CompositeUnspecified
	}
}

// gqlQueryFromProto converts a proto GqlQuery into the transport-neutral
// GQLQuery. A binding carries either an inline value or an opaque cursor; the
// core rejects a cursor binding when it is referenced.
func gqlQueryFromProto(q *datastorepb.GqlQuery) (core.GQLQuery, error) {
	out := core.GQLQuery{
		QueryString:   q.GetQueryString(),
		AllowLiterals: q.GetAllowLiterals(),
	}
	if named := q.GetNamedBindings(); len(named) > 0 {
		out.NamedBindings = make(map[string]core.GQLBinding, len(named))
		for name, b := range named {
			gb, err := gqlBindingFromProto(b)
			if err != nil {
				return core.GQLQuery{}, err
			}
			out.NamedBindings[name] = gb
		}
	}
	if positional := q.GetPositionalBindings(); len(positional) > 0 {
		out.PositionalBindings = make([]core.GQLBinding, 0, len(positional))
		for _, b := range positional {
			gb, err := gqlBindingFromProto(b)
			if err != nil {
				return core.GQLQuery{}, err
			}
			out.PositionalBindings = append(out.PositionalBindings, gb)
		}
	}
	return out, nil
}

func gqlBindingFromProto(b *datastorepb.GqlQueryParameter) (core.GQLBinding, error) {
	if b == nil {
		return core.GQLBinding{}, nil
	}
	switch pt := b.GetParameterType().(type) {
	case *datastorepb.GqlQueryParameter_Value:
		v, err := valueFromProto(pt.Value)
		if err != nil {
			return core.GQLBinding{}, err
		}
		return core.GQLBinding{Value: &v}, nil
	case *datastorepb.GqlQueryParameter_Cursor:
		return core.GQLBinding{Cursor: pt.Cursor}, nil
	default:
		return core.GQLBinding{}, invalidArgument("GQL binding has no value")
	}
}

// aggregationQueryFromProto translates a proto AggregationQuery into a neutral
// one. A non-nil query without a nested query is rejected (the wire requires
// the nested query).
func aggregationQueryFromProto(aq *datastorepb.AggregationQuery) (*core.AggregationQuery, error) {
	if aq == nil {
		return nil, nil
	}
	nested := aq.GetNestedQuery()
	if nested == nil {
		return nil, invalidArgument("aggregation query is missing its nested query")
	}
	nq, err := queryFromProto(nested)
	if err != nil {
		return nil, err
	}
	aggs := make([]core.Aggregation, 0, len(aq.GetAggregations()))
	for _, a := range aq.GetAggregations() {
		aggs = append(aggs, aggregationFromProto(a))
	}
	return &core.AggregationQuery{Nested: *nq, Aggregations: aggs}, nil
}

func aggregationFromProto(a *datastorepb.AggregationQuery_Aggregation) core.Aggregation {
	out := core.Aggregation{Alias: a.GetAlias()}
	switch op := a.GetOperator().(type) {
	case *datastorepb.AggregationQuery_Aggregation_Count_:
		out.Op = core.AggCount
		if upTo := op.Count.GetUpTo(); upTo != nil {
			v := upTo.GetValue()
			out.UpTo = &v
		}
	case *datastorepb.AggregationQuery_Aggregation_Sum_:
		out.Op = core.AggSum
		out.Property = op.Sum.GetProperty().GetName()
	case *datastorepb.AggregationQuery_Aggregation_Avg_:
		out.Op = core.AggAvg
		out.Property = op.Avg.GetProperty().GetName()
	default:
		out.Op = core.AggUnspecified
	}
	return out
}
