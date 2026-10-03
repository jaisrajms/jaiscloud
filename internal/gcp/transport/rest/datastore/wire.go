package datastore

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"jaiscloud/internal/model"

	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// ─── request helpers ──────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}

// int64FromWire parses a JSON int64, which Datastore encodes as a decimal
// string (protojson) but which may also arrive as a plain number.
func int64FromWire(v any) (int64, error) {
	switch x := v.(type) {
	case string:
		return strconv.ParseInt(x, 10, 64)
	case float64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("not an int64")
	}
}

func numberFromWire(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

// bytesFromWire decodes a base64-encoded bytes field (e.g. transaction).
func bytesFromWire(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, invalidArgument("invalid transaction")
	}
	return b, nil
}

func transactionFromReadOptions(v any) ([]byte, error) {
	m, _ := v.(map[string]any)
	if m == nil {
		return nil, nil
	}
	return bytesFromWire(m["transaction"])
}

func keysFromWire(v any) ([]core.Key, error) {
	raws, _ := v.([]any)
	out := make([]core.Key, 0, len(raws))
	for _, raw := range raws {
		k, err := keyFromWire(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, nil
}

// ─── key ──────────────────────────────────────────────────────────────────────

func keyFromWire(v any) (core.Key, error) {
	if v == nil {
		return core.Key{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return core.Key{}, invalidArgument("malformed key")
	}
	out := core.Key{}
	if pid, ok := m["partitionId"].(map[string]any); ok && pid != nil {
		out.Namespace = strFrom(pid["namespaceId"])
		out.Database = strFrom(pid["databaseId"])
	}
	raws, _ := m["path"].([]any)
	if len(raws) == 0 {
		return out, nil
	}
	els := make([]dsstore.PathElement, 0, len(raws))
	for _, raw := range raws {
		el, ok := raw.(map[string]any)
		if !ok {
			return core.Key{}, invalidArgument("malformed key path element")
		}
		pe := dsstore.PathElement{Kind: strFrom(el["kind"])}
		if idv, ok := el["id"]; ok {
			id, err := int64FromWire(idv)
			if err != nil {
				return core.Key{}, invalidArgument("invalid key id")
			}
			pe.ID, pe.HasID = id, true
		} else if namev, ok := el["name"]; ok {
			pe.Name, pe.HasName = strFrom(namev), true
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

func keyToWire(k core.Key, project string) map[string]any {
	pid := map[string]any{}
	if project != "" {
		pid["projectId"] = project
	}
	if k.Database != "" {
		pid["databaseId"] = k.Database
	}
	if k.Namespace != "" {
		pid["namespaceId"] = k.Namespace
	}
	path := make([]any, 0, len(k.PathElements()))
	for _, e := range k.PathElements() {
		el := map[string]any{"kind": e.Kind}
		switch {
		case e.HasID:
			el["id"] = strconv.FormatInt(e.ID, 10)
		case e.HasName:
			el["name"] = e.Name
		}
		path = append(path, el)
	}
	return map[string]any{"partitionId": pid, "path": path}
}

// ─── value ────────────────────────────────────────────────────────────────────

func valueFromWire(v any) (dsstore.Value, error) {
	if v == nil {
		return dsstore.Value{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return dsstore.Value{}, invalidArgument("malformed value")
	}
	switch {
	case hasKey(m, "nullValue"):
		s := "NULL_VALUE"
		return dsstore.Value{NullValue: &s}, nil
	case hasKey(m, "booleanValue"):
		b, _ := m["booleanValue"].(bool)
		return dsstore.Value{BooleanValue: &b}, nil
	case hasKey(m, "integerValue"):
		n, err := int64FromWire(m["integerValue"])
		if err != nil {
			return dsstore.Value{}, invalidArgument("invalid integerValue")
		}
		return dsstore.Value{IntegerValue: &n}, nil
	case hasKey(m, "doubleValue"):
		f, ok := numberFromWire(m["doubleValue"])
		if !ok {
			return dsstore.Value{}, invalidArgument("invalid doubleValue")
		}
		return dsstore.Value{DoubleValue: &f}, nil
	case hasKey(m, "timestampValue"):
		// Preserve the wire string verbatim (the store compares/orders by its
		// RFC 3339 form).
		s := strFrom(m["timestampValue"])
		return dsstore.Value{TimestampValue: &s}, nil
	case hasKey(m, "keyValue"):
		k, err := keyFromWire(m["keyValue"])
		if err != nil {
			return dsstore.Value{}, err
		}
		if !k.Complete() {
			return dsstore.Value{}, invalidArgument("key value is incomplete")
		}
		ck := core.CanonicalKey(k)
		return dsstore.Value{KeyValue: &ck}, nil
	case hasKey(m, "stringValue"):
		s := strFrom(m["stringValue"])
		return dsstore.Value{StringValue: &s}, nil
	case hasKey(m, "blobValue"):
		b, err := base64.StdEncoding.DecodeString(strFrom(m["blobValue"]))
		if err != nil {
			return dsstore.Value{}, invalidArgument("invalid blobValue")
		}
		return dsstore.Value{BlobValue: b}, nil
	case hasKey(m, "geoPointValue"):
		gm, _ := m["geoPointValue"].(map[string]any)
		lat, _ := numberFromWire(gm["latitude"])
		lon, _ := numberFromWire(gm["longitude"])
		return dsstore.Value{GeoPointValue: &dsstore.GeoPoint{Latitude: lat, Longitude: lon}}, nil
	case hasKey(m, "entityValue"):
		e, err := entityFromWire(m["entityValue"])
		if err != nil {
			return dsstore.Value{}, err
		}
		return dsstore.Value{EntityValue: &e}, nil
	case hasKey(m, "arrayValue"):
		am, _ := m["arrayValue"].(map[string]any)
		raws, _ := am["values"].([]any)
		arr := dsstore.ArrayValue{Values: make([]dsstore.Value, 0, len(raws))}
		for _, x := range raws {
			xv, err := valueFromWire(x)
			if err != nil {
				return dsstore.Value{}, err
			}
			arr.Values = append(arr.Values, xv)
		}
		return dsstore.Value{ArrayValue: &arr}, nil
	}
	return dsstore.Value{}, invalidArgument("value has no variant")
}

func valueToWire(v dsstore.Value, project string) map[string]any {
	switch {
	case v.NullValue != nil:
		return map[string]any{"nullValue": "NULL_VALUE"}
	case v.BooleanValue != nil:
		return map[string]any{"booleanValue": *v.BooleanValue}
	case v.IntegerValue != nil:
		return map[string]any{"integerValue": strconv.FormatInt(*v.IntegerValue, 10)}
	case v.DoubleValue != nil:
		return map[string]any{"doubleValue": *v.DoubleValue}
	case v.TimestampValue != nil:
		return map[string]any{"timestampValue": *v.TimestampValue}
	case v.KeyValue != nil:
		if k, ok := core.KeyFromCanonical(*v.KeyValue); ok {
			return map[string]any{"keyValue": keyToWire(k, project)}
		}
		return map[string]any{}
	case v.StringValue != nil:
		return map[string]any{"stringValue": *v.StringValue}
	case v.BlobValue != nil:
		return map[string]any{"blobValue": base64.StdEncoding.EncodeToString(v.BlobValue)}
	case v.GeoPointValue != nil:
		return map[string]any{"geoPointValue": map[string]any{
			"latitude":  v.GeoPointValue.Latitude,
			"longitude": v.GeoPointValue.Longitude,
		}}
	case v.EntityValue != nil:
		return map[string]any{"entityValue": entityToWire(*v.EntityValue, project)}
	case v.ArrayValue != nil:
		vals := make([]any, 0, len(v.ArrayValue.Values))
		for _, x := range v.ArrayValue.Values {
			vals = append(vals, valueToWire(x, project))
		}
		return map[string]any{"arrayValue": map[string]any{"values": vals}}
	}
	return map[string]any{}
}

// ─── entity ───────────────────────────────────────────────────────────────────

func entityFromWire(v any) (dsstore.Entity, error) {
	e := dsstore.Entity{Properties: map[string]dsstore.Value{}}
	if v == nil {
		return e, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return e, invalidArgument("malformed entity")
	}
	if kv, ok := m["key"]; ok && kv != nil {
		k, err := keyFromWire(kv)
		if err != nil {
			return e, err
		}
		e.Kind = k.Kind
		if k.Complete() {
			e.Key = core.CanonicalKey(k)
		}
	}
	if props, ok := m["properties"].(map[string]any); ok {
		for name, pv := range props {
			sv, err := valueFromWire(pv)
			if err != nil {
				return e, err
			}
			e.Properties[name] = sv
		}
	}
	return e, nil
}

func entityToWire(e dsstore.Entity, project string) map[string]any {
	props := make(map[string]any, len(e.Properties))
	for name, v := range e.Properties {
		props[name] = valueToWire(v, project)
	}
	out := map[string]any{"properties": props}
	if e.Key != "" {
		if k, ok := core.KeyFromCanonical(e.Key); ok {
			out["key"] = keyToWire(k, project)
		}
	}
	return out
}

// ─── mutations ────────────────────────────────────────────────────────────────

func mutationsFromWire(v any) ([]core.Mutation, error) {
	raws, _ := v.([]any)
	out := make([]core.Mutation, 0, len(raws))
	for _, raw := range raws {
		m, ok := raw.(map[string]any)
		if !ok {
			return nil, invalidArgument("malformed mutation")
		}
		cm, err := mutationFromWire(m)
		if err != nil {
			return nil, err
		}
		out = append(out, cm)
	}
	return out, nil
}

func mutationFromWire(m map[string]any) (core.Mutation, error) {
	out := core.Mutation{Precondition: preconditionFromWire(m)}
	switch {
	case hasKey(m, "insert"):
		k, e, err := mutationTargetWire(m["insert"])
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationInsert, k, e
	case hasKey(m, "upsert"):
		k, e, err := mutationTargetWire(m["upsert"])
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationUpsert, k, e
	case hasKey(m, "update"):
		k, e, err := mutationTargetWire(m["update"])
		if err != nil {
			return out, err
		}
		out.Op, out.Key, out.Entity = core.MutationUpdate, k, e
	case hasKey(m, "delete"):
		k, err := keyFromWire(m["delete"])
		if err != nil {
			return out, err
		}
		out.Op, out.DeleteKey = core.MutationDelete, k
	default:
		return out, invalidArgument("mutation has no operation")
	}
	return out, nil
}

// mutationTargetWire parses an insert/upsert/update entity's neutral key and
// properties. The neutral key carries any ancestors and partition (needed to
// allocate an ID for an incomplete key); the entity carries the properties.
func mutationTargetWire(v any) (core.Key, dsstore.Entity, error) {
	e, err := entityFromWire(v)
	if err != nil {
		return core.Key{}, dsstore.Entity{}, err
	}
	var kv any
	if em, ok := v.(map[string]any); ok {
		kv = em["key"]
	}
	k, err := keyFromWire(kv)
	if err != nil {
		return core.Key{}, dsstore.Entity{}, err
	}
	return k, e, nil
}

// withDatabase applies a request's database id to a mutation's keys that do not
// already carry one (the SDK sets both; a raw caller may set only the request
// field).
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

func preconditionFromWire(m map[string]any) *dsstore.Precondition {
	if bv, ok := m["baseVersion"]; ok && bv != nil {
		if n, err := int64FromWire(bv); err == nil {
			return &dsstore.Precondition{BaseVersion: &n}
		}
	}
	if ut := strFrom(m["updateTime"]); ut != "" {
		if t, err := time.Parse(time.RFC3339Nano, ut); err == nil {
			return &dsstore.Precondition{UpdateTime: &t}
		}
	}
	return nil
}

// ─── query / filter / aggregation ─────────────────────────────────────────────

func queryFromWire(v any) (*core.Query, error) {
	out := &core.Query{}
	if v == nil {
		return out, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, invalidArgument("malformed query")
	}
	if kinds, ok := m["kind"].([]any); ok && len(kinds) > 0 {
		if km, ok := kinds[0].(map[string]any); ok {
			out.Kind = strFrom(km["name"])
		}
	}
	if off, ok := numberFromWire(m["offset"]); ok {
		out.Offset = int(off)
	}
	if lf, ok := numberFromWire(m["limit"]); ok {
		l := int(lf)
		out.Limit = &l
	}
	f, err := filterFromWire(m["filter"])
	if err != nil {
		return nil, err
	}
	out.Filter = f
	return out, nil
}

// gqlQueryFromWire converts a Discovery-shaped gqlQuery body into the
// transport-neutral GQLQuery. A binding carries either an inline value or a
// base64 cursor; the core rejects a cursor binding when it is referenced.
func gqlQueryFromWire(v any) (core.GQLQuery, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return core.GQLQuery{}, invalidArgument("malformed gqlQuery")
	}
	allowLiterals, _ := m["allowLiterals"].(bool)
	out := core.GQLQuery{
		QueryString:   strFrom(m["queryString"]),
		AllowLiterals: allowLiterals,
	}
	if named, ok := m["namedBindings"].(map[string]any); ok && len(named) > 0 {
		out.NamedBindings = make(map[string]core.GQLBinding, len(named))
		for name, raw := range named {
			gb, err := gqlBindingFromWire(raw)
			if err != nil {
				return core.GQLQuery{}, err
			}
			out.NamedBindings[name] = gb
		}
	}
	if positional, ok := m["positionalBindings"].([]any); ok && len(positional) > 0 {
		out.PositionalBindings = make([]core.GQLBinding, 0, len(positional))
		for _, raw := range positional {
			gb, err := gqlBindingFromWire(raw)
			if err != nil {
				return core.GQLQuery{}, err
			}
			out.PositionalBindings = append(out.PositionalBindings, gb)
		}
	}
	return out, nil
}

func gqlBindingFromWire(v any) (core.GQLBinding, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return core.GQLBinding{}, invalidArgument("malformed GQL binding")
	}
	if raw, ok := m["value"]; ok && raw != nil {
		val, err := valueFromWire(raw)
		if err != nil {
			return core.GQLBinding{}, err
		}
		return core.GQLBinding{Value: &val}, nil
	}
	if raw, ok := m["cursor"]; ok {
		cur, err := base64.StdEncoding.DecodeString(strFrom(raw))
		if err != nil {
			return core.GQLBinding{}, invalidArgument("invalid GQL cursor binding")
		}
		return core.GQLBinding{Cursor: cur}, nil
	}
	return core.GQLBinding{}, invalidArgument("GQL binding has no value")
}

func filterFromWire(v any) (*core.Filter, error) {
	if v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return &core.Filter{}, nil
	}
	if pf, ok := m["propertyFilter"]; ok {
		return propertyFilterFromWire(pf)
	}
	if cf, ok := m["compositeFilter"]; ok {
		return compositeFilterFromWire(cf)
	}
	return &core.Filter{}, nil
}

func propertyFilterFromWire(v any) (*core.Filter, error) {
	pf := &core.PropertyFilter{}
	if v != nil {
		if m, ok := v.(map[string]any); ok {
			if pr, ok := m["property"].(map[string]any); ok {
				pf.Property = strFrom(pr["name"])
			}
			pf.Op = propertyOpFromWire(strFrom(m["op"]))
			val, err := valueFromWire(m["value"])
			if err != nil {
				return nil, err
			}
			pf.Value = val
		}
	}
	return &core.Filter{Property: pf}, nil
}

func compositeFilterFromWire(v any) (*core.Filter, error) {
	if v == nil {
		// A composite filter with no message matches everything (mirrors the
		// gRPC adapter).
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, nil
	}
	subs := []*core.Filter{}
	if raws, ok := m["filters"].([]any); ok {
		for _, r := range raws {
			sf, err := filterFromWire(r)
			if err != nil {
				return nil, err
			}
			subs = append(subs, sf)
		}
	}
	return &core.Filter{Composite: &core.CompositeFilter{Op: compositeOpFromWire(strFrom(m["op"])), Filters: subs}}, nil
}

func aggregationQueryFromWire(v any) (*core.AggregationQuery, error) {
	if v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, nil
	}
	nested, ok := m["nestedQuery"]
	if !ok || nested == nil {
		return nil, invalidArgument("aggregation query is missing its nested query")
	}
	nq, err := queryFromWire(nested)
	if err != nil {
		return nil, err
	}
	aggs := []core.Aggregation{}
	if raws, ok := m["aggregations"].([]any); ok {
		for _, r := range raws {
			agg, err := aggregationFromWire(r)
			if err != nil {
				return nil, err
			}
			aggs = append(aggs, agg)
		}
	}
	return &core.AggregationQuery{Nested: *nq, Aggregations: aggs}, nil
}

func aggregationFromWire(v any) (core.Aggregation, error) {
	out := core.Aggregation{}
	m, ok := v.(map[string]any)
	if !ok {
		return out, nil
	}
	out.Alias = strFrom(m["alias"])
	switch {
	case hasKey(m, "count"):
		out.Op = core.AggCount
		if cm, ok := m["count"].(map[string]any); ok {
			if up, ok := cm["upTo"]; ok && up != nil {
				n, err := int64FromWire(up)
				if err != nil {
					return out, invalidArgument("invalid count upTo")
				}
				out.UpTo = &n
			}
		}
	case hasKey(m, "sum"):
		out.Op = core.AggSum
		out.Property = propertyName(m["sum"])
	case hasKey(m, "avg"):
		out.Op = core.AggAvg
		out.Property = propertyName(m["avg"])
	default:
		out.Op = core.AggUnspecified
	}
	return out, nil
}

func propertyName(v any) string {
	m, _ := v.(map[string]any)
	pr, _ := m["property"].(map[string]any)
	return strFrom(pr["name"])
}

func propertyOpFromWire(s string) core.PropertyOp {
	switch s {
	case "EQUAL":
		return core.PropertyEqual
	case "NOT_EQUAL":
		return core.PropertyNotEqual
	case "IN":
		return core.PropertyIn
	case "LESS_THAN":
		return core.PropertyLessThan
	case "LESS_THAN_OR_EQUAL":
		return core.PropertyLessThanOrEqual
	case "GREATER_THAN":
		return core.PropertyGreaterThan
	case "GREATER_THAN_OR_EQUAL":
		return core.PropertyGreaterThanOrEqual
	case "HAS_ANCESTOR":
		return core.PropertyHasAncestor
	default:
		return core.PropertyUnspecified
	}
}

func compositeOpFromWire(s string) core.CompositeOp {
	switch s {
	case "AND":
		return core.CompositeAnd
	case "OR":
		return core.CompositeOr
	default:
		return core.CompositeUnspecified
	}
}
