package datastore

import (
	"encoding/base64"
	"strconv"
	"time"

	core "jaiscloud/internal/gcp/service/datastore"
)

// The functions below expose the wire conversion in both directions to callers that
// translate between the REST shape and the transport-neutral model (the console,
// when a plugin owns the Datastore keys). They are the inverse of the *FromWire
// decoders the handlers use, and share valueToWire/keyToWire/entityToWire with the
// response encoders, so a request encoded here is decoded by the handlers unchanged.

// KeyToWire encodes a key.
func KeyToWire(k core.Key, project string) map[string]any { return keyToWire(k, project) }

// QueryToWire encodes a structured query.
func QueryToWire(q *core.Query, project string) map[string]any {
	out := map[string]any{}
	if q == nil {
		return out
	}
	if q.Kind != "" {
		out["kind"] = []any{map[string]any{"name": q.Kind}}
	}
	if q.Filter != nil {
		if f := filterToWire(q.Filter, project); f != nil {
			out["filter"] = f
		}
	}
	if q.Offset > 0 {
		out["offset"] = float64(q.Offset)
	}
	if q.Limit != nil {
		out["limit"] = float64(*q.Limit)
	}
	if len(q.StartCursor) > 0 {
		out["startCursor"] = base64.StdEncoding.EncodeToString(q.StartCursor)
	}
	if len(q.EndCursor) > 0 {
		out["endCursor"] = base64.StdEncoding.EncodeToString(q.EndCursor)
	}
	if q.Projection != nil && len(q.Projection.Properties) > 0 {
		projs := make([]any, 0, len(q.Projection.Properties))
		for _, p := range q.Projection.Properties {
			projs = append(projs, map[string]any{"property": map[string]any{"name": p}})
		}
		out["projection"] = projs
	}
	return out
}

func filterToWire(f *core.Filter, project string) map[string]any {
	switch {
	case f == nil:
		return nil
	case f.Property != nil:
		return map[string]any{"propertyFilter": map[string]any{
			"property": map[string]any{"name": f.Property.Property},
			"op":       propertyOpToWire(f.Property.Op),
			"value":    valueToWire(f.Property.Value, project),
		}}
	case f.Composite != nil:
		subs := make([]any, 0, len(f.Composite.Filters))
		for _, s := range f.Composite.Filters {
			if w := filterToWire(s, project); w != nil {
				subs = append(subs, w)
			}
		}
		return map[string]any{"compositeFilter": map[string]any{"op": compositeOpToWire(f.Composite.Op), "filters": subs}}
	}
	return nil
}

func propertyOpToWire(op core.PropertyOp) string {
	switch op {
	case core.PropertyEqual:
		return "EQUAL"
	case core.PropertyNotEqual:
		return "NOT_EQUAL"
	case core.PropertyIn:
		return "IN"
	case core.PropertyLessThan:
		return "LESS_THAN"
	case core.PropertyLessThanOrEqual:
		return "LESS_THAN_OR_EQUAL"
	case core.PropertyGreaterThan:
		return "GREATER_THAN"
	case core.PropertyGreaterThanOrEqual:
		return "GREATER_THAN_OR_EQUAL"
	case core.PropertyHasAncestor:
		return "HAS_ANCESTOR"
	}
	return "OPERATOR_UNSPECIFIED"
}

func compositeOpToWire(op core.CompositeOp) string {
	switch op {
	case core.CompositeAnd:
		return "AND"
	case core.CompositeOr:
		return "OR"
	}
	return "OPERATOR_UNSPECIFIED"
}

// GQLQueryToWire encodes a GQL query with its bindings.
func GQLQueryToWire(q core.GQLQuery, project string) map[string]any {
	out := map[string]any{"queryString": q.QueryString}
	if q.AllowLiterals {
		out["allowLiterals"] = true
	}
	binding := func(b core.GQLBinding) map[string]any {
		if b.Value != nil {
			return map[string]any{"value": valueToWire(*b.Value, project)}
		}
		return map[string]any{"cursor": base64.StdEncoding.EncodeToString(b.Cursor)}
	}
	if len(q.NamedBindings) > 0 {
		named := make(map[string]any, len(q.NamedBindings))
		for n, b := range q.NamedBindings {
			named[n] = binding(b)
		}
		out["namedBindings"] = named
	}
	if len(q.PositionalBindings) > 0 {
		pos := make([]any, 0, len(q.PositionalBindings))
		for _, b := range q.PositionalBindings {
			pos = append(pos, binding(b))
		}
		out["positionalBindings"] = pos
	}
	return out
}

// MutationToWire encodes one mutation.
func MutationToWire(m core.Mutation, project string) map[string]any {
	out := map[string]any{}
	target := func(op string) {
		e := entityToWire(m.Entity, project)
		e["key"] = keyToWire(m.Key, project)
		out[op] = e
	}
	switch m.Op {
	case core.MutationInsert:
		target("insert")
	case core.MutationUpsert:
		target("upsert")
	case core.MutationUpdate:
		target("update")
	case core.MutationDelete:
		out["delete"] = keyToWire(m.DeleteKey, project)
	}
	if m.Precondition != nil {
		if m.Precondition.BaseVersion != nil {
			out["baseVersion"] = strconv.FormatInt(*m.Precondition.BaseVersion, 10)
		}
		if m.Precondition.UpdateTime != nil {
			out["updateTime"] = core.CanonicalTimestamp(*m.Precondition.UpdateTime)
		}
	}
	return out
}

// CommitModeToWire names a commit mode ("" for the unspecified one).
func CommitModeToWire(m core.CommitMode) string {
	switch m {
	case core.CommitModeTransactional:
		return "TRANSACTIONAL"
	case core.CommitModeNonTransactional:
		return "NON_TRANSACTIONAL"
	}
	return ""
}

// ---- answers ---------------------------------------------------------------------

func parseTime(m map[string]any, field string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, strFrom(m[field]))
	return t
}

func entityResultFromWire(m map[string]any) (core.EntityResult, error) {
	e, err := entityFromWire(m["entity"])
	if err != nil {
		return core.EntityResult{}, err
	}
	r := core.EntityResult{Entity: e, CreateTime: parseTime(m, "createTime")}
	r.Version, _ = int64FromWire(m["version"])
	if c := strFrom(m["cursor"]); c != "" {
		r.Cursor, _ = base64.StdEncoding.DecodeString(c)
	}
	return r, nil
}

// QueryResultFromWire decodes a runQuery answer's batch.
func QueryResultFromWire(answer map[string]any) (*core.QueryResult, error) {
	batch, _ := answer["batch"].(map[string]any)
	out := &core.QueryResult{ReadTime: parseTime(batch, "readTime")}
	switch strFrom(batch["entityResultType"]) {
	case "PROJECTION":
		out.ResultType = core.ResultProjection
	case "KEY_ONLY":
		out.ResultType = core.ResultKeysOnly
	}
	switch strFrom(batch["moreResults"]) {
	case "MORE_RESULTS_AFTER_LIMIT":
		out.MoreResults = core.MoreResultsAfterLimit
	case "MORE_RESULTS_AFTER_CURSOR":
		out.MoreResults = core.MoreResultsAfterCursor
	}
	out.SnapshotVersion, _ = int64FromWire(batch["snapshotVersion"])
	if n, ok := numberFromWire(batch["skippedResults"]); ok {
		out.Skipped = int(n)
	}
	out.EndCursor, _ = base64.StdEncoding.DecodeString(strFrom(batch["endCursor"]))
	out.SkippedCursor, _ = base64.StdEncoding.DecodeString(strFrom(batch["skippedCursor"]))
	if ers, ok := batch["entityResults"].([]any); ok {
		for _, raw := range ers {
			m, _ := raw.(map[string]any)
			r, err := entityResultFromWire(m)
			if err != nil {
				return nil, err
			}
			out.Entities = append(out.Entities, r)
		}
	}
	return out, nil
}

// LookupResultFromWire decodes a lookup answer.
func LookupResultFromWire(answer map[string]any) (*core.LookupResult, error) {
	out := &core.LookupResult{ReadTime: parseTime(answer, "readTime")}
	if found, ok := answer["found"].([]any); ok {
		for _, raw := range found {
			m, _ := raw.(map[string]any)
			r, err := entityResultFromWire(m)
			if err != nil {
				return nil, err
			}
			out.Found = append(out.Found, r)
		}
	}
	if missing, ok := answer["missing"].([]any); ok {
		for _, raw := range missing {
			m, _ := raw.(map[string]any)
			em, _ := m["entity"].(map[string]any)
			k, err := keyFromWire(em["key"])
			if err != nil {
				return nil, err
			}
			out.Missing = append(out.Missing, k)
		}
	}
	return out, nil
}

// CommitResponseFromWire decodes a commit answer.
func CommitResponseFromWire(answer map[string]any) (*core.CommitResponse, error) {
	out := &core.CommitResponse{CommitTime: parseTime(answer, "commitTime")}
	if n, ok := numberFromWire(answer["indexUpdates"]); ok {
		out.IndexUpdates = int32(n)
	}
	if results, ok := answer["mutationResults"].([]any); ok {
		for _, raw := range results {
			m, _ := raw.(map[string]any)
			r := core.MutationResult{CreateTime: parseTime(m, "createTime"), UpdateTime: parseTime(m, "updateTime")}
			r.Version, _ = int64FromWire(m["version"])
			r.ConflictDetected, _ = m["conflictDetected"].(bool)
			if kw, ok := m["key"]; ok {
				k, err := keyFromWire(kw)
				if err != nil {
					return nil, err
				}
				r.Key = &k
			}
			out.Results = append(out.Results, r)
		}
	}
	return out, nil
}
