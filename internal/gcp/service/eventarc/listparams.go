package eventarc

// This file wires the documented `filter`/`orderBy` request parameters of the
// Eventarc list methods the transport layer used to drop: providers.list,
// messageBuses.list, enrollments.list, pipelines.list and googleApiSources.list
// (both parameters) and channels.list (orderBy). triggers.list already honored
// them (triggerlist.go). Real Eventarc documents both on those methods
// (eventarc v1 Discovery), so a client that sorts or filters must not be handed
// an unfiltered, name-ascending list: anything outside the supported grammar is
// INVALID_ARGUMENT rather than silently ignored.
//
// A filter is AIP-160 — the shared tokenizer/sequencer lives in
// internal/gcp/aip160 — over the identity, label, displayName and timestamp
// fields each resource's schema defines. An orderBy is a comma-separated field
// list with an optional asc/desc direction; the shared aip160.OrderSpec also
// carries the composite page cursor, so paging a filtered/ordered list never
// resumes in a different order.

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// ─── filter ───────────────────────────────────────────────────────────────────

// listPredicate is one compiled list filter clause over an item type T.
type listPredicate[T any] func(T) bool

// listFilter is a conjunction of compiled clauses. An empty filter matches
// every item.
type listFilter[T any] struct {
	preds []listPredicate[T]
}

// match reports whether v satisfies every clause.
func (f listFilter[T]) match(v T) bool {
	for _, p := range f.preds {
		if !p(v) {
			return false
		}
	}
	return true
}

// foldListName normalizes a filter field spelling for lookup: AIP-160 names
// fields in snake_case while the JSON/proto shape is camelCase, so both
// spellings (and any case) resolve to the same key — the same folding the
// shared aip160.OrderSpec applies to orderBy fields.
func foldListName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// filterFields is one list surface's filterable field set. A nil accessor means
// the surface does not accept that field group (e.g. a provider has no uid or
// labels, and the channelConnections list method documents no filter at all).
// desc names the accepted keys for the error a rejected clause returns.
type filterFields[T any] struct {
	desc       string
	name       func(T) string
	uid        func(T) string               // nil when the resource has no uid
	labels     func(T) map[string]string    // nil when the resource has no labels
	strFields  map[string]func(T) string    // displayName etc., keyed by folded name
	timeFields map[string]func(T) time.Time // createTime/updateTime, keyed by folded name
}

// compileListFilter parses one surface's bounded AIP-160 filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	op     := "=" | "!=" | ":"            (":" only on identity/label fields)
//	value  := <ident> | <double-quoted string> | "*"
//
// Anything outside the grammar — OR/NOT, parenthesis groups, an unknown field,
// a comparison on a string field — is INVALID_ARGUMENT, so a filter is never
// silently ignored and never returns an unfiltered list to a client that asked
// for a subset. labelPrefix is prepended to the error messages so a client can
// tell which list method rejected the clause.
func compileListFilter[T any](label, s string, fields filterFields[T]) (listFilter[T], error) {
	if strings.TrimSpace(s) == "" {
		return listFilter[T]{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return listFilter[T]{}, invalidArgument(label + " filter: " + err.Error())
	}
	clause := func(p *aip160.Parser) (any, error) { return parseListClause(p, fields) }
	preds, err := aip160.NewParser(toks).ParseClauses(clause)
	if err != nil {
		return listFilter[T]{}, invalidArgument(label + " filter: " + err.Error())
	}
	f := listFilter[T]{preds: make([]listPredicate[T], 0, len(preds))}
	for _, p := range preds {
		pred, ok := p.(listPredicate[T])
		if !ok {
			return listFilter[T]{}, invalidArgument(label + " filter: internal clause type")
		}
		f.preds = append(f.preds, pred)
	}
	return f, nil
}

// parseListClause parses one filter clause (`field op value`) into the matching
// listPredicate, returned as an `any` for the shared clause sequencer.
func parseListClause[T any](p *aip160.Parser, fields filterFields[T]) (any, error) {
	keyTok := p.Next()
	if keyTok.Kind != aip160.KindIdent {
		return nil, fmt.Errorf("expected field, got %s", aip160.TokenText(keyTok))
	}
	key := keyTok.Text
	opTok := p.Next()
	if opTok.Kind != aip160.KindOp {
		return nil, fmt.Errorf("expected operator after %q, got %s", key, aip160.TokenText(opTok))
	}
	op := opTok.Text
	folded := foldListName(key)

	switch {
	case folded == "name":
		return parseListStringClause(p, key, op, fields.name)

	case folded == "uid" && fields.uid != nil:
		return parseListStringClause(p, key, op, fields.uid)

	case strings.HasPrefix(folded, "labels.") && fields.labels != nil:
		lk := key[len("labels."):]
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		return parseListLabelClause(p, op, lk, fields.labels)

	case folded == "labels" && fields.labels != nil:
		if op != ":" && op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on labels (only \":\" and \"=\" are supported)", op)
		}
		if v := p.Next(); v.Kind != aip160.KindStar {
			return nil, fmt.Errorf("labels takes only \"*\" (any label), got %s", aip160.TokenText(v))
		}
		return listPredicate[T](func(v T) bool { return len(fields.labels(v)) > 0 }), nil
	}

	if get, ok := fields.strFields[folded]; ok {
		return parseListStringClause(p, key, op, get)
	}
	if get, ok := fields.timeFields[folded]; ok {
		return parseListTimeClause(p, key, op, get)
	}
	return nil, fmt.Errorf("unsupported filter key %q (only %s are supported)", key, fields.desc)
}

// parseListStringClause compiles an identity/string-field clause. "=" and "!="
// are exact matches; ":" is AIP-160's has/contains operator (a substring match).
func parseListStringClause[T any](p *aip160.Parser, key, op string, get func(T) string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on %s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		return nil, fmt.Errorf("%s does not accept \"*\"", key)
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be a value or a quoted string, got %s", key, aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return listPredicate[T](func(v T) bool { return get(v) == val }), nil
	case "!=":
		return listPredicate[T](func(v T) bool { return get(v) != val }), nil
	default:
		return listPredicate[T](func(v T) bool { return strings.Contains(get(v), val) }), nil
	}
}

// parseListLabelClause compiles a labels.<key> clause. "=" and "!=" are exact
// matches; ":" (or "*" as the value) tests that the key is present with any
// value, and "!= *" tests that it is absent.
func parseListLabelClause[T any](p *aip160.Parser, op, key string, labels func(T) map[string]string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on labels.%s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		if op == "!=" {
			return listPredicate[T](func(v T) bool { _, ok := labels(v)[key]; return !ok }), nil
		}
		return listPredicate[T](func(v T) bool { _, ok := labels(v)[key]; return ok }), nil
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("label value must be a value, a quoted string or \"*\", got %s", aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return listPredicate[T](func(v T) bool { return labels(v)[key] == val }), nil
	case "!=":
		return listPredicate[T](func(v T) bool { return labels(v)[key] != val }), nil
	default:
		return listPredicate[T](func(v T) bool { return strings.Contains(labels(v)[key], val) }), nil
	}
}

// parseListTimeClause compiles a timestamp comparison. The value must be an
// RFC3339 timestamp or a YYYY-MM-DD date (quoted, per AIP-160); the six
// comparison operators are supported, ":" is not.
func parseListTimeClause[T any](p *aip160.Parser, key, op string, get func(T) time.Time) (any, error) {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
	default:
		return nil, fmt.Errorf("unsupported operator %q on %s (only =, !=, <, <=, > and >= are supported)", op, key)
	}
	valTok := p.Next()
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("%s value must be an RFC3339 timestamp or YYYY-MM-DD date, got %s", key, aip160.TokenText(valTok))
	}
	bound, err := aip160.ParseTime(val)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", key, err)
	}
	want := bound.UTC().Format(aip160.TimeLayout)
	return listPredicate[T](func(v T) bool {
		got := get(v).UTC().Format(aip160.TimeLayout)
		switch op {
		case "=":
			return got == want
		case "!=":
			return got != want
		case "<":
			return got < want
		case "<=":
			return got <= want
		case ">":
			return got > want
		default: // ">="
			return got >= want
		}
	}), nil
}

// ─── orderBy ──────────────────────────────────────────────────────────────────

// advancedOrderSpec is the orderBy grammar of an advanced-surface list
// (messageBuses/enrollments/pipelines/googleApiSources/channelConnections): the
// identity, uid, displayName and timestamp fields. The kind's own id parameter
// is accepted as an alias of name — consistent with the channels ("channel_id")
// and triggers ("trigger_id") id aliases the Discovery examples use.
func advancedOrderSpec(k Kind) aip160.OrderSpec[AdvancedRecord] {
	fields := []aip160.OrderField[AdvancedRecord]{
		{Name: "name", Value: func(r AdvancedRecord) string { return r.ID }},
		{Name: k.IDParam, Value: func(r AdvancedRecord) string { return r.ID }},
		{Name: "uid", Value: func(r AdvancedRecord) string { return r.UID }},
		{Name: "createTime", Value: func(r AdvancedRecord) string { return r.CreateTime.UTC().Format(aip160.TimeLayout) }},
		{Name: "updateTime", Value: func(r AdvancedRecord) string { return r.UpdateTime.UTC().Format(aip160.TimeLayout) }},
	}
	if advancedHasDisplayName(k) {
		fields = append(fields, aip160.OrderField[AdvancedRecord]{
			Name: "displayName", Value: func(r AdvancedRecord) string { return advancedDisplayName(r) },
		})
	}
	return aip160.NewOrderSpec(func(r AdvancedRecord) string { return r.ID }, fields...)
}

// channelOrderSpec is the channels.list orderBy grammar (the Discovery example
// is "name desc, channel_id", so channelId is the id alias of name). Channels
// have no displayName.
var channelOrderSpec = aip160.NewOrderSpec(func(c eventarcstore.Channel) string { return c.Name },
	aip160.OrderField[eventarcstore.Channel]{Name: "name", Value: func(c eventarcstore.Channel) string { return c.Name }},
	aip160.OrderField[eventarcstore.Channel]{Name: "channelId", Value: func(c eventarcstore.Channel) string { return c.Name }},
	aip160.OrderField[eventarcstore.Channel]{Name: "uid", Value: func(c eventarcstore.Channel) string { return c.UID }},
	aip160.OrderField[eventarcstore.Channel]{Name: "createTime", Value: func(c eventarcstore.Channel) string { return c.CreateTime.UTC().Format(aip160.TimeLayout) }},
	aip160.OrderField[eventarcstore.Channel]{Name: "updateTime", Value: func(c eventarcstore.Channel) string { return c.UpdateTime.UTC().Format(aip160.TimeLayout) }},
)

// providerOrderSpec is the providers.list orderBy grammar: name and displayName
// (the Provider schema has no uid, labels or timestamps), plus the `_id`
// spelling the Discovery parameter's own example uses ("name desc, _id"); the
// shared order folding strips the underscore, so `_id`/`id` both resolve to the
// provider id.
var providerOrderSpec = aip160.NewOrderSpec(func(p Provider) string { return p.ID },
	aip160.OrderField[Provider]{Name: "name", Value: func(p Provider) string { return p.ID }},
	aip160.OrderField[Provider]{Name: "_id", Value: func(p Provider) string { return p.ID }},
	aip160.OrderField[Provider]{Name: "displayName", Value: func(p Provider) string { return p.DisplayName }},
)

// ─── per-surface field descriptors ────────────────────────────────────────────

// advancedHasDisplayName reports whether the kind's Discovery schema defines a
// displayName. ChannelConnection has none, so naming it is an unknown field
// (INVALID_ARGUMENT) — exactly as a Trigger has none.
func advancedHasDisplayName(k Kind) bool { return k != ChannelConnectionKind }

// advancedDisplayName reads the stored displayName from a record's verbatim
// config body.
func advancedDisplayName(r AdvancedRecord) string {
	return bodyString(decodeBody(r.Config), "displayName")
}

// advancedFilterFields describes an advanced-surface list's filterable fields
// for a kind; the project is needed to render the canonical resource name.
func advancedFilterFields(project string, k Kind) filterFields[AdvancedRecord] {
	f := filterFields[AdvancedRecord]{
		desc:   "name, uid, labels.<key>, labels, createTime and updateTime",
		name:   func(r AdvancedRecord) string { return k.Name(project, r.Location, r.ID) },
		uid:    func(r AdvancedRecord) string { return r.UID },
		labels: func(r AdvancedRecord) map[string]string { return r.Labels },
		timeFields: map[string]func(AdvancedRecord) time.Time{
			"createtime": func(r AdvancedRecord) time.Time { return r.CreateTime },
			"updatetime": func(r AdvancedRecord) time.Time { return r.UpdateTime },
		},
	}
	if advancedHasDisplayName(k) {
		f.desc = "name, uid, displayName, labels.<key>, labels, createTime and updateTime"
		f.strFields = map[string]func(AdvancedRecord) string{
			"displayname": advancedDisplayName,
		}
	}
	return f
}

// providerFilterFields describes providers.list's filterable fields: name and
// displayName (the Provider schema has no uid, labels or timestamps).
func providerFilterFields(project, location string) filterFields[Provider] {
	return filterFields[Provider]{
		desc: "name and displayName",
		name: func(p Provider) string { return ProviderName(project, location, p.ID) },
		strFields: map[string]func(Provider) string{
			"displayname": func(p Provider) string { return p.DisplayName },
		},
	}
}
