package eventarc

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"jaiscloud/internal/gcp/aip160"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// This file implements the two documented triggers.list request parameters the
// transport layer used to drop: `filter` (AIP-160, e.g. "destination:gke") and
// `orderBy` (a comma-separated field list, optionally `<field> desc`). Real
// Eventarc documents both on
// eventarc.projects.locations.triggers.list, so a client that sorts or filters
// must not be handed an unfiltered, name-ascending list: anything outside the
// supported grammar is INVALID_ARGUMENT rather than silently ignored.

// ─── filter ───────────────────────────────────────────────────────────────────

// triggerPredicate is one compiled triggers.list filter clause.
type triggerPredicate func(eventarcstore.Trigger) bool

// triggerListFilter is the compiled subset of the triggers.list filter grammar:
// a conjunction of clauses over name, uid, displayName, labels and destination.
// An empty filter matches every trigger.
type triggerListFilter struct {
	preds []triggerPredicate
}

// match reports whether t satisfies every clause.
func (f triggerListFilter) match(t eventarcstore.Trigger) bool {
	for _, p := range f.preds {
		if !p(t) {
			return false
		}
	}
	return true
}

// triggerDestinationFields are the destination oneof members the
// `destination:<name>` clause accepts, mapped onto the JSON keys an Eventarc
// Trigger body uses (both protojson's lowerCamel and the Discovery camelCase
// spelling).
var triggerDestinationFields = map[string][]string{
	"gke":           {"gke"},
	"cloudRun":      {"cloudRun", "cloud_run"},
	"httpEndpoint":  {"httpEndpoint", "http_endpoint"},
	"workflow":      {"workflow"},
	"cloudFunction": {"cloudFunction", "cloud_function"},
	"networkConfig": {"networkConfig", "network_config"},
}

// triggerDestinationAliases folds every accepted spelling of a destination
// member (its canonical name and its JSON keys) onto that member's JSON keys, so
// `destination:cloudRun`, `destination:cloudrun` and `destination:cloud_run`
// all resolve to the same predicate.
var triggerDestinationAliases = func() map[string][]string {
	m := make(map[string][]string, len(triggerDestinationFields)*2)
	for name, fields := range triggerDestinationFields {
		m[foldDestinationName(name)] = fields
		for _, f := range fields {
			m[foldDestinationName(f)] = fields
		}
	}
	return m
}()

// foldDestinationName normalizes a destination member spelling for lookup.
func foldDestinationName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// compileTriggerListFilter parses the bounded triggers.list filter grammar:
//
//	clause := field op value
//	filter := clause ( ("AND" | implicit whitespace) clause )*
//	field  := "name" | "uid" | "labels." <key> | "labels" | "destination"
//	op     := "=" | "!=" | ":"            (":" only on destination and labels)
//	value  := <ident> | <double-quoted string> | "*"
//
// The clause set mirrors the API's own example ("?filter=destination:gke") plus
// the identity and label fields every AIP-160 list exposes. Only fields the
// Discovery Trigger schema actually defines are accepted (there is no
// displayName on a Trigger, so naming one is INVALID_ARGUMENT like any other
// unknown field). Comparison operators (<, <=, >, >=) and the OR/NOT/
// parenthesis forms are outside the grammar and fail with INVALID_ARGUMENT; a
// filter is never silently ignored, which would return an unfiltered list to a
// client that asked for a subset.
func compileTriggerListFilter(s string) (triggerListFilter, error) {
	if strings.TrimSpace(s) == "" {
		return triggerListFilter{}, nil
	}
	toks, err := aip160.Tokenize(s)
	if err != nil {
		return triggerListFilter{}, invalidArgument("eventarc ListTriggers filter: " + err.Error())
	}
	preds, err := aip160.NewParser(toks).ParseClauses(parseTriggerClause)
	if err != nil {
		return triggerListFilter{}, invalidArgument("eventarc ListTriggers filter: " + err.Error())
	}
	f := triggerListFilter{preds: make([]triggerPredicate, 0, len(preds))}
	for _, p := range preds {
		pred, ok := p.(triggerPredicate)
		if !ok {
			return triggerListFilter{}, invalidArgument("eventarc ListTriggers filter: internal clause type")
		}
		f.preds = append(f.preds, pred)
	}
	return f, nil
}

// parseTriggerClause parses one triggers.list filter clause (`field op value`)
// into the matching triggerPredicate, returned as an `any` for the shared
// clause sequencer.
func parseTriggerClause(p *aip160.Parser) (any, error) {
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

	switch {
	case key == "name":
		return parseTriggerStringClause(p, key, op, func(t eventarcstore.Trigger) string {
			return TriggerName(t.ProjectID, t.Location, t.Name)
		})

	case key == "uid":
		return parseTriggerStringClause(p, key, op, func(t eventarcstore.Trigger) string { return t.UID })

	case strings.HasPrefix(key, "labels."):
		lk := strings.TrimPrefix(key, "labels.")
		if lk == "" {
			return nil, fmt.Errorf("empty label key in %q", key)
		}
		return parseTriggerLabelClause(p, op, lk)

	case key == "labels":
		if op != ":" && op != "=" {
			return nil, fmt.Errorf("unsupported operator %q on labels (only \":\" and \"=\" are supported)", op)
		}
		if v := p.Next(); v.Kind != aip160.KindStar {
			return nil, fmt.Errorf("labels takes only \"*\" (any label), got %s", aip160.TokenText(v))
		}
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return len(t.Labels) > 0 }), nil

	case key == "destination":
		if op != ":" {
			return nil, fmt.Errorf("unsupported operator %q on destination (only \":\" is supported)", op)
		}
		return parseTriggerDestinationClause(p)

	default:
		return nil, fmt.Errorf("unsupported filter key %q "+
			"(only name, uid, labels.<key>, labels and destination are supported)", key)
	}
}

// parseTriggerStringClause compiles an identity-field clause. "=" and "!=" are
// exact matches; ":" is AIP-160's has/contains operator (a substring match).
func parseTriggerStringClause(p *aip160.Parser, key, op string, get func(eventarcstore.Trigger) string) (any, error) {
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
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return get(t) == val }), nil
	case "!=":
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return get(t) != val }), nil
	default:
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return strings.Contains(get(t), val) }), nil
	}
}

// parseTriggerLabelClause compiles a labels.<key> clause. "=" and "!=" are exact
// matches; ":" (or "*" as the value) tests that the key is present with any
// value, and "!= *" tests that it is absent.
func parseTriggerLabelClause(p *aip160.Parser, op, key string) (any, error) {
	if op != "=" && op != "!=" && op != ":" {
		return nil, fmt.Errorf("unsupported operator %q on labels.%s (only \"=\", \"!=\" and \":\" are supported)", op, key)
	}
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		if op == "!=" {
			return triggerPredicate(func(t eventarcstore.Trigger) bool { _, ok := t.Labels[key]; return !ok }), nil
		}
		return triggerPredicate(func(t eventarcstore.Trigger) bool { _, ok := t.Labels[key]; return ok }), nil
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("label value must be a value, a quoted string or \"*\", got %s", aip160.TokenText(valTok))
	}
	switch op {
	case "=":
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return t.Labels[key] == val }), nil
	case "!=":
		return triggerPredicate(func(t eventarcstore.Trigger) bool { return t.Labels[key] != val }), nil
	default:
		return triggerPredicate(func(t eventarcstore.Trigger) bool {
			return strings.Contains(t.Labels[key], val)
		}), nil
	}
}

// parseTriggerDestinationClause compiles the documented `destination:<member>`
// clause: `destination:gke` selects triggers whose destination is a GKE target,
// and `destination:*` selects any trigger with a destination set.
func parseTriggerDestinationClause(p *aip160.Parser) (any, error) {
	valTok := p.Next()
	if valTok.Kind == aip160.KindStar {
		return triggerPredicate(func(t eventarcstore.Trigger) bool {
			return bodyMap(decodeBody(t.Config), "destination") != nil
		}), nil
	}
	val, ok := aip160.TokenValue(valTok)
	if !ok {
		return nil, fmt.Errorf("destination value must be a destination name or \"*\", got %s", aip160.TokenText(valTok))
	}
	fields, ok := triggerDestinationAliases[foldDestinationName(val)]
	if !ok {
		return nil, fmt.Errorf("unsupported destination %q "+
			"(want gke, cloudRun, httpEndpoint, workflow, cloudFunction or \"*\")", val)
	}
	return triggerPredicate(func(t eventarcstore.Trigger) bool {
		dest := bodyMap(decodeBody(t.Config), "destination")
		for _, f := range fields {
			if _, set := dest[f]; set {
				return true
			}
		}
		return false
	}), nil
}

// ─── orderBy ──────────────────────────────────────────────────────────────────

// triggerTimeLayout is the fixed-width RFC3339 layout for the createTime and
// updateTime sort keys. time.RFC3339Nano is unusable here: it trims trailing
// zeros from the fraction, so a whole-second timestamp renders "...05Z" while a
// fractional one renders "...05.1Z", and 'Z' sorts after '.' — the textual order
// would invert. A fixed nine-digit fraction keeps it chronological.
const triggerTimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// triggerOrderKey is one parsed orderBy field.
type triggerOrderKey struct {
	field string // name|uid|createTime|updateTime
	desc  bool
}

// triggerOrder is a parsed triggers.list orderBy. An empty key list is the
// default ascending-by-name order the API applies when orderBy is absent.
type triggerOrder struct {
	keys []triggerOrderKey
}

// compileTriggerOrder parses the documented orderBy grammar: a comma-separated
// list of fields, each optionally suffixed `desc` (or `asc`). The Discovery
// example "name desc, trigger_id" is supported — triggerId/trigger_id are the
// trigger's id, which within a location sorts identically to the full name. Any
// other field is INVALID_ARGUMENT rather than silently ignored (which would
// return the default order to a client that asked for another).
func compileTriggerOrder(s string) (triggerOrder, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return triggerOrder{}, nil
	}
	var order triggerOrder
	for _, raw := range strings.Split(s, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			return triggerOrder{}, invalidArgument("eventarc ListTriggers orderBy: empty field in " + s)
		}
		fields := strings.Fields(part)
		field := fields[0]
		key := triggerOrderKey{field: canonicalTriggerOrderField(field)}
		if key.field == "" {
			return triggerOrder{}, invalidArgument("eventarc ListTriggers orderBy: unsupported field " + field +
				" (want name, uid, createTime or updateTime)")
		}
		switch {
		case len(fields) == 1:
		case len(fields) == 2 && strings.EqualFold(fields[1], "desc"):
			key.desc = true
		case len(fields) == 2 && strings.EqualFold(fields[1], "asc"):
		default:
			return triggerOrder{}, invalidArgument("eventarc ListTriggers orderBy: unsupported direction in " + part)
		}
		order.keys = append(order.keys, key)
	}
	return order, nil
}

// canonicalTriggerOrderField folds an orderBy field spelling onto its canonical
// form, or "" when the field is not orderable. Only fields the Discovery Trigger
// schema defines are accepted (a Trigger has no displayName).
func canonicalTriggerOrderField(f string) string {
	switch f {
	case "name", "triggerId", "trigger_id":
		return "name"
	case "uid":
		return "uid"
	case "createTime", "create_time":
		return "createTime"
	case "updateTime", "update_time":
		return "updateTime"
	}
	return ""
}

// value returns the field's sort value. Timestamps render in a fixed-width
// layout so the textual order is the chronological order.
func (k triggerOrderKey) value(t eventarcstore.Trigger) string {
	switch k.field {
	case "name":
		return t.Name
	case "uid":
		return t.UID
	case "createTime":
		return t.CreateTime.UTC().Format(triggerTimeLayout)
	default: // updateTime
		return t.UpdateTime.UTC().Format(triggerTimeLayout)
	}
}

// less reports whether a sorts before b. The trigger name is the final
// tiebreak, so the order is total and the page cursor is unambiguous.
func (o triggerOrder) less(a, b eventarcstore.Trigger) bool {
	for _, k := range o.keys {
		av, bv := k.value(a), k.value(b)
		if av == bv {
			continue
		}
		if k.desc {
			return av > bv
		}
		return av < bv
	}
	return a.Name < b.Name
}

// cursor renders the opaque page token for t under this order. The default
// ascending-by-name order emits the bare name — byte-identical to the token
// paging.Page has always produced for triggers — while a custom order carries
// the ordered field values so a later page resumes at the right position even
// when the item the token was minted from was deleted in between.
func (o triggerOrder) cursor(t eventarcstore.Trigger) string {
	if len(o.keys) == 0 {
		return t.Name
	}
	parts := make([]string, 0, len(o.keys)+1)
	for _, k := range o.keys {
		parts = append(parts, base64.RawURLEncoding.EncodeToString([]byte(k.value(t))))
	}
	parts = append(parts, base64.RawURLEncoding.EncodeToString([]byte(t.Name)))
	return strings.Join(parts, ".")
}

// afterCursor reports whether t sorts strictly after the position the decoded
// page token denotes. A token this order cannot interpret — wrong part count,
// undecodable base64 — is reported as "after" for every item, so the scan
// restarts at the first page: the same visible outcome the legacy
// paging.Page-based cursor gave an undecodable token, and never a silent empty
// page or a resume in the wrong position.
func (o triggerOrder) afterCursor(t eventarcstore.Trigger, token string) bool {
	if len(o.keys) == 0 {
		return t.Name > token
	}
	parts := strings.Split(token, ".")
	if len(parts) != len(o.keys)+1 {
		return true
	}
	for i, k := range o.keys {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil {
			return true
		}
		v := k.value(t)
		if v == string(raw) {
			continue
		}
		if k.desc {
			return v < string(raw)
		}
		return v > string(raw)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[len(o.keys)])
	if err != nil {
		return true
	}
	return t.Name > string(raw)
}

// orderTriggers returns triggers ordered by the parsed orderBy, defaulting to
// ascending name (the store's own order) when orderBy is absent.
func orderTriggers(triggers []eventarcstore.Trigger, order triggerOrder) []eventarcstore.Trigger {
	sort.SliceStable(triggers, func(i, j int) bool { return order.less(triggers[i], triggers[j]) })
	return triggers
}
