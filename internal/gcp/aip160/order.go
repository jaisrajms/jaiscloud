package aip160

import (
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"
)

// TimeLayout is the fixed-width RFC3339 layout for timestamp sort/filter keys.
// time.RFC3339Nano is unusable for a textual sort: it trims trailing zeros from
// the fraction, so a whole-second timestamp renders "...05Z" while a fractional
// one renders "...05.1Z", and 'Z' sorts after '.' — the textual order would
// invert. A fixed nine-digit fraction keeps the text order chronological, so a
// string comparison is a time comparison.
const TimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// ParseTime parses an AIP-160 filter/sort timestamp value: an RFC3339
// timestamp, or a YYYY-MM-DD date (which AIP-160 interprets as that day's
// midnight). A value in any other form is an error so the caller can reject it
// rather than compare against a zero time.
func ParseTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("value %q is not an RFC3339 timestamp or a YYYY-MM-DD date", s)
}

// OrderField is one orderable field a list surface documents: its canonical
// name and how to read the field's value from an item.
type OrderField[T any] struct {
	Name  string
	Value func(T) string
}

// OrderSpec is a list surface's orderBy grammar: the fields it accepts plus a
// final tiebreak key (the resource id or name) that makes the order total, so
// the page cursor is unambiguous.
type OrderSpec[T any] struct {
	fields map[string]OrderField[T]
	tie    func(T) string
}

// NewOrderSpec builds an OrderSpec over the given fields. Field names are
// matched case-insensitively after folding underscores, so a canonical
// camelCase name and a proto/snake_case spelling both resolve to the same
// field.
func NewOrderSpec[T any](tie func(T) string, fields ...OrderField[T]) OrderSpec[T] {
	m := make(map[string]OrderField[T], len(fields))
	for _, f := range fields {
		m[foldOrderName(f.Name)] = f
	}
	return OrderSpec[T]{fields: m, tie: tie}
}

// foldOrderName normalizes an orderBy field spelling for lookup.
func foldOrderName(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}

// Tie returns an item's tiebreak key (the resource id/name). It is the order
// applied when no orderBy field is given, and the key paging.Page sorts by.
func (s OrderSpec[T]) Tie(item T) string { return s.tie(item) }

// Order is a parsed orderBy. An empty key list is the surface's default order:
// the tiebreak key ascending (for these services, the resource id/name).
type Order[T any] struct {
	keys []OrderKey[T]
	tie  func(T) string
}

// OrderKey is one parsed orderBy field.
type OrderKey[T any] struct {
	name  string
	desc  bool
	value func(T) string
}

// Parse parses the documented orderBy grammar: a comma-separated list of field
// names, each optionally followed by an "asc" or "desc" direction. Anything
// outside the grammar is an error — a sort is never silently ignored, which
// would return the default order to a client that asked for another.
func (s OrderSpec[T]) Parse(orderBy string) (Order[T], error) {
	orderBy = strings.TrimSpace(orderBy)
	if orderBy == "" {
		return Order[T]{tie: s.tie}, nil
	}
	var o Order[T]
	o.tie = s.tie
	for _, raw := range strings.Split(orderBy, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			return Order[T]{}, fmt.Errorf("empty field in orderBy")
		}
		pieces := strings.Fields(part)
		field, ok := s.fields[foldOrderName(pieces[0])]
		if !ok {
			return Order[T]{}, fmt.Errorf("unsupported orderBy field %q", pieces[0])
		}
		key := OrderKey[T]{name: field.Name, value: field.Value}
		switch {
		case len(pieces) == 1:
		case len(pieces) == 2 && strings.EqualFold(pieces[1], "desc"):
			key.desc = true
		case len(pieces) == 2 && strings.EqualFold(pieces[1], "asc"):
		default:
			return Order[T]{}, fmt.Errorf("unsupported direction in orderBy field %q", part)
		}
		o.keys = append(o.keys, key)
	}
	return o, nil
}

// Sort orders items (stably) by the parsed keys and the tiebreak key. An empty
// order sorts by the tiebreak key ascending.
func (o Order[T]) Sort(items []T) {
	sort.SliceStable(items, func(i, j int) bool { return o.less(items[i], items[j]) })
}

func (o Order[T]) less(a, b T) bool {
	for i := range o.keys {
		k := o.keys[i]
		av, bv := k.value(a), k.value(b)
		if av == bv {
			continue
		}
		if k.desc {
			return av > bv
		}
		return av < bv
	}
	return o.tie(a) < o.tie(b)
}

// Cursor returns an item's page-token payload (before the page token's base64
// encoding): the bare tiebreak key for the default order — byte-identical to
// the token paging.Page has always produced — or the ordered field values
// followed by the tiebreak key for a custom order, so a later page resumes at
// the right position even when the item the token was minted from was deleted
// in between.
func (o Order[T]) Cursor(item T) string {
	if len(o.keys) == 0 {
		return o.tie(item)
	}
	parts := make([]string, 0, len(o.keys)+1)
	for i := range o.keys {
		parts = append(parts, base64.RawURLEncoding.EncodeToString([]byte(o.keys[i].value(item))))
	}
	parts = append(parts, base64.RawURLEncoding.EncodeToString([]byte(o.tie(item))))
	return strings.Join(parts, ".")
}

// AfterCursor reports whether item sorts strictly after the position the
// decoded page-token payload denotes. A payload this order cannot interpret —
// wrong part count, undecodable base64 — is reported as "after" for every item,
// so the scan restarts at the first item rather than resuming in the wrong
// position or returning a silent empty page.
func (o Order[T]) AfterCursor(item T, token string) bool {
	if len(o.keys) == 0 {
		return o.tie(item) > token
	}
	parts := strings.Split(token, ".")
	if len(parts) != len(o.keys)+1 {
		return true
	}
	for i := range o.keys {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil {
			return true
		}
		v := o.keys[i].value(item)
		if v == string(raw) {
			continue
		}
		if o.keys[i].desc {
			return v < string(raw)
		}
		return v > string(raw)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[len(o.keys)])
	if err != nil {
		return true
	}
	return o.tie(item) > string(raw)
}
