// Package paging provides the cursor-based pagination shared by the GCP v1
// service providers (Pub/Sub, Secret Manager, KMS, IAM). It honors the
// pageSize/pageToken query parameters and emits nextPageToken, matching the
// GCP REST convention (distinct from GCS's maxResults).
//
// The pageSize contract follows AIP-158: an absent or zero pageSize selects the
// server default, an over-large one is a suggestion the server may return fewer
// than, and a negative one is INVALID_ARGUMENT. The last rule is the reason
// every entry point here returns an error: a caller must surface it to the
// client rather than silently falling back to the default. A list method whose
// Discovery document instead documents that a non-positive value is ignored or
// selects the default — Cloud Logging's non-positive-ignored methods
// (logs.list, monitoredResourceDescriptors.list, metrics.list, …) and Cloud
// Run's projects.locations.operations.list ("If not set or set to less than or
// equal to 0, the default page size is 100") — must clear a negative size
// before calling Page/PageOrdered, not lean on the default. A list method with
// a bespoke cursor (Cloud Scheduler and Cloud Tasks lists, Cloud Workflows
// ListWorkflowRevisions, the gRPC Cloud Storage lists) applies Size to its
// already-parsed pageSize instead of re-deriving the rule.
package paging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// defaultPageSize is the page size applied when the request omits pageSize or
// sets it to zero, matching the shared GCP default.
const defaultPageSize = 1000

// Apply returns the page of ResourceEntry values for the current request and
// the next-page token (empty when there are no more results). A negative
// pageSize is an InvalidArgument error.
func Apply(entries []store.ResourceEntry, params map[string]any) (page []store.ResourceEntry, nextToken string, err error) {
	return Page(entries, func(e store.ResourceEntry) string { return e.ID }, params)
}

// Page is the generic cursor-pagination core: it sorts items by key and
// applies pageSize/pageToken cursor pagination, returning the page and the
// next-page token (empty when there are no more results). A negative pageSize
// is an InvalidArgument error.
func Page[T any](items []T, key func(T) string, params map[string]any) (page []T, nextToken string, err error) {
	size, err := PageSize(params)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })

	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
			for start < len(items) && key(items[start]) <= cursor {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextToken = encodeCursor(key(items[end-1]))
	}
	return page, nextToken, nil
}

// PageOrdered applies pageSize/pageToken cursor pagination to items the caller
// has already ordered. cursor returns an item's cursor payload (the value that
// identifies its position, before the page token's base64 encoding) and
// afterCursor reports whether item sorts strictly after the position a decoded
// token denotes. Unlike Page it never sorts, so a non-name orderBy (a service
// list whose caller supplied orderBy) composes with pagination instead of the
// token silently resuming in a different order. As in Page, a token that cannot
// be decoded is treated as absent, so the scan restarts at the first item, and
// a negative pageSize is an InvalidArgument error.
func PageOrdered[T any](items []T, params map[string]any,
	cursor func(T) string, afterCursor func(item T, token string) bool) (page []T, nextToken string, err error) {
	size, err := PageSize(params)
	if err != nil {
		return nil, "", err
	}
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cur := decodeCursor(tok); cur != "" {
			for start < len(items) && !afterCursor(items[start], cur) {
				start++
			}
		}
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	page = items[start:end]
	if end < len(items) {
		nextToken = encodeCursor(cursor(items[end-1]))
	}
	return page, nextToken, nil
}

// PageSize parses the pageSize query parameter (string, int, or float64),
// falling back to the GCP default of 1000 when it is absent or zero. A negative
// value is rejected with INVALID_ARGUMENT per AIP-158, and so is a value that
// is not a valid int32 (real GCP's query/proto binding rejects it rather than
// treating it as absent).
func PageSize(params map[string]any) (int, error) {
	n, err := ParseSize(params["pageSize"])
	if err != nil {
		return 0, err
	}
	return normalizePageSize(n)
}

// ParseSize reads a raw pageSize value as an int32, the type the GCP Discovery
// documents declare for pageSize (a handful of legacy `maxResults` bindings are
// uint32 instead; see ParseUintSizeNamed). Unlike PageSize it applies no paging
// policy: an absent value (nil) is 0, and the caller decides whether zero means
// the default (PageSize) or the maximum (Size). A value the int32 binding would
// reject — an unparseable string such as "abc", an explicitly empty value
// (`?pageSize=`, which the frontend binds as `""` and rejects), a fraction such
// as "1.5" or "-0.5", or one outside int32 — is INVALID_ARGUMENT, never
// silently absent. This is the entry point for a bespoke REST pager that parses
// its own pageSize before handing the integer to paging.Size or to a
// transport-neutral core.
func ParseSize(v any) (int, error) {
	return ParseSizeNamed(v, "pageSize")
}

// ParseSizeNamed is ParseSize for a page-size parameter with another documented
// name (`maxResults` on the legacy list APIs: Cloud DNS, Cloud SQL, Compute
// Engine). The parse rule is the int32 query binding the Discovery document
// declares, and a rejected value gets the envelope the Google API frontend
// sends for an unparseable query binding — "Invalid value at 'max_results'
// (TYPE_INT32), \"abc\"" — naming the proto field the parameter binds to and
// echoing the value exactly as the client sent it, so a client that echoes the
// error reads the parameter it actually sent. An absent value (nil) is 0; the
// caller keeps its own zero/negative policy. An explicitly empty query value is
// rejected like any other unparseable binding (DC63).
func ParseSizeNamed(v any, name string) (int, error) {
	return parseBoundSize(v, name, true)
}

// ParseUintSizeNamed is the uint32 form of the same binding. It is for the list
// APIs whose Discovery document declares `maxResults` an unsigned integer
// (Compute Engine and Cloud SQL's instances/operations lists), where a negative
// value is not a valid binding at all, and a value above MaxInt32 is still a
// legal uint32. The caller still owns the range policy (Compute's documented
// 0-500 clamp, Cloud SQL's documented 1,000 ceiling). A service whose frontend
// binds the parameter under a message path passes that path as the name
// (Compute Engine sends "header.maxResults", which renders as
// 'header.max_results'), since the name is also the proto field the error text
// quotes.
func ParseUintSizeNamed(v any, name string) (int, error) {
	return parseBoundSize(v, name, false)
}

// parseBoundSize implements the int32/uint32 query binding shared by the named
// resolvers. An absent parameter arrives as nil and binds to 0; any value the
// binding cannot represent — an explicitly empty query value (`?pageSize=`)
// included, which the frontend rejects with the binding envelope — is
// INVALID_ARGUMENT.
func parseBoundSize(v any, name string, signed bool) (int, error) {
	min, max := int64(math.MinInt32), int64(math.MaxInt32)
	kind := "int32"
	if !signed {
		min, max = 0, math.MaxUint32
		kind = "uint32"
	}
	invalid := func() error { return invalidBoundSize(kind, name, v) }
	check := func(n int64) (int, error) {
		if n < min || n > max {
			return 0, invalid()
		}
		return int(n), nil
	}
	switch x := v.(type) {
	case nil:
		return 0, nil
	case string:
		if signed {
			n, err := strconv.ParseInt(x, 10, 32)
			if err != nil {
				return 0, invalid()
			}
			return check(n)
		}
		n, err := strconv.ParseUint(x, 10, 32)
		if err != nil {
			return 0, invalid()
		}
		return check(int64(n))
	case int:
		return check(int64(x))
	case int32:
		return check(int64(x))
	case int64:
		return check(x)
	case float64:
		// A proto int32/uint32 binding rejects a fraction outright (never
		// truncates); check that before the range so "-0.5" is "invalid value",
		// not "must not be negative", and "1.9" is not silently sent as 1.
		if x != math.Trunc(x) || x < float64(min) || x > float64(max) {
			return 0, invalid()
		}
		return check(int64(x))
	case json.Number:
		if signed {
			n, err := strconv.ParseInt(x.String(), 10, 32)
			if err != nil {
				return 0, invalid()
			}
			return check(n)
		}
		n, err := strconv.ParseUint(x.String(), 10, 32)
		if err != nil {
			return 0, invalid()
		}
		return check(int64(n))
	default:
		return 0, invalid()
	}
}

// SizeFrom is the bespoke-pager entry point that combines ParseSize with the
// AIP-158 ceiling: an absent/zero value selects max, a negative one is
// INVALID_ARGUMENT, an over-large one is coerced down, and a value the int32
// query binding cannot parse is INVALID_ARGUMENT (rather than being folded into
// zero and treated as absent). A pager whose size arrives as a raw query value
// calls this instead of parsing the string itself.
func SizeFrom(v any, max int) (int, error) {
	n, err := ParseSize(v)
	if err != nil {
		return 0, err
	}
	return Size(n, max)
}

// Size applies the AIP-158 page-size contract to a bespoke pager that has
// already parsed its own pageSize (the REST handlers parse the query value and
// the gRPC ones read the int32 field). Zero — including an absent parameter,
// which such callers pass as zero — selects the maximum, a negative value is
// InvalidArgument, and an over-large value is coerced down to the maximum
// (AIP-158's "should coerce", matching the Discovery documents' "If unspecified,
// the page size will be the maximum"). This deliberately differs from PageSize,
// which leaves an over-large value to Page's caller because Page already returns
// at most the available items: here the resolver owns the ceiling as well, so a
// bespoke pager cannot accidentally page with an unbounded size.
//
// max must be the service's documented positive ceiling; a non-positive value
// is floored to 1 so the resolver always returns a usable page size (and a
// bespoke pager can never index an empty page). A pager with a bespoke cursor
// calls this instead of re-deriving the rule, so the negative case cannot
// silently fall back to the first page.
func Size(n, max int) (int, error) {
	if max < 1 {
		max = 1
	}
	if n < 0 {
		return 0, negativePageSize()
	}
	if n == 0 || n > max {
		return max, nil
	}
	return n, nil
}

// normalizePageSize applies the AIP-158 rule to a parsed integer size: zero
// selects the default, a negative value is invalid, and a positive value is
// passed through.
func normalizePageSize(n int) (int, error) {
	if n < 0 {
		return 0, negativePageSize()
	}
	if n == 0 {
		return defaultPageSize, nil
	}
	return n, nil
}

func negativePageSize() error {
	return model.NewProviderError("InvalidArgument", "pageSize must not be negative", 400)
}

// invalidBoundSize is the error real GCP's query binding returns for a value it
// cannot parse: the proto3-JSON/ESF text "Invalid value at 'max_results'
// (TYPE_INT32), \"abc\"", naming the proto field the query parameter binds to
// (snake_case, not the camelCase query name) and echoing the value as the
// client sent it. Probe-confirmed on Cloud DNS, Cloud SQL, Pub/Sub, Cloud
// Scheduler and Cloud KMS alike (`pageSize`/`maxResults`); GCS's JSON API uses
// its own "Invalid unsigned integer value" text and has its own binding
// (provider/storage). Compute Engine's frontend binds the parameter on its
// `header` message, so its caller passes "header.maxResults" and the text names
// 'header.max_results' (DC68). ParseSizeNamed and ParseUintSizeNamed route
// through it.
func invalidBoundSize(kind, name string, v any) error {
	return model.NewProviderError("InvalidArgument",
		"Invalid value at '"+protoFieldName(name)+"' (TYPE_"+strings.ToUpper(kind)+"), "+boundValueText(v), 400)
}

// boundValueText renders the offending value the way the frontend echoes it: a
// query parameter arrives as a string and is re-quoted verbatim, while a typed
// (proto) value is rendered plainly.
func boundValueText(v any) string {
	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}
	return fmt.Sprintf("%v", v)
}

// protoFieldName converts the camelCase query-parameter name `ParseSizeNamed`
// receives into the snake_case proto field the frontend names in its binding
// error (`maxResults` -> `max_results`, `pageSize` -> `page_size`). A dotted
// message path passes through unchanged apart from the case conversion
// (`header.maxResults` -> `header.max_results`, the Compute Engine carve-out).
func protoFieldName(name string) string {
	var b strings.Builder
	for i, r := range name {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r - 'A' + 'a')
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// rangeBoundSize is the other half of a legacy list's maxResults contract: a
// value the binding *can* parse but that falls outside the range the service's
// list backend accepts. Real GCP answers it with the parameter-validation text
// ("Invalid value for 'parameters.maxResults': '0'"), which — unlike the
// binding error — names the camelCase query parameter and quotes the value
// without JSON escaping. Probe-confirmed on Cloud DNS (DC62): managedZones,
// rrsets and changes accept 1..1000 and reject 0, a negative and anything above
// 1,000.
func rangeBoundSize(name string, v any) error {
	value := fmt.Sprintf("%v", v)
	if s, ok := v.(string); ok {
		value = s
	}
	return model.NewProviderError("InvalidArgument",
		"Invalid value for 'parameters."+name+"': '"+value+"'", 400)
}

// MaxResultsBinding is a legacy `maxResults` page-size parameter after the
// query binding has been applied: Size is the parsed integer, and Param/Raw are
// the key and the value the client sent. The zero value (Param == "") means the
// parameter was absent (or nil); an explicitly empty value is a binding error,
// not an absent parameter (DC63).
type MaxResultsBinding struct {
	Size  int
	Param string
	Raw   any
}

// BindMaxResults applies the int32 query binding — the half real GCP validates
// in the API frontend, before the request is routed, so an unparseable value is
// INVALID_ARGUMENT ("Invalid value at 'max_results' (TYPE_INT32), \"abc\"")
// even when the parent resource does not exist. Probed: `rrsets?maxResults=abc`
// on a missing zone is 400, while `rrsets?maxResults=0` is the zone's 404 —
// which is why this is separate from Resolve, whose range check runs after the
// parent lookup. Keys are read in order (so a caller can keep the emulator's
// pageSize alias) and no default or range policy is applied here.
//
// An explicitly empty value is NOT skipped: real GCP's frontend binds `""` and
// rejects it, and the row that had deferred that rule (DC63) was settled by
// direct probes (Cloud DNS, Cloud SQL, GCS and Pub/Sub all answer 400). The
// emulator's `pageSize` alias is bound the same way when listed in keys, since
// the emulator cannot ignore it the way real GCP ignores an unknown parameter.
func BindMaxResults(params map[string]any, keys []string) (MaxResultsBinding, error) {
	for _, k := range keys {
		v, ok := params[k]
		if !ok || v == nil {
			continue
		}
		n, err := ParseSizeNamed(v, k)
		if err != nil {
			return MaxResultsBinding{}, err
		}
		return MaxResultsBinding{Size: n, Param: k, Raw: v}, nil
	}
	return MaxResultsBinding{}, nil
}

// Resolve applies the caller's policy to a bound maxResults: def when the
// parameter was absent, and INVALID_ARGUMENT "Invalid value for
// 'parameters.<param>': '<v>'" when the parsed value falls outside the list
// backend's [lo, hi] range.
func (b MaxResultsBinding) Resolve(def, lo, hi int) (int, error) {
	if b.Param == "" {
		return def, nil
	}
	if b.Size < lo || b.Size > hi {
		return 0, rangeBoundSize(b.Param, b.Raw)
	}
	return b.Size, nil
}

// MaxResultsUint32 resolves a maxResults parameter the Discovery document
// declares unsigned (Cloud SQL's instances/operations lists) to its caller's
// default when absent, nil or zero. A negative value is not a legal binding at
// all, so it is INVALID_ARGUMENT ("Invalid value at 'max_results'
// (TYPE_UINT32), \"-1\""), and a value the binding cannot parse — an explicitly
// empty one included — is the same error rather than a silently absent
// parameter. The caller clamps above its own documented ceiling (Cloud SQL's
// 1,000), which is why this resolver carries no range policy. `keys` are query
// parameter names (map keys), never a field path: Compute Engine, whose
// frontend binds on its `header` message, calls ParseUintSizeNamed directly
// with "header.maxResults".
func MaxResultsUint32(params map[string]any, keys []string, def int) (int, error) {
	return maxResultsParam(params, keys, def, ParseUintSizeNamed)
}

func maxResultsParam(params map[string]any, keys []string, def int, parse func(any, string) (int, error)) (int, error) {
	for _, k := range keys {
		v, ok := params[k]
		if !ok {
			continue
		}
		n, err := parse(v, k)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return n, nil
		}
		// A zero value (or a present nil) selects the default; an absent key is
		// handled by the loop exit. The uint32 binding rejects a negative or
		// empty value before it can reach this fallback.
	}
	return def, nil
}

func encodeCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeCursor(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}
	return string(b)
}

// MaxPages bounds Follow. A real listing is far shorter; the bound exists for a
// plugin that answers every page with another page token.
const MaxPages = 10000

// ErrPageLoop is returned by Follow when a list never ends: the next token repeats
// or MaxPages pages were read.
var ErrPageLoop = errors.New("paging: the listing never ends (its next page token repeats)")

// Follow reads a listing page by page. fetch returns the token of the next page
// ("" when it was the last). It stops with ctx's error when the context ends, and
// with ErrPageLoop when a token repeats or MaxPages pages have been read, so a
// source that cannot be trusted (a plugin) cannot keep the caller busy forever.
func Follow(ctx context.Context, fetch func(pageToken string) (next string, err error)) error {
	seen := map[string]bool{}
	token := ""
	for page := 0; ; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if page >= MaxPages {
			return ErrPageLoop
		}
		next, err := fetch(token)
		if err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		if seen[next] {
			return ErrPageLoop
		}
		seen[next] = true
		token = next
	}
}
