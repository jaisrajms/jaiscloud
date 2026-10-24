//go:build gcp_parity

package gcpparity

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

// normalizeJSON decodes a raw response body and rewrites it to a canonical
// logical form shared by both transports:
//
//   - volatile, server-generated fields (etags, timestamps, resource uids,
//     tokens, hashes) fold to a fixed sentinel, so a per-render value cannot
//     decide a divergence;
//   - integer-shaped strings become JSON numbers, because protojson renders
//     64-bit integers as strings while the Discovery REST JSON renders many of
//     them as numbers;
//   - empty/zero-valued members are dropped on both sides, so a transport that
//     omits a defaulted field is not reported against one that emits it;
//   - arrays are sorted by canonical form, because element order is not part of
//     the parity contract.
//
// The transform is deterministic and symmetric: applying it to both transports
// leaves a genuine logical-field difference (a field present on one side and
// absent, or a different value) intact, which is exactly what the diff must see.
func normalizeJSON(raw []byte) (json.RawMessage, error) { return normalize(raw, "") }

// normalizeScoped is normalizeJSON with list-scoping: after canonicalization,
// every array of named objects is filtered to the elements whose name contains
// scope. A list step uses it so a shared emulator (CI runs several suites
// against one emulator) cannot leave unrelated resources in the comparison —
// importantly, filtering makes a list's position deterministic so a value that
// differs between transports (e.g. a REST-only field) cannot reorder the two
// sides and misalign a positional diff.
func normalizeScoped(raw []byte, scope string) (json.RawMessage, error) {
	return normalize(raw, scope)
}

// mutationNormalize canonicalizes a mutation-parity response body after folding
// the per-transport twin token out of resource names, so the gRPC twin
// (.../parity-topic-grpc-<suffix>) and the REST twin
// (.../parity-topic-rest-<suffix>) compare equal. The rest of the pipeline is
// the ordinary read normalizer, so a genuine logical-field difference survives.
func mutationNormalize(raw []byte, project func(json.RawMessage) (json.RawMessage, error)) (json.RawMessage, error) {
	raw = foldTwinSide(raw)
	if project != nil && len(bytes.TrimSpace(raw)) > 0 {
		r, err := project(raw)
		if err != nil {
			return nil, err
		}
		raw = r
	}
	return normalizeJSON(raw)
}

// twinSides are the per-transport id tokens Env.Resource folds in during a
// mutation-parity step. They are stripped before diffing; the tokens are
// hyphen-delimited (`-grpc-` / `-rest-`) so an ordinary value never contains
// them, and the run suffix is hexadecimal so it can never look like one.
var twinSides = [][]byte{[]byte("-grpc-"), []byte("-rest-")}

// foldTwinSide removes every twin token from a raw body so the two transports'
// twins share one canonical name. The replacement joins the surrounding
// segments with a single hyphen, which is exactly the name the twin would have
// had with no side token.
func foldTwinSide(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	for _, side := range twinSides {
		raw = bytes.ReplaceAll(raw, side, []byte("-"))
	}
	return raw
}

func normalize(raw []byte, scope string) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return nil, err
	}
	out, ok := normalizeValue("", v)
	if !ok {
		return nil, nil
	}
	if scope != "" {
		out = scopeArrays(out, scope)
	}
	return marshalNoHTML(out)
}

// scopeArrays recursively filters every array of named objects down to the
// elements whose `name` contains scope. Arrays without a uniform `name` field
// (scalar lists, or nested message lists such as event filters) are left
// untouched.
func scopeArrays(v any, scope string) any {
	switch t := v.(type) {
	case []any:
		named := len(t) > 0
		for _, e := range t {
			m, ok := e.(map[string]any)
			if !ok {
				named = false
				break
			}
			if _, ok := m["name"].(string); !ok {
				named = false
				break
			}
		}
		if named {
			kept := make([]any, 0, len(t))
			for _, e := range t {
				if m, ok := e.(map[string]any); ok {
					if s, ok := m["name"].(string); ok && strings.Contains(s, scope) {
						kept = append(kept, scopeArrays(e, scope))
					}
				}
			}
			return kept
		}
		for i, e := range t {
			t[i] = scopeArrays(e, scope)
		}
		return t
	case map[string]any:
		for k, e := range t {
			t[k] = scopeArrays(e, scope)
		}
		return t
	default:
		return v
	}
}

// volatileKeys are fields whose value is generated per render or per write and
// must never decide a cross-transport divergence. The set mirrors the REST
// differential normalizer (tests/gcpdifferential) so both harnesses agree on what
// "volatile" means; the differential's list-scoping is intentionally omitted
// here because this harness runs both transports against the same emulator and
// its lists therefore contain only this run's resources.
var volatileKeys = map[string]bool{
	"etag":             true,
	"generation":       true,
	"metageneration":   true,
	"selfLink":         true,
	"mediaLink":        true,
	"timeCreated":      true,
	"updated":          true,
	"createTime":       true,
	"updateTime":       true,
	"deleteTime":       true,
	"expireTime":       true,
	"destroyTime":      true,
	"validAfterTime":   true,
	"validBeforeTime":  true,
	"creationTime":     true,
	"lastModifiedTime": true,
	"lastModified":     true,
	"revisionId":       true,
	"nextPageToken":    true,
	"requestId":        true,
	"md5Hash":          true,
	"crc32c":           true,
	"sha256Hash":       true,
	"ciphertext":       true,
	"dataCrc32c":       true,
	"ciphertextCrc32c": true,
	"ackId":            true,
	"messageId":        true,
	"messageIds":       true,
	"jobId":            true,
	"projectNumber":    true,
	"uniqueId":         true,
	"oauth2ClientId":   true,
	// Proto-defined resources carry a server-assigned `uid`; the REST and gRPC
	// adapters may render it with different casing/encoding, and it is not part
	// of the logical contract.
	"uid": true,
	// AUD6-5 output-only fields real Datastore/Cloud Logging return: the batch
	// snapshot version is a per-render server value (the two transports query
	// at different instants), and insertId is server-generated when the client
	// omits one. readTime/commitTime/receiveTimestamp fold by their RFC3339
	// shape already; naming them here keeps the intent explicit.
	"snapshotVersion": true,
	"insertId":        true,
	// Dataproc assigns each cluster and job a server-generated uuid at create.
	// A same-resource read returns the same value over both transports, but the
	// mutation-parity twins (an independent create per transport) cannot share
	// it, so it is volatile by definition.
	"clusterUuid": true,
	"jobUuid":     true,
	// Cloud Workflows/Dataproc revision and operation identifiers.
	"operationId": true,
	"nameServer":  true,
	"nameServers": true,
}

// volatileValue is the sentinel a folded volatile field collapses to. It is a
// fixed string so presence is preserved and a field missing on one side still
// surfaces as a divergence.
const volatileValue = "<volatile>"

// droppedKeys are members removed entirely from both sides before diffing, for
// fields one transport defines and the other omits where the difference is a
// redundant list count rather than a logical field. REST list responses carry a
// totalSize the proto list response does not populate (e.g. Secret Manager); the
// count is fully determined by the element array, so dropping it avoids a
// one-sided informational divergence without hiding any resource field.
var droppedKeys = map[string]bool{
	"totalSize": true,
}

// normalizeValue rewrites v into canonical logical form. The bool result is
// false when the member is empty/zero and should be dropped entirely.
func normalizeValue(key string, v any) (any, bool) {
	if droppedKeys[key] {
		return nil, false
	}
	if volatileKeys[key] {
		return volatileValue, true
	}
	switch t := v.(type) {
	case nil:
		return nil, false
	case bool:
		return t, t // drop false
	case float64:
		return t, t != 0
	case json.Number:
		return t, t.String() != "0"
	case string:
		if s := strings.TrimSpace(t); s == "" {
			return nil, false
		}
		if looksLikeTimestamp(t) {
			return volatileValue, true
		}
		if n, ok := canonicalDuration(t); ok {
			return n, true
		}
		if n, ok := integerString(t); ok {
			return n, true
		}
		return t, true
	case []any:
		out := make([]any, 0, len(t))
		for _, e := range t {
			if nv, ok := normalizeValue(key, e); ok {
				out = append(out, nv)
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		sort.SliceStable(out, func(i, j int) bool {
			return arraySortKey(out[i]) < arraySortKey(out[j])
		})
		return out, true
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			if nv, ok := normalizeValue(k, e); ok {
				out[k] = nv
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	default:
		return v, true
	}
}

// integerString reports whether s is a plain base-10 integer, and returns it as
// a json.Number so protojson's quoted int64 and REST's unquoted number compare
// equal. A leading zero, sign or whitespace is rejected so a real string id
// (e.g. a zero-padded key) is not misread as a number.
func integerString(s string) (json.Number, bool) {
	if s == "" {
		return "", false
	}
	i := 0
	if s[0] == '-' {
		i = 1
	}
	if i >= len(s) || (len(s)-i > 1 && s[i] == '0') {
		return "", false
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return "", false
		}
	}
	if _, err := strconv.ParseInt(s, 10, 64); err != nil {
		return "", false
	}
	return json.Number(s), true
}

// looksLikeTimestamp matches the RFC3339 shape the differential normalizer
// folds, so a timestamp nested under a non-volatile key still cancels.
func looksLikeTimestamp(s string) bool {
	if len(s) < 20 || s[4] != '-' || s[7] != '-' || s[10] != 'T' {
		return false
	}
	if s[len(s)-1] != 'Z' && s[len(s)-1] != 'z' && !strings.ContainsRune(s, '+') {
		if !strings.Contains(s, "-") || strings.LastIndexByte(s, '-') <= 10 {
			return false
		}
	}
	for i := 0; i < 19; i++ {
		c := s[i]
		switch i {
		case 4, 7:
			if c != '-' {
				return false
			}
		case 10:
			if c != 'T' {
				return false
			}
		case 13, 16:
			if c != ':' {
				return false
			}
		default:
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

// canonicalJSON encodes a value without HTML escaping so sentinels such as
// "<volatile>" stay readable and sort deterministically.
// canonicalDuration canonicalizes a protobuf duration string. protojson renders
// a duration with a fixed number of fractional digits ("0.100s") while the
// Discovery/JSON codecs render the shortest form ("0.1s"); both denote the same
// duration, so they are folded to seconds with a trailing "s".
func canonicalDuration(s string) (string, bool) {
	if len(s) < 2 || s[len(s)-1] != 's' {
		return "", false
	}
	body := s[:len(s)-1]
	i := 0
	if body != "" && body[0] == '-' {
		i = 1
	}
	if i >= len(body) || body[i] < '0' || body[i] > '9' {
		return "", false
	}
	digits, dot := false, false
	for ; i < len(body); i++ {
		switch c := body[i]; {
		case c >= '0' && c <= '9':
			digits = true
		case c == '.' && !dot:
			dot = true
		default:
			return "", false
		}
	}
	if !digits {
		return "", false
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return "", false
	}
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s", true
}

// arraySortKey orders array elements deterministically and, for collections of
// named objects, by name. Sorting by name (rather than by the full canonical
// body) keeps the two transports aligned even when a field differs between
// them, so a positional diff compares like elements.
func arraySortKey(v any) string {
	if m, ok := v.(map[string]any); ok {
		if s, ok := m["name"].(string); ok {
			return s
		}
	}
	return canonicalJSON(v)
}

func canonicalJSON(v any) string {
	b, err := marshalNoHTML(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func marshalNoHTML(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
