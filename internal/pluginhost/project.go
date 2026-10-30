package pluginhost

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"io"
	"reflect"
	"strings"
)

// Params and response data cross to a plugin as JSON, but the built-in codecs
// and providers exchange typed values ([]byte payloads, io.Reader streams,
// map[string]string, ...). The rules that keep both sides honest:
//
//   - Values that survive JSON are projected as-is; []byte becomes the marker
//     {"$base64": "..."} so a plugin can read and write binary payloads
//     (Lambda "_payload", S3 object bodies) without losing the type.
//   - Values that cannot be represented (readers, funcs, channels) are never
//     shown to the plugin and are always preserved.
//   - When the plugin returns edited params, every key it left unchanged keeps
//     its ORIGINAL typed value; only keys it changed or added are taken from
//     the plugin (markers converted back to []byte). A key it dropped is removed.
//
// "_"-prefixed keys are no longer special: they are the codec's own channel
// for binary and non-wire data, and a plugin that replaces a service needs them.

const b64Key = "$base64"

// project returns the plugin-visible view of m. Unrepresentable values are skipped.
func project(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if pv, ok := projectValue(v); ok {
			out[k] = pv
		}
	}
	return out
}

func projectValue(v any) (any, bool) {
	switch t := v.(type) {
	case nil, bool, string, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, json.Number:
		return t, true
	case []byte:
		return map[string]any{b64Key: base64.StdEncoding.EncodeToString(t)}, true
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			pv, ok := projectValue(x)
			if !ok {
				return nil, false
			}
			out[k] = pv
		}
		return out, true
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			pv, ok := projectValue(x)
			if !ok {
				return nil, false
			}
			out[i] = pv
		}
		return out, true
	}
	// Typed maps/slices (map[string]string, []string, ...) are fine when they
	// round-trip through JSON; anything else (readers, funcs, chans) is hidden.
	// Streams are structs or pointers that JSON would happily render as {}, so
	// they are excluded by capability first.
	switch v.(type) {
	case io.Reader, io.Writer, io.Closer:
		return nil, false
	}
	k := reflect.TypeOf(v).Kind()
	if k == reflect.Struct {
		// Only structs that know how to serialise themselves (time.Time, ...).
		if _, ok := v.(json.Marshaler); !ok {
			if _, ok := v.(encoding.TextMarshaler); !ok {
				return nil, false
			}
		}
	}
	switch k {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, false
		}
		var out any
		if json.Unmarshal(b, &out) != nil {
			return nil, false
		}
		return out, true
	}
	return nil, false
}

// restore converts plugin-supplied values back: {"$base64": ...} becomes []byte.
func restore(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 1 {
			if s, ok := t[b64Key].(string); ok {
				if b, err := base64.StdEncoding.DecodeString(s); err == nil {
					return b
				}
			}
		}
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = restore(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = restore(x)
		}
		return out
	}
	return v
}

// restoreMap applies restore to every value of a plugin-supplied map.
func restoreMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = restore(v)
	}
	return out
}

// mergeParams applies a plugin's edited view back onto the original params.
func mergeParams(orig, patch map[string]any) map[string]any {
	out := make(map[string]any, len(orig)+len(patch))
	for k, ov := range orig {
		pv, projectable := projectValue(ov)
		if !projectable {
			out[k] = ov // the plugin never saw it: preserve
			continue
		}
		nv, kept := patch[k]
		if !kept {
			continue // the plugin dropped it
		}
		if jsonEqual(pv, nv) {
			out[k] = ov // unchanged: keep the original typed value
		} else {
			out[k] = restore(nv)
		}
	}
	for k, nv := range patch {
		if _, had := orig[k]; !had {
			out[k] = restore(nv)
		}
	}
	return out
}

func jsonEqual(a, b any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(ab, bb)
}

// hasHidden reports whether data holds values a plugin cannot see (streams).
func hasHidden(data map[string]any) bool {
	for _, v := range data {
		if _, ok := projectValue(v); !ok {
			return true
		}
	}
	return false
}

func isInternalKey(k string) bool { return strings.HasPrefix(k, "_") }
