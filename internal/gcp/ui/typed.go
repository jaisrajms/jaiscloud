package ui

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/pluginchain"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Some console services work on typed store structs, not on request and response
// maps, so a plugin's answer cannot be handed to them as it is. When a plugin
// covers a registry key, the console call is translated to the REST shape the
// plugin knows (path params plus body), run through the plugin chain with the
// REST built-in as the innermost handler, and the answer is translated back into
// the typed model. When no plugin covers the key the console calls its core
// directly, exactly as before, so a run without plugins is unchanged.
//
// One consequence: with a plugin in the path the console shows the REST rendering
// of a resource, which is what every other client of the key sees.

// typedCaller runs covered console calls through the plugin chain.
type typedCaller struct {
	chain    *pluginchain.Chain
	builtins map[string]provider.HandlerFunc // REST built-ins by key
	cfg      *config.Config
}

// do runs key through the chain. covered is false when no plugin touches key (or
// there is no built-in to put under the chain), in which case the caller takes its
// normal typed path. next overrides the REST built-in for console calls the REST
// API has no counterpart for (a project-wide list).
func (c typedCaller) do(ctx context.Context, key, project, region string, params map[string]any, next provider.HandlerFunc) (data map[string]any, covered bool, err error) {
	if c.chain == nil || !c.chain.Covers(key) {
		return nil, false, nil
	}
	if next == nil {
		next = c.builtins[key]
	}
	if next == nil {
		return nil, false, nil
	}
	service, action, _ := strings.Cut(key, ".")
	nr := uihelper.NR(ctx, c.cfg, strings.ToLower(service), action, region, project)
	nr.Origin = model.OriginUI
	nr.Params = params
	resp, err := c.chain.Run(ctx, key, nr, next)
	if err != nil {
		return nil, true, err
	}
	if resp == nil {
		return map[string]any{}, true, nil
	}
	data, err = normalize(resp.Data)
	return data, true, err
}

// doList is do for a listing: it follows nextPageToken until the list ends, so the
// console shows every page a plugin serves and not only the first. A repeated token or
// an endless list stops with a PluginProtocolError. field names the array in the answer.
func (c typedCaller) doList(ctx context.Context, key, project, region string, params map[string]any, next provider.HandlerFunc, field string) (rows []map[string]any, covered bool, err error) {
	err = paging.Follow(ctx, func(token string) (string, error) {
		page := make(map[string]any, len(params)+1)
		for k, v := range params {
			page[k] = v
		}
		if token != "" {
			page["pageToken"] = token
		}
		data, cov, err := c.do(ctx, key, project, region, page, next)
		covered = cov
		if err != nil || !cov {
			return "", err
		}
		rows = append(rows, items(data, field)...)
		nextToken, _ := data["nextPageToken"].(string)
		return nextToken, nil
	})
	if errors.Is(err, paging.ErrPageLoop) {
		err = model.NewProviderError("PluginProtocolError", "the listing for "+key+" never ends: its next page token repeats", 502)
	}
	return rows, covered, err
}

// normalize turns a handler's Go-typed answer (or a plugin's decoded JSON) into
// plain decoded JSON, which is what the REST decoders expect.
func normalize(m map[string]any) (map[string]any, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// items returns the objects of a list answer under field.
func items(m map[string]any, field string) []map[string]any {
	raw, _ := m[field].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, e := range raw {
		if o, ok := e.(map[string]any); ok {
			out = append(out, o)
		}
	}
	return out
}

// nameParts splits a resource name like projects/p/locations/l/jobs/j into its
// alternating segments: {"projects":"p","locations":"l","jobs":"j"}.
func nameParts(name string) map[string]string {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	out := map[string]string{}
	for i := 0; i+1 < len(segs); i += 2 {
		out[segs[i]] = segs[i+1]
	}
	return out
}

// decodeTyped turns a REST answer back into the typed store model. The model's own
// JSON names cover the output-only fields (state, timestamps, counters), taken
// best-effort; the REST request decoder is applied on top for the fields that need
// its parsing (durations, bytes, resource names). A field the answer does not carry
// stays at its zero value.
func decodeTyped[T any](m map[string]any, fromBody func(map[string]any) (T, error)) (T, error) {
	var out T
	if b, err := json.Marshal(m); err == nil {
		_ = json.Unmarshal(b, &out) // best effort: a field of another shape is skipped
	}
	in, err := fromBody(m)
	if err != nil {
		return out, err
	}
	overlay(&out, in)
	return out, nil
}

// overlay copies every non-zero field of src (a struct) onto dst (a pointer to the same struct type).
func overlay[T any](dst *T, src T) {
	d, s := reflect.ValueOf(dst).Elem(), reflect.ValueOf(src)
	for i := 0; i < s.NumField(); i++ {
		if f := s.Field(i); f.CanInterface() && !f.IsZero() {
			d.Field(i).Set(f)
		}
	}
}

// strField reads a string field; absent is "".
func strField(m map[string]any, field string) string {
	s, _ := m[field].(string)
	return s
}

// strMapField reads a string-to-string map field; absent is nil, present but empty is empty.
func strMapField(m map[string]any, field string) map[string]string {
	raw, ok := m[field].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		out[k], _ = v.(string)
	}
	return out
}

// timeField reads an RFC 3339 timestamp field; absent or malformed is the zero time.
func timeField(m map[string]any, field string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, strField(m, field))
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}
