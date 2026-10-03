package run

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

func parseJSON(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

// splitVerb splits a "name:verb" segment into its name and custom verb.
func splitVerb(seg string) (name, verb string) {
	if i := strings.IndexByte(seg, ':'); i >= 0 {
		return seg[:i], seg[i+1:]
	}
	return seg, ""
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}
