package pluginhost

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// matcher answers the requests the plugins' declared services own. It is built
// once, after apply validated the declarations, and is nil-safe: a cloud whose
// plugins declare no service gets no matcher and keeps its path detection as it
// was.
type matcher struct {
	services []*matchService // plugin order, then path order per service
	paths    []adapter.PluginPath
}

// matchService is one declared service with its codec and compiled paths.
type matchService struct {
	name  string
	codec adapter.Codec
	paths []*compiledPath
}

// compiledPath is one declared method+pattern, parsed once at load.
type compiledPath struct {
	method   string // upper-case wire method
	action   string
	raw      string // the declared pattern, for errors
	segments []segment
	verb     string // ":verb" suffix of the last segment, without the colon
}

type segKind int

const (
	segLiteral segKind = iota // /widgets
	segVar                    // /{widget}
	segPrefix                 // /{widget=widget-*}
)

type segment struct {
	kind   segKind
	text   string // literal text or variable name
	prefix string // segPrefix: the part the segment must start with
}

// versionRe is the API version token a declared path starts with, directly or
// after a product segment (v1, v2beta1, v1p1).
var versionRe = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^v[0-9]+((beta|alpha|p)[0-9]+)?$`)
})

// actionRe is the shape of a declared action name.
var actionRe = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
})

// pluginMethods are the wire methods a declared path may use.
var pluginMethods = map[string]bool{
	http.MethodGet:    true,
	http.MethodPost:   true,
	http.MethodPut:    true,
	http.MethodPatch:  true,
	http.MethodDelete: true,
}

// newMatcher compiles the services the plugins declared for this binary's cloud,
// in plugin order, and caches one codec per service. A compile error or a codec
// error is a load error.
func newMatcher(services []sdk.Service, codecs adapter.PluginCodecs) (*matcher, error) {
	m := &matcher{}
	for _, s := range services {
		codec, err := codecs.PluginCodec(s.Name)
		if err != nil {
			return nil, fmt.Errorf("service %s: %w", s.Name, err)
		}
		paths, err := compilePaths(s)
		if err != nil {
			return nil, err
		}
		m.services = append(m.services, &matchService{name: s.Name, codec: codec, paths: paths})
		for _, p := range paths {
			m.paths = append(m.paths, adapter.PluginPath{Service: s.Name, Method: p.method, Pattern: p.raw})
		}
	}
	return m, nil
}

// compilePaths compiles every path of a validated service, in declaration order.
func compilePaths(s sdk.Service) ([]*compiledPath, error) {
	out := make([]*compiledPath, 0, len(s.Paths))
	for _, pa := range s.Paths {
		cp, err := compilePattern(pa.Pattern)
		if err != nil {
			return nil, fmt.Errorf("service %s: %w", s.Name, err)
		}
		cp.method, cp.action, cp.raw = strings.ToUpper(pa.Method), pa.Action, pa.Pattern
		out = append(out, cp)
	}
	return out, nil
}

// PluginPaths lists the declared paths in the order they were declared.
func (m *matcher) PluginPaths() []adapter.PluginPath {
	if m == nil {
		return nil
	}
	return append([]adapter.PluginPath(nil), m.paths...)
}

// Match reports whether r belongs to a declared service. A matching path with a
// method no declared pattern uses is an owned 404: the plugin owns the path, so
// the built-in must not see it.
func (m *matcher) Match(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, bool, error) {
	if m == nil {
		return nil, nil, false, nil
	}
	segs := requestSegments(r.URL.EscapedPath())
	if len(segs) == 0 {
		return nil, nil, false, nil
	}
	method := strings.ToUpper(r.Method)
	var owned *matchService
	for _, svc := range m.services {
		for _, p := range svc.paths {
			captures, shape := p.matchPath(segs)
			if !shape {
				continue
			}
			if owned == nil {
				owned = svc
			}
			if p.method != method {
				continue
			}
			nr, err := buildRequest(svc, p, r, segs, captures, body)
			return nr, svc.codec, true, err
		}
	}
	if owned != nil {
		return nil, owned.codec, true,
			model.NewProviderError("NotFound", "the requested URL was not found on this server", http.StatusNotFound)
	}
	return nil, nil, false, nil
}

// buildRequest fills the normalized request a plugin service handler receives:
// the captures, the query string, the decoded body, the path, and the project
// and API version when the path carries them. name/resourceType are not
// synthesised, because the built-in resource-type tables do not know these
// resources.
func buildRequest(svc *matchService, p *compiledPath, r *http.Request, segs []string, captures map[string]string, body []byte) (*model.NormalizedRequest, error) {
	params := make(map[string]any, len(captures)+4)
	for k, v := range captures {
		params[k] = v
	}
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
	if len(bytes.TrimSpace(body)) > 0 {
		var v any
		if err := json.Unmarshal(body, &v); err != nil {
			return nil, model.NewProviderError("InvalidArgument", "malformed JSON body", http.StatusBadRequest)
		}
		params["body"] = v
	}
	// apiVersion comes from whichever of the first two segments is the version
	// token, so a product-prefixed path (/<product>/<version>/...) reports it too.
	params["path"] = r.URL.Path
	if v, ok := captures["project"]; ok {
		params["project"] = v
	}
	switch {
	case versionRe().MatchString(segs[0]):
		params["apiVersion"] = segs[0]
	case len(segs) > 1 && versionRe().MatchString(segs[1]):
		params["apiVersion"] = segs[1]
	}
	return &model.NormalizedRequest{Service: svc.name, Action: p.action, Raw: r, Params: params}, nil
}

// matchPath matches the request segments against the pattern, ignoring the
// method, and returns the captures. A pattern with a verb only matches a request
// whose last segment ends in it; a pattern without one only matches a last
// segment that has no verb at all.
func (p *compiledPath) matchPath(segs []string) (map[string]string, bool) {
	if len(segs) != len(p.segments) {
		return nil, false
	}
	last := segs[len(segs)-1]
	if p.verb == "" {
		if strings.Contains(last, ":") {
			return nil, false
		}
	} else {
		suffix := ":" + p.verb
		if !strings.HasSuffix(last, suffix) || len(last) == len(suffix) {
			return nil, false
		}
		last = last[:len(last)-len(suffix)]
	}
	captures := make(map[string]string, len(p.segments))
	for i, ps := range p.segments {
		seg := segs[i]
		if i == len(segs)-1 {
			seg = last
		}
		switch ps.kind {
		case segLiteral:
			if seg != ps.text {
				return nil, false
			}
		case segVar:
			if seg == "" {
				return nil, false
			}
			captures[ps.text] = seg
		case segPrefix:
			if !strings.HasPrefix(seg, ps.prefix) {
				return nil, false
			}
			captures[ps.text] = seg
		}
	}
	return captures, true
}

// requestSegments splits a request path the way the path detector does: leading
// slashes collapse to one and a single trailing slash is ignored. Segments are
// unescaped before matching, so a capture reaches the plugin as the client meant
// it.
func requestSegments(escaped string) []string {
	segs := strings.Split(escaped, "/")
	for len(segs) > 0 && segs[0] == "" {
		segs = segs[1:]
	}
	if n := len(segs); n > 0 && segs[n-1] == "" {
		segs = segs[:n-1]
	}
	for i, s := range segs {
		if u, err := url.PathUnescape(s); err == nil {
			segs[i] = u
		}
	}
	return segs
}

// compilePattern parses one declared pattern and enforces the path constraints:
// it must start with "/", have at least two literal leading segments (so it
// cannot be mistaken for a built-in's variable head), and carry an API version in
// one of those two segments.
func compilePattern(pattern string) (*compiledPath, error) {
	if !strings.HasPrefix(pattern, "/") {
		return nil, fmt.Errorf("pattern %q must start with /", pattern)
	}
	raw := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	if len(raw) < 2 {
		return nil, fmt.Errorf("pattern %q must have at least two segments", pattern)
	}
	cp := &compiledPath{}
	for i, s := range raw {
		seg, verb, err := parseSegment(s, i == len(raw)-1)
		if err != nil {
			return nil, fmt.Errorf("pattern %q: %w", pattern, err)
		}
		cp.segments = append(cp.segments, seg)
		cp.verb = verb
	}
	literals := 0
	for _, s := range cp.segments {
		if s.kind != segLiteral {
			break
		}
		literals++
	}
	if literals < 2 {
		return nil, fmt.Errorf("pattern %q must start with at least two literal segments and no variable", pattern)
	}
	if !versionRe().MatchString(cp.segments[0].text) && !versionRe().MatchString(cp.segments[1].text) {
		return nil, fmt.Errorf("pattern %q: neither %q nor %q is an API version (vN, vNbetaM, vNalphaM, vNpM)",
			pattern, cp.segments[0].text, cp.segments[1].text)
	}
	return cp, nil
}

// parseSegment parses one segment: a literal, {name}, {name=prefix*}, and an
// optional :verb on the last segment. A stray brace is an error, never a literal.
func parseSegment(raw string, last bool) (segment, string, error) {
	if raw == "" {
		return segment{}, "", fmt.Errorf("empty segment")
	}
	body, verb := raw, ""
	if i := verbIndex(raw); i >= 0 {
		if !last {
			return segment{}, "", fmt.Errorf("segment %q: :verb is only allowed on the last segment", raw)
		}
		body, verb = raw[:i], raw[i+1:]
		if body == "" || verb == "" || strings.ContainsAny(verb, "{}") {
			return segment{}, "", fmt.Errorf("segment %q: incomplete :verb", raw)
		}
	}
	switch {
	case strings.HasPrefix(body, "{"):
		if !strings.HasSuffix(body, "}") {
			return segment{}, "", fmt.Errorf("segment %q: stray {", raw)
		}
		inner := body[1 : len(body)-1]
		if strings.ContainsAny(inner, "{}") {
			return segment{}, "", fmt.Errorf("segment %q: stray { or }", raw)
		}
		name, prefix, ok := strings.Cut(inner, "=")
		if !ok {
			if name == "" {
				return segment{}, "", fmt.Errorf("segment %q: empty capture name", raw)
			}
			return segment{kind: segVar, text: name}, verb, nil
		}
		if !strings.HasSuffix(prefix, "*") {
			return segment{}, "", fmt.Errorf("segment %q: a capture that is not {name} must be {name=prefix*}", raw)
		}
		prefix = strings.TrimSuffix(prefix, "*")
		if name == "" || prefix == "" {
			return segment{}, "", fmt.Errorf("segment %q: {name=prefix*} needs a name and a non-empty prefix", raw)
		}
		return segment{kind: segPrefix, text: name, prefix: prefix}, verb, nil
	case strings.ContainsAny(body, "{}"):
		return segment{}, "", fmt.Errorf("segment %q: stray { or }", raw)
	}
	return segment{kind: segLiteral, text: body}, verb, nil
}

// verbIndex returns the index of the ':' that introduces a verb, or -1. A colon
// inside a capture (a {name=prefix*} prefix) is not a verb.
func verbIndex(s string) int {
	depth, idx := 0, -1
	for i, c := range s {
		switch c {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case ':':
			if depth == 0 {
				idx = i
			}
		}
	}
	return idx
}

// pathsOverlap reports whether two declared patterns can match the same request.
// Overlap is a real predicate, not string equality: a literal matches {var} and
// {prefix*}, two prefix captures overlap when one prefix starts with the other,
// and two {var} captures always overlap. Verbs only match exactly.
func pathsOverlap(a, b *compiledPath) bool {
	if a.method != b.method || len(a.segments) != len(b.segments) || a.verb != b.verb {
		return false
	}
	for i := range a.segments {
		if !segmentsOverlap(a.segments[i], b.segments[i]) {
			return false
		}
	}
	return true
}

func segmentsOverlap(a, b segment) bool {
	switch {
	case a.kind == segVar || b.kind == segVar:
		return true
	case a.kind == segLiteral && b.kind == segLiteral:
		return a.text == b.text
	case a.kind == segLiteral:
		return strings.HasPrefix(a.text, b.prefix)
	case b.kind == segLiteral:
		return strings.HasPrefix(b.text, a.prefix)
	default: // two prefix captures
		return strings.HasPrefix(a.prefix, b.prefix) || strings.HasPrefix(b.prefix, a.prefix)
	}
}
