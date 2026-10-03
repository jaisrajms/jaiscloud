//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// DiscoveryDoc is the subset of a Google Discovery document this harness needs
// to validate wire responses. Vendored snapshots live under discovery/*.json.gz.
type DiscoveryDoc struct {
	Name        string               `json:"name"`
	Version     string               `json:"version"`
	Title       string               `json:"title"`
	RootURL     string               `json:"rootUrl"`
	ServicePath string               `json:"servicePath"`
	Resources   map[string]*Resource `json:"resources"`
	Schemas     map[string]*Schema   `json:"schemas"`
}

// Resource is a nested Discovery resource: it owns methods and sub-resources.
type Resource struct {
	Methods   map[string]*Method   `json:"methods"`
	Resources map[string]*Resource `json:"resources"`
}

// Method is one REST method declaration.
type Method struct {
	ID         string                `json:"id"`
	Path       string                `json:"path"`
	FlatPath   string                `json:"flatPath"`
	HTTPMethod string                `json:"httpMethod"`
	Parameters map[string]*Parameter `json:"parameters"`
	Request    *Ref                  `json:"request"`
	Response   *Ref                  `json:"response"`

	doc *DiscoveryDoc
}

// Ref is a Discovery "$ref" wrapper (request/response bodies).
type Ref struct {
	Ref string `json:"$ref"`
}

// Parameter is a method parameter declaration (path/query).
type Parameter struct {
	Type     string   `json:"type"`
	Format   string   `json:"format"`
	Required bool     `json:"required"`
	Location string   `json:"location"`
	Enum     []string `json:"enum"`
	Repeated bool     `json:"repeated"`
	Ref      string   `json:"$ref"`
}

// Schema is a Discovery JSON schema. additionalProperties is kept raw because
// Discovery emits it either as a boolean or as a nested schema object.
type Schema struct {
	Ref                  string             `json:"$ref"`
	Type                 string             `json:"type"`
	Format               string             `json:"format"`
	Description          string             `json:"description"`
	Properties           map[string]*Schema `json:"properties"`
	Required             []string           `json:"required"`
	Enum                 []string           `json:"enum"`
	Items                *Schema            `json:"items"`
	AdditionalProperties json.RawMessage    `json:"additionalProperties"`
}

// allowsAdditional reports whether the schema explicitly permits properties
// beyond those declared. Discovery commonly omits additionalProperties; that
// absence is NOT treated as "allowed" here so the harness can flag fields the
// emulator emits that Discovery does not declare (as info-level divergences).
func (s *Schema) allowsAdditional() bool {
	raw := bytes.TrimSpace(s.AdditionalProperties)
	if len(raw) == 0 {
		return false
	}
	return !bytes.Equal(raw, []byte("false"))
}

// additionalSchema returns the schema for additional properties when
// additionalProperties is an object rather than a boolean, else nil.
func (s *Schema) additionalSchema() *Schema {
	raw := bytes.TrimSpace(s.AdditionalProperties)
	if len(raw) == 0 || raw[0] != '{' {
		return nil
	}
	var child Schema
	if err := json.Unmarshal(raw, &child); err != nil {
		return nil
	}
	return &child
}

// ResolveRef resolves a schema reference within the document. Discovery refs
// appear both in short form ("Object") and fully-qualified form
// ("storage.v1.Object" / "cloudkms.projects.locations.KeyRing"); the latter
// is matched by its final dotted segment.
func (d *DiscoveryDoc) ResolveRef(ref string) (*Schema, bool) {
	if ref == "" {
		return nil, false
	}
	if s, ok := d.Schemas[ref]; ok {
		return s, true
	}
	if i := strings.LastIndexByte(ref, '.'); i >= 0 {
		if s, ok := d.Schemas[ref[i+1:]]; ok {
			return s, true
		}
	}
	return nil, false
}

// WalkMethods calls fn for every method in the document (depth-first).
func (d *DiscoveryDoc) WalkMethods(fn func(*Method)) {
	var walk func(map[string]*Resource)
	walk = func(rs map[string]*Resource) {
		keys := make([]string, 0, len(rs))
		for k := range rs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			r := rs[k]
			if r == nil {
				continue
			}
			mkeys := make([]string, 0, len(r.Methods))
			for mk := range r.Methods {
				mkeys = append(mkeys, mk)
			}
			sort.Strings(mkeys)
			for _, mk := range mkeys {
				if m := r.Methods[mk]; m != nil {
					fn(m)
				}
			}
			walk(r.Resources)
		}
	}
	walk(d.Resources)
}

// LoadSnapshots loads and gunzips every *.json.gz in dir, keying the result by
// the file basename (e.g. "storage", "clouddns").
func LoadSnapshots(dir string) (map[string]*DiscoveryDoc, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read discovery dir %s: %w", dir, err)
	}
	docs := map[string]*DiscoveryDoc{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json.gz") {
			continue
		}
		svc := strings.TrimSuffix(e.Name(), ".json.gz")
		doc, err := loadDoc(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		docs[svc] = doc
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("no *.json.gz discovery snapshots found in %s", dir)
	}
	return docs, nil
}

func loadDoc(path string) (*DiscoveryDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	data, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var doc DiscoveryDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	doc.WalkMethods(func(m *Method) { m.doc = &doc })
	return &doc, nil
}

// methodEntry is one matchable (httpMethod, template) pair.
type methodEntry struct {
	doc         *DiscoveryDoc
	service     string
	method      *Method
	template    string // no leading slash
	flat        bool
	literals    int
	literalChar int
	segments    int
}

// MethodIndex matches request paths against the Discovery method templates of
// every loaded document.
type MethodIndex struct {
	entries []methodEntry
}

// BuildMethodIndex flattens all methods (flatPath preferred over path) into a
// matchable index.
func BuildMethodIndex(docs map[string]*DiscoveryDoc) *MethodIndex {
	ix := &MethodIndex{}
	services := make([]string, 0, len(docs))
	for svc := range docs {
		services = append(services, svc)
	}
	sort.Strings(services)
	for _, svc := range services {
		doc := docs[svc]
		doc.WalkMethods(func(m *Method) {
			candidates := []struct {
				tpl  string
				flat bool
			}{
				{m.FlatPath, true},
				{m.Path, false},
			}
			for _, c := range candidates {
				tpl := strings.TrimPrefix(c.tpl, "/")
				if tpl == "" {
					continue
				}
				ix.entries = append(ix.entries, methodEntry{
					doc:         doc,
					service:     svc,
					method:      m,
					template:    tpl,
					flat:        c.flat,
					literals:    countLiterals(tpl),
					literalChar: countLiteralChars(tpl),
					segments:    len(strings.Split(tpl, "/")),
				})
			}
		})
	}
	return ix
}

// MatchMethod finds the Discovery method whose flatPath (preferred) or path
// matches httpMethod + urlPath. Query strings are stripped. Literal segments
// must match exactly; {x} matches exactly one segment; {+x} matches one or more.
func (ix *MethodIndex) MatchMethod(httpMethod, urlPath string) (*DiscoveryDoc, *Method, bool) {
	return ix.match("", httpMethod, urlPath)
}

// MatchMethodInService is like MatchMethod but restricts candidates to the
// document loaded for service (the snapshot basename, e.g. "storage").
func (ix *MethodIndex) MatchMethodInService(service, httpMethod, urlPath string) (*DiscoveryDoc, *Method, bool) {
	return ix.match(service, httpMethod, urlPath)
}

func (ix *MethodIndex) match(service, httpMethod, urlPath string) (*DiscoveryDoc, *Method, bool) {
	if i := strings.IndexAny(urlPath, "?#"); i >= 0 {
		urlPath = urlPath[:i]
	}
	urlPath = strings.TrimPrefix(urlPath, "/")
	segs := splitPathSegments(urlPath)

	best := -1
	bestScore := -1
	for i := range ix.entries {
		e := &ix.entries[i]
		if service != "" && e.service != service {
			continue
		}
		if !strings.EqualFold(e.method.HTTPMethod, httpMethod) {
			continue
		}
		if !matchTemplate(e.template, segs) {
			continue
		}
		score := 0
		if e.flat {
			score += 1 << 20
		}
		score += e.literals << 10
		score += e.literalChar
		score += e.segments
		if score > bestScore {
			bestScore = score
			best = i
		}
	}
	if best < 0 {
		return nil, nil, false
	}
	e := ix.entries[best]
	return e.doc, e.method, true
}

func splitPathSegments(p string) []string {
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func countLiterals(tpl string) int {
	n := 0
	for _, seg := range strings.Split(tpl, "/") {
		if seg != "" && !strings.Contains(seg, "{") {
			n++
		}
	}
	return n
}

// countLiteralChars counts literal characters in a template (everything outside
// {…} placeholders). It lets a template with a literal custom-method suffix
// ("{name}:getIamPolicy") outrank a bare "{name}" placeholder.
func countLiteralChars(tpl string) int {
	n := 0
	for i := 0; i < len(tpl); {
		if tpl[i] == '{' {
			j := strings.IndexByte(tpl[i:], '}')
			if j < 0 {
				break
			}
			i += j + 1
			continue
		}
		n++
		i++
	}
	return n
}

// matchTemplate reports whether the template consumes the path segments from
// some offset through the final segment (the service prefix, which is not part
// of the Discovery template, may precede it).
func matchTemplate(tpl string, segs []string) bool {
	t := strings.Split(tpl, "/")
	for start := 0; start <= len(segs); start++ {
		if matchSegs(t, segs, 0, start) {
			return true
		}
	}
	return false
}

func matchSegs(t, p []string, ti, si int) bool {
	if ti == len(t) {
		return si == len(p)
	}
	ts := t[ti]
	plus, suffix, isPlaceholder := parseTemplateSegment(ts)
	if isPlaceholder && plus {
		for k := 1; si+k <= len(p); k++ {
			joined := strings.Join(p[si:si+k], "/")
			if suffix != "" {
				if !strings.HasSuffix(joined, suffix) || len(joined) == len(suffix) {
					continue
				}
			}
			if matchSegs(t, p, ti+1, si+k) {
				return true
			}
		}
		return false
	}
	if si >= len(p) {
		return false
	}
	if !matchOneSegment(ts, p[si]) {
		return false
	}
	return matchSegs(t, p, ti+1, si+1)
}

// matchOneSegment matches a single template segment against a path segment.
// A placeholder may carry a literal suffix (e.g. "{name}:setIamPolicy").
func matchOneSegment(ts, ps string) bool {
	_, suffix, isPlaceholder := parseTemplateSegment(ts)
	if !isPlaceholder {
		return ts == ps
	}
	if !strings.HasSuffix(ps, suffix) {
		return false
	}
	return len(ps) > len(suffix)
}

// parseTemplateSegment returns (plus, literalSuffix, isPlaceholder) for a
// template segment such as "{bucket}", "{+name}" or "{name}:publish".
func parseTemplateSegment(ts string) (plus bool, suffix string, isPlaceholder bool) {
	if !strings.HasPrefix(ts, "{") {
		return false, "", false
	}
	closeIdx := strings.IndexByte(ts, '}')
	if closeIdx < 0 {
		return false, "", false
	}
	return strings.HasPrefix(ts, "{+"), ts[closeIdx+1:], true
}
