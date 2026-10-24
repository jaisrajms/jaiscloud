//go:build gcp_conformance

package gcpconformance

import (
	"net/http"
	"sort"
	"strings"
	"testing"

	gcpadapter "jaiscloud/internal/gcp/adapter"
)

// docServiceToWire maps a vendored Discovery snapshot basename to the emulator's
// wire service name. Most snapshots are already named for the wire service; the
// three below use the upstream product name instead.
var docServiceToWire = map[string]string{
	"clouddns":    "dns",
	"cloudsql":    "sqladmin",
	"memorystore": "redis",
}

// wireServiceToDoc is the inverse of docServiceToWire (identity otherwise).
func wireServiceToDoc(wire string) string {
	switch wire {
	case "dns":
		return "clouddns"
	case "sqladmin":
		return "cloudsql"
	case "redis":
		return "memorystore"
	}
	return wire
}

// wireServiceFor resolves the emulator wire service for a snapshot basename
// (identity for the names that already match).
func wireServiceFor(docService string) string {
	if w, ok := docServiceToWire[docService]; ok {
		return w
	}
	return docService
}

// routeProbe is one (method, path, host) request shape plus the emulator service
// it must route to. methodID is the Discovery method the shape was derived from
// ("" when it could not be resolved), which keys the documented exemptions.
type routeProbe struct {
	service  string // expected emulator wire service
	method   string
	path     string
	host     string
	methodID string
	source   string
}

// routeExemption documents a request shape whose Discovery-derived owner is not
// the emulator's default path owner on the single shared origin. owner is the
// service the request is intentionally routed to; match selects the shape and
// reason says why. An entry that never matches a probe fails the matrix (stale
// exemption).
type routeExemption struct {
	service string // expected wire service (from Discovery)
	owner   string // emulator service actually routed to
	reason  string
	match   func(p routeProbe) bool
	seen    bool
}

// locationsCollectionProxy selects the bare /v1/projects/{p}/locations[/{l}]
// location-discovery shapes (the common google.cloud.location surface that
// several service documents declare on the same path).
func locationsCollectionProxy(p routeProbe) bool {
	if p.method != http.MethodGet || !strings.Contains(p.path, "/locations") {
		return false
	}
	rest := strings.TrimPrefix(strings.SplitN(p.path, "/locations", 2)[1], "/")
	return rest == "" || !strings.Contains(rest, "/")
}

// operationsProxy selects the shared location-scoped operations family.
func operationsProxy(method string) func(routeProbe) bool {
	return func(p routeProbe) bool {
		return p.method == method && strings.HasSuffix(p.path, "/operations") ||
			p.method == method && strings.Contains(p.path, "/operations/")
	}
}

// routeExemptions are the intentional single-origin shared-path owners. Each is
// a place where two real-GCP services declare the same path and the emulator's
// path-only resolver must pick one owner; the alternative owner is reachable via
// its canonical host (or a distinguishing id) where the emulator models one.
var routeExemptions = []*routeExemption{
	{
		service: "functions",
		owner:   "redis",
		reason:  "bare /v1/projects/{p}/locations[/{l}] is claimed by Memorystore's location discovery (the only registered bare-locations owner); Functions clients reach it via cloudfunctions.googleapis.com",
		match:   locationsCollectionProxy,
	},
	{
		service: "run",
		owner:   "functions",
		reason:  "the shared /v2/.../operations path stays on Cloud Functions; Run operations are returned inline and reachable by their run-owned 'operation-run-' prefixed id (documented in detectV2Service)",
		match:   operationsProxy(http.MethodGet),
	},
	{
		service: "run",
		owner:   "functions",
		reason:  "a run operation get/delete id carries the run-owned 'operation-run-' prefix; the synthetic probe id does not, so the shared path resolves to Functions as documented",
		match:   operationsProxy(http.MethodDelete),
	},
}

// methodIDFor resolves the Discovery method id a probe path matches within the
// snapshot of its expected wire service ("" when none matches). It walks the
// document's own methods (not BuildMethodIndex, which adds a synthesized
// google.cloud.location method that would shadow the real one).
func methodIDFor(docs map[string]*DiscoveryDoc, wireService, method, path string) string {
	doc := docs[wireServiceToDoc(wireService)]
	if doc == nil {
		return ""
	}
	if i := strings.IndexAny(path, "?#"); i >= 0 {
		path = path[:i]
	}
	segs := splitPathSegments(strings.TrimPrefix(path, "/"))
	best, bestScore := "", -1
	doc.WalkMethods(func(m *Method) {
		if m.ID == "" || !strings.EqualFold(m.HTTPMethod, method) {
			return
		}
		tpl := templateOf(m)
		if !matchTemplate(tpl, segs) {
			return
		}
		score := countLiterals(tpl)<<10 + countLiteralChars(tpl) + len(strings.Split(tpl, "/"))
		if score > bestScore {
			bestScore, best = score, m.ID
		}
	})
	return best
}

// restRouteProbes enumerates the request shapes the emulator's REST adapter must
// route correctly: the curated real-client flows, the fixed prelude resources,
// and one synthesized request per registry operation that resolves to a
// Discovery method (the exhaustive Discovery-derived set).
func restRouteProbes(t *testing.T) []routeProbe {
	t.Helper()
	docs := loadDocs(t)

	var probes []routeProbe
	add := func(src string, scs []Scenario) {
		for _, sc := range scs {
			// A path that references a captured ${var} is a stateful flow
			// template, not a concrete request shape; its resource-level route
			// is covered by the Discovery-synthesized probes instead.
			if strings.Contains(sc.Path, "${") {
				continue
			}
			wire := wireServiceFor(sc.Service)
			probes = append(probes, routeProbe{
				service:  wire,
				method:   sc.Method,
				path:     sc.Path,
				host:     sc.Host,
				methodID: methodIDFor(docs, wire, sc.Method, sc.Path),
				source:   src,
			})
		}
	}
	add("curated", Scenarios("-route"))
	add("prelude", SynthPrelude())
	add("synth", SynthScenarios(docs, "-route"))
	return probes
}

// TestRESTRoutingMatrix asserts every request shape a real client emits routes
// to its owning provider. The curated flows and the Discovery-synthesized probe
// set together cover every registered operation, so a heuristic that mis-claims
// a shared path is caught here. The routeExemptions table records the
// intentional single-origin shared-path owners; it fails if an entry goes stale.
func TestRESTRoutingMatrix(t *testing.T) {
	probes := restRouteProbes(t)
	if len(probes) == 0 {
		t.Fatal("no route probes built")
	}

	type mismatch struct {
		probe routeProbe
		got   string
	}
	var bad []mismatch
	knownServices := map[string]bool{}
	for _, s := range gcpadapter.KnownServiceNames() {
		knownServices[s] = true
	}
	for _, p := range probes {
		r, err := http.NewRequest(p.method, p.path, nil)
		if err != nil {
			t.Fatalf("build request %s %s: %v", p.method, p.path, err)
		}
		if p.host != "" {
			r.Host = p.host
		}
		got, _ := gcpadapter.DetectService(r)
		if got == p.service {
			// The detected service must map to a registered provider so the
			// registry dispatch key is well-formed.
			if got != "" && !knownServices[got] {
				t.Errorf("probe %s %s routed to %q, which is not a known service", p.method, p.path, got)
			}
			continue
		}
		exempted := false
		for _, e := range routeExemptions {
			if e.service == p.service && got == e.owner && e.match(p) {
				e.seen = true
				exempted = true
				break
			}
		}
		if exempted {
			continue
		}
		bad = append(bad, mismatch{probe: p, got: got})
	}

	for _, e := range routeExemptions {
		if !e.seen {
			t.Errorf("stale route exemption: %s -> %s (%s) never matched a probe",
				e.service, e.owner, e.reason)
		}
	}

	if len(bad) > 0 {
		sort.Slice(bad, func(i, j int) bool {
			if bad[i].probe.service != bad[j].probe.service {
				return bad[i].probe.service < bad[j].probe.service
			}
			return bad[i].probe.path < bad[j].probe.path
		})
		for _, b := range bad {
			t.Errorf("mis-route: %s %s (host %q, %s, method %s) routed to %q, want %q",
				b.probe.method, b.probe.path, b.probe.host, b.probe.source, b.probe.methodID, b.got, b.probe.service)
		}
		t.Fatalf("%d/%d routing probes mis-routed", len(bad), len(probes))
	}
	t.Logf("routing matrix: %d probes routed correctly (%d documented exemptions)", len(probes), len(routeExemptions))
}
