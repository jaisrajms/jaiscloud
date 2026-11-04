//go:build gcp_conformance

package gcpconformance

// This file derives the *served* REST surface: for every method a vendored
// Discovery document publishes, does the emulator register a route for it?
//
// The question is deliberately about registration, not grading. The fidelity
// matrix grades cells (operation + transport), and a service can read as
// complete while most of its published surface has no cell at all. Coverage
// answers the other half: a published method with no registered route is
// reported as not served, and never silently folded into a `ga` rollup.
//
// The probe reuses the Discovery-driven request synthesizer the routing matrix
// uses (synthScenario), feeds the request through the real adapter codec, and
// requires the decoded (service, action) to be a registered provider route.
// A codec that answers a *documented* UNIMPLEMENTED (a `knownUnimplemented`
// path or a fail-loud catch-all action such as Compute.Unimplemented) still
// counts as served: the method is registered, it just is not implemented. A
// decode error that is not `Unimplemented` — UnknownService, UnsupportedOperation,
// InvalidRequest, NotFound — means no route was registered at all.
//
// The generator additionally ORs in the operation registry's own resolved
// method mapping (ActionResolver), so a method the matrix already grades is
// always counted as served even if a synthesized probe cannot express it (a
// body- or host-dependent decode).

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/model"
)

// MethodRoute is the registration status of one published Discovery method.
type MethodRoute struct {
	// Service is the fidelity service name (the Discovery snapshot basename).
	Service string
	// Method is the full Discovery method id, e.g.
	// "container.projects.locations.clusters.get".
	Method string
	// Served reports whether the emulator registers a route for the method.
	Served bool
	// Handler is the registry dispatch key the route resolves to (e.g.
	// "Container.GetCluster", "Dataproc.Serverless", "Compute.Unimplemented").
	// Empty when not served.
	Handler string
	// Unimplemented reports that the registered route answers the documented
	// UNIMPLEMENTED (a fail-loud catch-all or a codec-declared stub). It is
	// only meaningful when Served is true.
	Unimplemented bool
}

// wireToFidelityService maps the emulator's wire service name onto the fidelity
// service name (the Discovery snapshot basename). The three aliases below are
// the product-name snapshots: discovery/clouddns.json.gz is served as "dns",
// cloudsql.json.gz as "sqladmin" and memorystore.json.gz as "redis".
func wireToFidelityService(wire string) string {
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

// RESTMethodRoutes probes every method declared by the vendored Discovery
// documents and reports whether the emulator registers a route for it.
//
// The result is sorted by (service, method) so repeated runs are byte-identical.
// A nil/empty docs map yields no routes (a fresh clone without snapshots still
// generates the matrix; coverage is then simply unavailable).
func RESTMethodRoutes(docs map[string]*DiscoveryDoc) []MethodRoute {
	if len(docs) == 0 {
		return nil
	}

	registered := map[string]bool{}
	for _, op := range Enumerate() {
		registered[op.Key()] = true
	}
	resolver := NewActionResolver(docs)
	resolvedByMethod := map[string]string{}
	for _, op := range Enumerate() {
		if id, ok := resolver.Resolve(op); ok {
			key := op.Service + "\x00" + id
			// Enumerate is sorted, so the first dispatch key wins
			// deterministically when several operations share a method.
			if _, exists := resolvedByMethod[key]; !exists {
				resolvedByMethod[key] = op.Key()
			}
		}
	}

	adapter := gcpadapter.New()

	services := make([]string, 0, len(docs))
	for svc := range docs {
		services = append(services, svc)
	}
	sort.Strings(services)

	var out []MethodRoute
	seen := map[string]bool{}
	for _, svc := range services {
		doc := docs[svc]
		cfg := synthConfigFor(svc)
		doc.WalkMethods(func(m *Method) {
			if m.ID == "" || seen[svc+"\x00"+m.ID] {
				return
			}
			seen[svc+"\x00"+m.ID] = true

			if handler, ok := resolvedByMethod[svc+"\x00"+m.ID]; ok {
				out = append(out, MethodRoute{Service: svc, Method: m.ID, Served: true, Handler: handler})
				return
			}

			served, handler, unimplemented := probeRESTMethod(adapter, registered, svc, cfg, doc, m)
			out = append(out, MethodRoute{
				Service:       svc,
				Method:        m.ID,
				Served:        served,
				Handler:       handler,
				Unimplemented: unimplemented,
			})
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service != out[j].Service {
			return out[i].Service < out[j].Service
		}
		return out[i].Method < out[j].Method
	})
	return out
}

// probeRESTMethod synthesizes the canonical request for one Discovery method
// and asks the real adapter codec whether it routes to a registered handler.
func probeRESTMethod(adapter *gcpadapter.GCPAdapter, registered map[string]bool, svc string, cfg synthService, doc *DiscoveryDoc, m *Method) (served bool, handler string, unimplemented bool) {
	sc := synthScenario(svc, cfg, doc, m, "-coverage", map[string]string{})
	req, err := http.NewRequest(sc.Method, sc.Path, strings.NewReader(sc.Body))
	if err != nil {
		return false, "", false
	}
	if sc.Host != "" {
		req.Host = sc.Host
	}
	if sc.Body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	nr, _, err := adapter.DetectAndDecode(req, []byte(sc.Body))
	if err != nil {
		// A documented UNIMPLEMENTED is a claimed route that fails loud; any
		// other decode error means the path is not claimed by a codec.
		var perr *model.ProviderError
		if errors.As(err, &perr) && perr.Code == "Unimplemented" {
			return true, "", true
		}
		return false, "", false
	}
	// The path must still resolve to the owning service: a shared single-origin
	// path (e.g. locations/{location}/operations) that decodes to another
	// service is not a route for this method.
	if wireToFidelityService(nr.Service) != svc {
		return false, "", false
	}
	key := adapter.ServiceToProvider(nr.Service) + "." + nr.Action
	if !registered[key] {
		return false, "", false
	}
	return true, key, strings.HasSuffix(nr.Action, "Unimplemented")
}
