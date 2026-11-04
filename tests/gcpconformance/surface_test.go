//go:build gcp_conformance

package gcpconformance

import (
	"sort"
	"testing"
)

func routeIndex(t *testing.T) map[string]MethodRoute {
	t.Helper()
	routes := RESTMethodRoutes(loadDocs(t))
	ix := map[string]MethodRoute{}
	seen := map[string]bool{}
	for _, r := range routes {
		key := r.Service + "/" + r.Method
		if seen[key] {
			t.Errorf("duplicate route probe for %s", key)
		}
		seen[key] = true
		ix[key] = r
	}
	return ix
}

// TestRESTMethodRoutesPinned pins the registration boundary the coverage report
// relies on: a method with a route is served, a method without one is visibly
// not served, and the zonal twins the emulator does not serve are not silently
// counted.
func TestRESTMethodRoutesPinned(t *testing.T) {
	ix := routeIndex(t)

	served := []string{
		"container/container.projects.locations.clusters.get",
		"container/container.projects.locations.clusters.create",
		"container/container.projects.zones.getServerconfig",
		// Documented UNIMPLEMENTED: the REST codec claims the path so the
		// serverless client gets 501, not an unknown-service 404.
		"dataproc/dataproc.projects.locations.batches.create",
		"dataproc/dataproc.projects.locations.sessions.create",
	}
	for _, key := range served {
		r, ok := ix[key]
		if !ok {
			t.Errorf("%s: no probe (published method missing from the derivation)", key)
			continue
		}
		if !r.Served {
			t.Errorf("%s: Served = false, want true (handler %q)", key, r.Handler)
		}
	}

	notServed := []string{
		// The REST codec only serves projects.zones.getServerconfig under the
		// legacy zonal spelling; the other zonal twins are unregistered.
		"container/container.projects.zones.clusters.get",
		"container/container.projects.zones.clusters.create",
		// The GKE documented stubs are gRPC-only today: the REST codec has no
		// route for them, so they are not-served on REST (their gRPC cells are
		// graded unsupported).
		"container/container.projects.locations.clusters.checkAutopilotCompatibility",
		"container/container.projects.locations.clusters.getJwks",
		// Dataproc surfaces the emulator does not register at all.
		"dataproc/dataproc.projects.regions.clusters.repair",
		"dataproc/dataproc.projects.regions.clusters.nodeGroups.create",
		"dataproc/dataproc.projects.locations.autoscalingPolicies.list",
	}
	for _, key := range notServed {
		r, ok := ix[key]
		if !ok {
			t.Errorf("%s: no probe (published method missing from the derivation)", key)
			continue
		}
		if r.Served {
			t.Errorf("%s: Served = true (handler %q), want false", key, r.Handler)
		}
	}
}

// TestRESTMethodRoutesCoverEveryPublishedMethod is the completeness invariant:
// every Discovery method id yields exactly one route record. A new method in a
// vendored document therefore cannot be silently dropped from coverage.
func TestRESTMethodRoutesCoverEveryPublishedMethod(t *testing.T) {
	docs := loadDocs(t)
	routes := RESTMethodRoutes(docs)
	ix := map[string]bool{}
	for _, r := range routes {
		ix[r.Service+"\x00"+r.Method] = true
	}
	for svc, doc := range docs {
		doc.WalkMethods(func(m *Method) {
			if m.ID == "" {
				return
			}
			if !ix[svc+"\x00"+m.ID] {
				t.Errorf("%s: published method %s has no route probe", svc, m.ID)
			}
		})
	}
}

// TestPublishedGRPCServices checks the proto descriptor side: the published
// list is the union of the wired services (registered + runtime-unimplemented),
// one row per wire service, and every registered method is published.
func TestPublishedGRPCServices(t *testing.T) {
	published := methodsByWire(PublishedGRPCServices())
	registered := methodsByWire(EnumerateGRPC())

	for wire, methods := range registered {
		pub, ok := published[wire]
		if !ok {
			t.Errorf("%s: registered but not published by any descriptor", wire)
			continue
		}
		for m := range methods {
			if !pub[m] {
				t.Errorf("%s: registered method %s is missing from the published descriptors", wire, m)
			}
		}
	}

	for _, wire := range []string{
		"google.container.v1.ClusterManager",
		"google.cloud.dataproc.v1.ClusterController",
		"google.cloud.dataproc.v1.BatchController",
		"google.cloud.dataproc.v1.SessionController",
		"google.cloud.dataproc.v1.SessionTemplateController",
	} {
		if _, ok := published[wire]; !ok {
			t.Errorf("%s: not in the published gRPC surface", wire)
		}
	}
	if got := len(published["google.container.v1.ClusterManager"]); got != 37 {
		t.Errorf("container ClusterManager published methods = %d, want 37", got)
	}
	if got := len(published["google.cloud.dataproc.v1.BatchController"]); got == 0 {
		t.Error("Dataproc BatchController publishes no methods")
	}
}

// TestEnumerateGRPCUnimplemented pins the runtime-registered, deliberately
// ungraded controllers: they are served (UNIMPLEMENTED) but must never appear in
// the graded surface.
func TestEnumerateGRPCUnimplemented(t *testing.T) {
	unimpl := EnumerateGRPCUnimplemented()
	if len(unimpl) != 3 {
		t.Fatalf("runtime-unimplemented services = %d, want 3", len(unimpl))
	}
	graded := methodsByWire(EnumerateGRPC())
	total := 0
	for _, s := range unimpl {
		if s.Service != "dataproc" {
			t.Errorf("%s: service = %q, want dataproc", s.WireService, s.Service)
		}
		if len(s.Methods) == 0 {
			t.Errorf("%s: no methods", s.WireService)
		}
		total += len(s.Methods)
		for _, m := range s.Methods {
			if graded[s.WireService][m] {
				t.Errorf("%s/%s: runtime-unimplemented method is also in the graded surface", s.WireService, m)
			}
		}
	}
	if total != 14 {
		t.Errorf("runtime-unimplemented RPCs = %d, want 14", total)
	}
}

func methodsByWire(services []GRPCService) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, s := range services {
		if out[s.WireService] == nil {
			out[s.WireService] = map[string]bool{}
		}
		for _, m := range s.Methods {
			out[s.WireService][m] = true
		}
	}
	return out
}

// TestRESTMethodRoutesDeterministic guards the regenerate-and-diff gate: the
// probe list must be sorted and stable across runs.
func TestRESTMethodRoutesDeterministic(t *testing.T) {
	docs := loadDocs(t)
	a, b := RESTMethodRoutes(docs), RESTMethodRoutes(docs)
	if len(a) != len(b) {
		t.Fatalf("route counts differ between runs: %d vs %d", len(a), len(b))
	}
	if !sort.SliceIsSorted(a, func(i, j int) bool {
		if a[i].Service != a[j].Service {
			return a[i].Service < a[j].Service
		}
		return a[i].Method < a[j].Method
	}) {
		t.Error("RESTMethodRoutes is not sorted by (service, method)")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("route %d differs between runs: %+v vs %+v", i, a[i], b[i])
		}
	}
}
