//go:build gcp_conformance

package main

import (
	"fmt"
	"sort"
	"strings"

	conf "jaiscloud/tests/gcpconformance"
)

// Coverage-source values. A coverage record is a statement about the published
// surface, not a grade: `served` means a route/RPC is registered (it may answer
// a documented UNIMPLEMENTED and be graded `unsupported`), and `graded` counts
// the published methods that have a matrix cell.
const (
	CoverageDiscovery   = "discovery"
	CoverageProto       = "proto"
	CoverageUnavailable = "unavailable"
)

// CoverageEntry compares one service's published method list — the vendored
// Discovery document for REST, the proto service descriptors for gRPC — with
// the routes the emulator actually registers and the methods the matrix grades.
//
// It exists because a cell-only matrix makes absence read as completeness: a
// service with 60 `ga` cells can still publish 71 REST methods, 40 of which
// have no route and no cell. The entry never adds or re-grades a cell; it only
// states how much of the published surface the cells cover.
type CoverageEntry struct {
	Service   string `json:"service"`
	Transport string `json:"transport"` // "rest" | "grpc"
	// Source is discovery, proto or unavailable.
	Source string `json:"source"`
	// SourceRef names the document/descriptors the published list came from,
	// e.g. "container.v1" or "google.container.v1.ClusterManager".
	SourceRef string `json:"source_ref,omitempty"`
	// Published is the number of methods in the source of truth. Zero when the
	// source is unavailable.
	Published int `json:"published"`
	// Served is the number of published methods with a registered route/RPC.
	Served int `json:"served"`
	// Graded is the number of published methods with a matrix cell.
	Graded int `json:"graded"`
	// Unserved lists every published method with no registered route. It is
	// the machine-readable "visibly not served" list; the markdown renders it.
	Unserved []string `json:"unserved,omitempty"`
	// Ungraded is how many served methods have no cell (served but ungraded).
	Ungraded int `json:"ungraded,omitempty"`
	// Unimplemented is the subset of Ungraded the emulator serves only through
	// a documented UNIMPLEMENTED (a fail-loud catch-all or a runtime-registered
	// unimplemented controller). Those are served/unsupported, never absent.
	Unimplemented int `json:"unimplemented,omitempty"`
	// Note explains an unavailable source or a caveat.
	Note string `json:"note,omitempty"`
}

// BuildCoverage derives one entry per (service, transport) from the real
// sources: the cells already classified, the vendored Discovery documents, the
// routed-method probe, and the proto descriptors. It never mutates the cells.
func BuildCoverage(cells []Cell, docs map[string]*conf.DiscoveryDoc) []CoverageEntry {
	routes := conf.RESTMethodRoutes(docs)
	grpcPublished := conf.PublishedGRPCServices()
	grpcUnimplemented := conf.EnumerateGRPCUnimplemented()
	grpcServed := append(conf.EnumerateGRPC(), grpcUnimplemented...)

	services := map[string]bool{}
	for _, c := range cells {
		services[c.Service] = true
	}
	for svc := range docs {
		services[svc] = true
	}
	for _, s := range grpcPublished {
		services[s.Service] = true
	}
	for _, s := range grpcServed {
		services[s.Service] = true
	}
	svs := make([]string, 0, len(services))
	for svc := range services {
		svs = append(svs, svc)
	}
	sort.Strings(svs)

	cov := make([]CoverageEntry, 0, len(svs)*2)
	for _, svc := range svs {
		cov = append(cov, restCoverage(svc, cells, docs[svc], routes))
		cov = append(cov, buildGRPCCoverage(svc, cells, grpcPublished, grpcServed, grpcUnimplemented))
	}

	sort.Slice(cov, func(i, j int) bool {
		if cov[i].Service != cov[j].Service {
			return cov[i].Service < cov[j].Service
		}
		return transportRank(cov[i].Transport) < transportRank(cov[j].Transport)
	})
	return cov
}

// transportRank orders REST before gRPC in the coverage table.
func transportRank(t string) int {
	if t == "rest" {
		return 0
	}
	return 1
}

func restCoverage(svc string, cells []Cell, doc *conf.DiscoveryDoc, routes []conf.MethodRoute) CoverageEntry {
	e := CoverageEntry{Service: svc, Transport: "rest"}
	if doc == nil {
		e.Source = CoverageUnavailable
		e.Note = "no vendored Discovery document describes this service, so its published REST surface cannot be derived; the cells below are not a coverage claim"
		return e
	}

	published := map[string]bool{}
	doc.WalkMethods(func(m *conf.Method) {
		if m.ID != "" {
			published[m.ID] = true
		}
	})
	e.Source = CoverageDiscovery
	e.SourceRef = doc.Name + "." + doc.Version
	e.Published = len(published)

	unsupportedHandlers := map[string]bool{}
	for _, c := range cells {
		if c.Transport == "rest" && c.State == StateUnsupported {
			unsupportedHandlers[c.Operation] = true
		}
	}

	served := map[string]bool{}
	unimplemented := map[string]bool{}
	for _, r := range routes {
		if r.Service != svc || !published[r.Method] {
			continue
		}
		if r.Served {
			served[r.Method] = true
			// A route dispatched to a cell graded `unsupported` (or to a
			// catch-all Unimplemented handler) answers the documented
			// UNIMPLEMENTED: served, not absent.
			if r.Unimplemented || unsupportedHandlers[r.Handler] {
				unimplemented[r.Method] = true
			}
		}
	}
	graded := gradedRESTMethods(svc, cells)

	e.Served = len(served)
	e.Graded = countIn(published, graded)
	e.Unserved = unservedList(published, served)
	e.Ungraded = e.Served - countIn(served, graded)
	for m := range served {
		if unimplemented[m] && !graded[m] {
			e.Unimplemented++
		}
	}
	e.Note = restCoverageNote(e, doc)
	return e
}

func buildGRPCCoverage(svc string, cells []Cell, publishedServices, servedServices, unimplementedServices []conf.GRPCService) CoverageEntry {
	e := CoverageEntry{Service: svc, Transport: "grpc"}
	published := methodsByService(publishedServices)[svc]
	if len(published) == 0 {
		e.Source = CoverageUnavailable
		e.Note = "no gRPC service is registered for this service, so its published RPC surface cannot be derived; its surface is REST-only in the matrix"
		return e
	}

	served := methodsByService(servedServices)[svc]
	unimplemented := methodsByService(unimplementedServices)[svc]
	graded := gradedGRPCMethods(svc, cells)

	e.Source = CoverageProto
	e.SourceRef = strings.Join(wireServicesFor(svc, publishedServices), ", ")
	e.Published = len(published)
	e.Served = countIn(published, served)
	e.Graded = countIn(published, graded)
	e.Unserved = unservedList(published, served)
	e.Ungraded = e.Served - countIn(served, graded)
	for m := range served {
		if unimplemented[m] && !graded[m] {
			e.Unimplemented++
		}
	}
	if e.Ungraded > 0 {
		e.Note = fmt.Sprintf("%d served RPC(s) have no graded cell; a registered RPC without a cell answers the documented UNIMPLEMENTED", e.Ungraded)
	}
	return e
}

// gradedRESTMethods is the set of Discovery method ids the REST cells name.
// A cell with no Discovery method does not grade a published method.
func gradedRESTMethods(svc string, cells []Cell) map[string]bool {
	out := map[string]bool{}
	for _, c := range cells {
		if c.Service == svc && c.Transport == "rest" && c.DiscoveryMethod != "" {
			out[c.DiscoveryMethod] = true
		}
	}
	return out
}

func gradedGRPCMethods(svc string, cells []Cell) map[string]bool {
	out := map[string]bool{}
	for _, c := range cells {
		if c.Service == svc && c.Transport == "grpc" {
			out[c.Operation] = true
		}
	}
	return out
}

func methodsByService(services []conf.GRPCService) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, s := range services {
		if out[s.Service] == nil {
			out[s.Service] = map[string]bool{}
		}
		for _, m := range s.Methods {
			out[s.Service][m] = true
		}
	}
	return out
}

func wireServicesFor(svc string, services []conf.GRPCService) []string {
	var wires []string
	for _, s := range services {
		if s.Service == svc && s.WireService != "" {
			wires = append(wires, s.WireService)
		}
	}
	sort.Strings(wires)
	return wires
}

func countIn(set, subset map[string]bool) int {
	n := 0
	for m := range subset {
		if set[m] {
			n++
		}
	}
	return n
}

// unservedList is the sorted published-not-served list.
func unservedList(published, served map[string]bool) []string {
	var out []string
	for m := range published {
		if !served[m] {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

func restCoverageNote(e CoverageEntry, doc *conf.DiscoveryDoc) string {
	var notes []string
	if e.Ungraded > 0 {
		note := fmt.Sprintf("%d served method(s) have no graded cell", e.Ungraded)
		if e.Unimplemented > 0 {
			note += fmt.Sprintf("; %d of them are served only by a documented UNIMPLEMENTED stub (served/unsupported, never absent)", e.Unimplemented)
		} else {
			note += " (their routes are aggregated under one cell)"
		}
		notes = append(notes, note)
	}
	if doc.Name == "firestore" {
		notes = append(notes, "the Firestore Discovery document also declares the Firestore Admin methods, which this REST wire service serves; the Admin gRPC service is reported separately as firestoreadmin")
	}
	return strings.Join(notes, "; ")
}

// ValidateCoverage is the generator gate: every cell's (service, transport)
// must have a coverage record, and a record that claims a derivable source must
// be non-empty. A service can therefore never be added to the matrix without
// saying how much of its published surface it serves.
func ValidateCoverage(cells []Cell, cov []CoverageEntry) error {
	byKey := map[string]CoverageEntry{}
	for _, e := range cov {
		byKey[e.Service+"\x00"+e.Transport] = e
	}
	for _, c := range cells {
		e, ok := byKey[c.Service+"\x00"+c.Transport]
		if !ok {
			return fmt.Errorf("service %q transport %q has cells but no coverage record", c.Service, c.Transport)
		}
		if e.Source == "" {
			return fmt.Errorf("service %q transport %q: coverage record has no source", c.Service, c.Transport)
		}
		if (e.Source == CoverageDiscovery || e.Source == CoverageProto) && e.Published == 0 {
			return fmt.Errorf("service %q transport %q: source %s yields no published methods", c.Service, c.Transport, e.Source)
		}
		if e.Source == CoverageUnavailable && e.Note == "" {
			return fmt.Errorf("service %q transport %q: unavailable source must state why", c.Service, c.Transport)
		}
		if e.Served > e.Published || e.Graded > e.Published {
			return fmt.Errorf("service %q transport %q: served=%d graded=%d exceed published=%d", c.Service, c.Transport, e.Served, e.Graded, e.Published)
		}
	}
	return nil
}

// coverageSummary renders the one-line per-service coverage sentence(s) used in
// the markdown and the generator's stdout.
func coverageSummary(e CoverageEntry) string {
	switch e.Source {
	case CoverageUnavailable:
		return fmt.Sprintf("%s: coverage unavailable — %s", strings.ToUpper(e.Transport), e.Note)
	default:
		s := fmt.Sprintf("%s: **%d of %d** published methods served", strings.ToUpper(e.Transport), e.Served, e.Published)
		if len(e.Unserved) > 0 {
			s += fmt.Sprintf(" · %d not served", len(e.Unserved))
		}
		s += fmt.Sprintf(" · %d graded", e.Graded)
		if e.Ungraded > 0 {
			s += fmt.Sprintf(" · %d served-but-ungraded", e.Ungraded)
		}
		if e.Source == CoverageDiscovery {
			return s + fmt.Sprintf(" (`%s` Discovery)", e.SourceRef)
		}
		return s + fmt.Sprintf(" (`%s` proto)", e.SourceRef)
	}
}
