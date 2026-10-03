//go:build gcp_conformance

package main

import (
	"sort"

	conf "jaiscloud/tests/gcpconformance"
)

// mutatingVerbs are the action words that mark an emulator operation as
// state-changing. The list is exactly the one named by the plan (T3):
//
//	Insert, Create, Update, Patch, Delete, Set, Add, Remove, Publish, Commit,
//	Move, Restore, Compose, Enable, Disable, Cancel, Start, Stop, Submit,
//	Import, Export, Attach, Detach
//
// Matching is on CamelCase word boundaries (conf.ActionWords) rather than a raw
// substring, so an action like "GetSettings" is not misread as a "Set" — its
// words are ["Get","Settings"], neither of which is a mutating verb.
var mutatingVerbs = map[string]bool{
	"Insert":  true,
	"Create":  true,
	"Update":  true,
	"Patch":   true,
	"Delete":  true,
	"Set":     true,
	"Add":     true,
	"Remove":  true,
	"Publish": true,
	"Commit":  true,
	"Move":    true,
	"Restore": true,
	"Compose": true,
	"Enable":  true,
	"Disable": true,
	"Cancel":  true,
	"Start":   true,
	"Stop":    true,
	"Submit":  true,
	"Import":  true,
	"Export":  true,
	"Attach":  true,
	"Detach":  true,
}

// isMutating reports whether an action name denotes a state-changing operation.
func isMutating(action string) bool {
	for _, w := range conf.ActionWords(action) {
		if mutatingVerbs[w] {
			return true
		}
	}
	return false
}

// RestFacts builds one Facts per enumerated REST operation from the real
// sources: the registry (ops), the vendored Discovery documents (docs), the
// recorded conformance report (report), and the curated overrides (ov).
//
// Every op yields exactly one fact with Transport "rest" and Implemented true.
// Findings are attributed by the divergence's wire service plus its official
// Discovery method id, mapped back to the registry operation(s) that resolve to
// that method. A divergence whose (service, method) cannot be tied to a
// specific operation is attached to every operation of that service; one whose
// service is unknown is dropped (there is nothing to attach it to). Both cases
// are documented here because that attribution is the only inference the
// generator makes about evidence.
//
// nil docs, report and ov are all tolerated (empty resolution / no findings /
// no overrides respectively), which keeps the function testable without I/O.
func RestFacts(ops []conf.Operation, docs map[string]*conf.DiscoveryDoc, report *conf.Report, ov *Overrides) []Facts {
	resolver := conf.NewActionResolver(docs)
	byMethod := methodIndex(resolver, ops)

	byService := map[string][]conf.Operation{}
	for _, op := range ops {
		byService[op.Service] = append(byService[op.Service], op)
	}

	findings := map[string][]Finding{}
	if report != nil && len(report.Divergences) > 0 {
		allowed := allowlistedSet(report.Divergences)
		for _, d := range report.Divergences {
			f := Finding{Severity: d.Severity, Kind: d.Kind, Path: d.Path, Allowlisted: allowed[d]}

			targets := byMethod[attributionKey(d.Service, d.Method)]
			if len(targets) == 0 {
				// No operation resolves to this Discovery method: fall back to
				// the whole service so the evidence is not lost.
				targets = byService[d.Service]
			}
			for _, op := range targets {
				findings[op.Key()] = append(findings[op.Key()], f)
			}
		}
	}

	facts := make([]Facts, 0, len(ops))
	for _, op := range ops {
		method, _ := resolver.Resolve(op)

		fs := findings[op.Key()]
		sortFindings(fs)

		var override *Override
		if ov != nil {
			override = ov.For(op.Service, op.Key())
		}

		facts = append(facts, Facts{
			Service:           op.Service,
			Operation:         op.Key(),
			Transport:         "rest",
			Implemented:       true,
			DiscoveryMethod:   method,
			Findings:          fs,
			PersistentBackend: persistentBackends[op.Service],
			Mutating:          isMutating(op.Action),
			Override:          override,
		})
	}
	return facts
}

// methodIndex is the inverse of conf.ActionResolver.Resolve: it maps
// (service, Discovery method id) -> the registry operations that resolve to it.
func methodIndex(resolver *conf.ActionResolver, ops []conf.Operation) map[string][]conf.Operation {
	m := map[string][]conf.Operation{}
	for _, op := range ops {
		method, ok := resolver.Resolve(op)
		if !ok {
			continue
		}
		k := attributionKey(op.Service, method)
		m[k] = append(m[k], op)
	}
	return m
}

func attributionKey(service, method string) string { return service + "\x00" + method }

// allowlistedSet runs the conformance allowlist over divs and returns the set
// of divergences it suppresses (keyed by value; Divergence is comparable).
func allowlistedSet(divs []conf.Divergence) map[conf.Divergence]bool {
	_, suppressed := conf.ApplyAllowlist(divs)
	set := make(map[conf.Divergence]bool, len(suppressed))
	for _, d := range suppressed {
		set[d] = true
	}
	return set
}

// sortFindings orders findings deterministically (kind, then path) so repeated
// runs produce byte-identical facts.
func sortFindings(fs []Finding) {
	sort.SliceStable(fs, func(i, j int) bool {
		if fs[i].Kind != fs[j].Kind {
			return fs[i].Kind < fs[j].Kind
		}
		return fs[i].Path < fs[j].Path
	})
}
