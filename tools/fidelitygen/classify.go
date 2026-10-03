//go:build gcp_conformance

package main

import (
	"fmt"
	"sort"
)

// Fidelity states, ordered best (ga) to worst (unsupported).
const (
	StateGA          = "ga"
	StateLimited     = "limited"
	StatePreview     = "preview"
	StateUnsupported = "unsupported"
)

// stateRank orders the states so an override can be classified as an upgrade
// (raises the state) or a downgrade (lowers it).
var stateRank = map[string]int{
	StateUnsupported: 0,
	StatePreview:     1,
	StateLimited:     2,
	StateGA:          3,
}

// ValidState reports whether s is a known fidelity state.
func ValidState(s string) bool {
	_, ok := stateRank[s]
	return ok
}

var severityRank = map[string]int{"info": 0, "low": 1, "medium": 2, "high": 3}

// Finding is one wire-conformance divergence relevant to a cell.
type Finding struct {
	Severity    string // high | medium | low | info
	Kind        string
	Path        string
	Allowlisted bool
}

// Override is an explicit, curated classification for a cell.
type Override struct {
	State        string
	Reason       string
	AllowUpgrade bool
}

// Facts are the derived inputs for a single (service, operation, transport)
// cell. Everything here comes from the emulator's own registries plus the
// conformance evidence — never from hand-maintained lists.
type Facts struct {
	Service           string
	Operation         string
	Transport         string // "rest" | "grpc"
	Implemented       bool
	DiscoveryMethod   string // "" when no Discovery method matches (REST)
	Findings          []Finding
	PersistentBackend bool // service has a Postgres backend + snapshot
	Mutating          bool
	Override          *Override
	// GRPCChecksPassed / GRPCChecksTotal record the gRPC conformance evidence
	// behind this cell: the pass count and total count of report checks for
	// (service, operation). Total 0 means the report does not cover the method
	// (unverified); Passed == Total > 0 means every check passed (verified).
	// Both are zero for non-gRPC transports.
	GRPCChecksPassed int
	GRPCChecksTotal  int
}

// Cell is one row of the fidelity matrix.
type Cell struct {
	Service   string   `json:"service"`
	Operation string   `json:"operation"`
	Transport string   `json:"transport"`
	State     string   `json:"state"`
	Reason    string   `json:"reason,omitempty"`
	Evidence  []string `json:"evidence,omitempty"`
}

// Classify turns Facts into a Cell. An override wins, but may only downgrade
// the derived state unless AllowUpgrade is set; an upgrade without it is
// rejected and recorded as evidence.
func Classify(f Facts) Cell {
	state, reason := deriveState(f)

	cell := Cell{Service: f.Service, Operation: f.Operation, Transport: f.Transport}
	switch ov := f.Override; {
	case ov == nil:
		cell.State, cell.Reason = state, reason
	case !ValidState(ov.State):
		// Defensive: overrides are validated up front, but never emit a bogus state.
		cell.State, cell.Reason = state, reason
		cell.Evidence = append(cell.Evidence, "ignored override with invalid state "+ov.State)
	case stateRank[ov.State] <= stateRank[state] || ov.AllowUpgrade:
		cell.State, cell.Reason = ov.State, ov.Reason
	default:
		cell.State, cell.Reason = state, reason
		cell.Evidence = append(cell.Evidence,
			fmt.Sprintf("override to %s rejected (needs allow_upgrade)", ov.State))
	}

	if cell.State != StateGA && cell.Reason == "" {
		cell.Reason = "unspecified"
	}
	cell.Evidence = append(evidenceFor(f), cell.Evidence...)
	return cell
}

// deriveState implements the derivation rules (plan §5) without overrides.
func deriveState(f Facts) (state, reason string) {
	if !f.Implemented {
		return StateUnsupported, "not implemented"
	}
	if fd, ok := worstUnallowedFinding(f.Findings); ok {
		return StateLimited, fmt.Sprintf("wire divergence (%s %s at %s)", fd.Severity, fd.Kind, fd.Path)
	}
	if f.Transport == "rest" && f.DiscoveryMethod == "" {
		return StateLimited, "no matching Discovery method (unverified against the official schema)"
	}
	if f.Mutating && !f.PersistentBackend {
		return StateLimited, "mutating operation without a Postgres backend / snapshot"
	}
	return StateGA, ""
}

// worstUnallowedFinding returns the highest-severity non-allowlisted finding at
// medium or above (the threshold that downgrades a cell).
func worstUnallowedFinding(fs []Finding) (Finding, bool) {
	var best Finding
	found := false
	for _, f := range fs {
		if f.Allowlisted || severityRank[f.Severity] < severityRank["medium"] {
			continue
		}
		if !found || severityRank[f.Severity] > severityRank[best.Severity] {
			best, found = f, true
		}
	}
	return best, found
}

// evidenceFor records the inputs that shaped a cell, for the report.
func evidenceFor(f Facts) []string {
	var ev []string
	if !f.Implemented {
		ev = append(ev, "not in registry")
	}
	if f.Transport == "grpc" {
		if f.GRPCChecksTotal > 0 {
			ev = append(ev, fmt.Sprintf("transport=grpc (conformance=%d/%d pass)",
				f.GRPCChecksPassed, f.GRPCChecksTotal))
		} else {
			ev = append(ev, "transport=grpc (proto-conformance pending)")
		}
	}
	if f.DiscoveryMethod != "" {
		ev = append(ev, "discovery="+f.DiscoveryMethod)
	}
	if f.Mutating {
		ev = append(ev, "mutating")
	}
	if f.PersistentBackend {
		ev = append(ev, "postgres=yes")
	}
	sort.Strings(ev)
	return ev
}
