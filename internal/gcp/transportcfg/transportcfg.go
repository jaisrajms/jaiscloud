// Package transportcfg resolves which GCP wire transports (REST and/or gRPC)
// the emulator exposes, globally and per service.
//
// A deployment selects a global default with --transports
// (JAISCLOUD_TRANSPORTS) and may refine it per service with
// --transport-overrides (JAISCLOUD_TRANSPORT_OVERRIDES). Only the listeners and
// route surfaces for a selected transport are started, so a REST-only or
// gRPC-only user never has to run the other transport's listener.
package transportcfg

import (
	"fmt"
	"sort"
	"strings"
)

// Transport tokens accepted by the parser.
const (
	REST = "rest"
	GRPC = "grpc"
)

// Selection is the resolved, per-service transport enablement.
type Selection struct {
	rest map[string]bool
	grpc map[string]bool
}

// Parse resolves the global default and per-service overrides against the
// known service names. An empty global default enables both transports. Values
// are comma-separated tokens: "rest"/"http", "grpc", "both"/"all", or "none".
func Parse(global, overrides string, known []string) (*Selection, error) {
	if len(known) == 0 {
		return nil, fmt.Errorf("transport selection: no services are registered")
	}
	rest, grpc, err := parseTokens(global)
	if err != nil {
		return nil, fmt.Errorf("invalid --transports %q: %w", global, err)
	}
	sel := &Selection{rest: make(map[string]bool, len(known)), grpc: make(map[string]bool, len(known))}
	for _, svc := range known {
		sel.rest[svc] = rest
		sel.grpc[svc] = grpc
	}

	if strings.TrimSpace(overrides) == "" {
		return sel, nil
	}
	for _, entry := range strings.Split(overrides, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, val, ok := strings.Cut(entry, "=")
		name = strings.TrimSpace(name)
		val = strings.TrimSpace(val)
		if !ok || name == "" || val == "" {
			return nil, fmt.Errorf("invalid --transport-overrides entry %q: want service=rest|grpc|both|none", entry)
		}
		if _, exists := sel.rest[name]; !exists {
			return nil, fmt.Errorf("invalid --transport-overrides: unknown service %q", name)
		}
		r, g, err := parseTokens(val)
		if err != nil {
			return nil, fmt.Errorf("invalid --transport-overrides for %q: %w", name, err)
		}
		sel.rest[name], sel.grpc[name] = r, g
	}
	return sel, nil
}

// parseTokens parses a comma-separated transport token list. An empty string
// means "both" (the default).
func parseTokens(s string) (rest, grpc bool, err error) {
	if strings.TrimSpace(s) == "" {
		return true, true, nil
	}
	for _, tok := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(tok)) {
		case "":
			// tolerate stray separators
		case "all", "both":
			rest, grpc = true, true
		case "rest", "http":
			rest = true
		case "grpc":
			grpc = true
		case "none":
			// leaves both false
		default:
			return false, false, fmt.Errorf("unknown transport %q (want rest|grpc|both|none)", tok)
		}
	}
	return rest, grpc, nil
}

// REST reports whether any service has the REST transport enabled.
func (s *Selection) REST() bool { return anyEnabled(s.rest) }

// GRPC reports whether any service has the gRPC transport enabled.
func (s *Selection) GRPC() bool { return anyEnabled(s.grpc) }

// RESTFor reports whether a service has the REST transport enabled.
func (s *Selection) RESTFor(service string) bool { return s.rest[service] }

// GRPCFor reports whether a service has the gRPC transport enabled.
func (s *Selection) GRPCFor(service string) bool { return s.grpc[service] }

// Services returns the sorted service names known to the selection.
func (s *Selection) Services() []string {
	out := make([]string, 0, len(s.rest))
	for svc := range s.rest {
		out = append(out, svc)
	}
	sort.Strings(out)
	return out
}

// String renders the selection for logging, e.g. "rest=[a b] grpc=[a b c]".
func (s *Selection) String() string {
	var restSvc, grpcSvc []string
	for _, svc := range s.Services() {
		if s.rest[svc] {
			restSvc = append(restSvc, svc)
		}
		if s.grpc[svc] {
			grpcSvc = append(grpcSvc, svc)
		}
	}
	return fmt.Sprintf("rest=%v grpc=%v", restSvc, grpcSvc)
}

func anyEnabled(m map[string]bool) bool {
	for _, v := range m {
		if v {
			return true
		}
	}
	return false
}
