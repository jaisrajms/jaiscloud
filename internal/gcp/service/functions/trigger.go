package functions

import (
	"context"
	"errors"
	"net"
	"strings"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
)

// FunctionTriggerDomain is the DNS suffix of the synthesized HTTPS-trigger URL
// for an HTTP-triggered function (see defaultHttpsTriggerURL):
// "{location}-{project}.cloudfunctions.net/{id}".
const FunctionTriggerDomain = "cloudfunctions.net"

// IsTriggerHost reports whether host is a Cloud Functions HTTPS-trigger host.
// Host matching is case-insensitive (DNS names are), and a host:port form (as
// well as the port-less form used in the synthesized URL, which defaults to
// 443) is accepted.
func IsTriggerHost(host string) bool {
	_, ok := TriggerHostLabel(host)
	return ok
}

// TriggerHostLabel returns the "{location}-{project}" label of a trigger host,
// or ok=false when host is not a trigger host. It does NOT resolve the ambiguous
// label: the location and project may both contain hyphens, so ParseTriggerHost
// resolves it against the advertised region catalog when possible, and
// ResolveHTTPTriggerFunction resolves it against the stored functions otherwise.
func TriggerHostLabel(host string) (label string, ok bool) {
	host = strings.ToLower(stripHostPort(host))
	suffix := "." + FunctionTriggerDomain
	if !strings.HasSuffix(host, suffix) {
		return "", false
	}
	label = strings.TrimSuffix(host, suffix)
	if label == "" {
		return "", false
	}
	return label, true
}

// ParseTriggerHost parses a Cloud Functions HTTPS-trigger host of the form
// "{location}-{project}.cloudfunctions.net" into its project and location,
// resolving the location against the advertised function region set. ok is
// false when host is not a trigger host or does not name an advertised region —
// callers should then resolve the ambiguous label against the stored functions
// (ResolveHTTPTriggerFunction), because a function may be created in a region
// outside the advertised set.
func ParseTriggerHost(host string) (project, location string, ok bool) {
	label, ok := TriggerHostLabel(host)
	if !ok {
		return "", "", false
	}
	for _, region := range functionRegions {
		prefix := region + "-"
		if len(label) > len(prefix) && strings.HasPrefix(label, prefix) {
			return label[len(prefix):], region, true
		}
	}
	return "", "", false
}

// ResolveHTTPTriggerFunction finds the function named id whose
// "{location}-{project}" trigger-host label is label, returning its project and
// record. The label is ambiguous on its own — a location and a project may both
// contain hyphens — so every split point is tried; an HTTP-triggered match wins
// over a split that resolves to an event-only function, and an event-only match
// is returned (the caller renders it as NotFound) only when no HTTP-triggered
// function matches. The function regions outside the advertised catalog are
// thus still servable at the URL the emulator synthesized for them.
//
// A well-formed trigger URL that names no function yields NotFound.
func (s *Service) ResolveHTTPTriggerFunction(ctx context.Context, label, id string) (project string, f functionsstore.Function, err error) {
	if label == "" || id == "" {
		return "", functionsstore.Function{}, triggerNotFoundError()
	}
	var eventOnly *struct {
		project string
		f       functionsstore.Function
	}
	for i := 1; i+1 < len(label); i++ {
		if label[i] != '-' {
			continue
		}
		candidateLocation, candidateProject := label[:i], label[i+1:]
		found, gerr := s.functions.GetFunction(ctx, candidateProject, candidateLocation, id)
		if gerr != nil {
			if errors.Is(gerr, functionsstore.ErrNoSuchFunction) {
				continue
			}
			return "", functionsstore.Function{}, gerr
		}
		if found.HttpsTriggerURL != "" {
			return candidateProject, found, nil
		}
		if eventOnly == nil {
			eventOnly = &struct {
				project string
				f       functionsstore.Function
			}{candidateProject, found}
		}
	}
	if eventOnly != nil {
		return eventOnly.project, eventOnly.f, nil
	}
	return "", functionsstore.Function{}, triggerNotFoundError()
}

// triggerNotFoundError is the canonical NotFound error for a trigger URL that
// serves no function.
func triggerNotFoundError() error {
	return model.NewProviderError("NotFound", "function not found", 404)
}

// stripHostPort removes a trailing ":port" from a request host, tolerating the
// port-less form (which no net.SplitHostPort accepts) and bracketed IPv6.
func stripHostPort(host string) string {
	if host == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}
