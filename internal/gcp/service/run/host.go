package run

import "strings"

// DefaultURLSuffix is the DNS suffix of a synthesized service uri/authority when
// no override is configured. It matches the real *.run.app host.
const DefaultURLSuffix = "run.app"

// HostConfig describes how synthesized Cloud Run invocation hosts and URLs are
// built and recognized. Suffix is the DNS suffix after the location label; Port
// is an optional external port appended to the authority ("" leaves it off);
// Scheme defaults to https. In k8s execution mode the scheme is http and Port is
// the externally reachable emulator port so the generated authority can be sent
// back as a Host header and detected by IsInvocationHost.
type HostConfig struct {
	Scheme string
	Suffix string
	Port   string
}

// invocationHost is the process-wide default. The router's host detection and
// the run core's uri synthesis must agree, so both consult it (the core copies
// it at construction unless WithInvocationHost overrides).
var invocationHost = HostConfig{Scheme: "https", Suffix: DefaultURLSuffix}

// ConfigureInvocationHost sets the process-wide invocation host config used by
// IsInvocationHost. Call once at startup before serving.
func ConfigureInvocationHost(c HostConfig) {
	invocationHost = normalizeHostConfig(c)
}

// DefaultInvocationHost returns the process-wide invocation host config.
func DefaultInvocationHost() HostConfig { return invocationHost }

func normalizeHostConfig(c HostConfig) HostConfig {
	out := HostConfig{
		Scheme: strings.ToLower(strings.TrimSpace(c.Scheme)),
		Suffix: strings.Trim(strings.TrimSpace(c.Suffix), "."),
		Port:   strings.TrimPrefix(strings.TrimSpace(c.Port), ":"),
	}
	if out.Scheme == "" {
		out.Scheme = "https"
	}
	if out.Suffix == "" {
		out.Suffix = DefaultURLSuffix
	}
	return out
}

// InvocationAuthority builds the host authority for a service's synthesized
// data-plane host: "{id}-{token}.{location}.{suffix}[:port]".
func InvocationAuthority(project, location, id string) string {
	host := id + "-" + projectToken(project) + "." + strings.ToLower(location) + "." + invocationHost.Suffix
	if invocationHost.Port != "" {
		host += ":" + invocationHost.Port
	}
	return host
}

// InvocationURL builds the full data-plane URL for a service.
func InvocationURL(project, location, id string) string {
	return invocationHost.Scheme + "://" + InvocationAuthority(project, location, id)
}
