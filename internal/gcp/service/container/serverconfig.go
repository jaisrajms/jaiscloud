package container

import (
	"strings"
)

// ServerConfig is google.container.v1.ServerConfig: the Kubernetes version and
// node-image catalog the service supports, plus its release channels. It is
// derived (the emulator has no version catalog to store), so GetServerConfig
// builds a fresh one per call.
type ServerConfig struct {
	DefaultClusterVersion string
	ValidMasterVersions   []string
	ValidNodeVersions     []string
	DefaultImageType      string
	ValidImageTypes       []string
	Channels              []ReleaseChannelConfig
}

// ReleaseChannelConfig is google.container.v1.ServerConfig.ReleaseChannelConfig.
type ReleaseChannelConfig struct {
	Channel        string
	DefaultVersion string
	ValidVersions  []string
}

// ResolveClusterVersion resolves a requested Cluster.initialClusterVersion to
// the concrete release the service will run. Real GKE accepts a full version,
// the `latest` alias, and the `1` / `1.X` prefix forms, and rejects anything it
// does not serve with `Master version "<v>" is unsupported.` — it never silently
// substitutes a different release. The emulator serves one version, so those
// prefix forms resolve against it and every other request is rejected rather
// than rewritten (a create pinned to a version the mock does not run must fail
// the same way real GKE fails it).
func ResolveClusterVersion(requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	switch requested {
	case "", "latest":
		return DefaultMasterVersion, nil
	}
	if requested == DefaultMasterVersion {
		return DefaultMasterVersion, nil
	}
	// Accept the documented prefix aliases (1, 1.30, 1.30.5) when they are a
	// whole-segment prefix of the release actually served. The `-gke.N` build
	// suffix is not part of the alias space, so only the plain version part is
	// prefix-matched.
	base, _, _ := strings.Cut(DefaultMasterVersion, "-")
	if requested == base || strings.HasPrefix(base, requested+".") {
		return DefaultMasterVersion, nil
	}
	// Real GKE distinguishes a release it does not serve from a prefix that
	// matches no version at all (both captured 2026-11-03): a full release
	// string gets `Master version "<v>" is unsupported.`, while a prefix gets
	// `No valid versions with the prefix "<v>" found.`
	if !strings.Contains(requested, "-") {
		return "", invalidArgument(`No valid versions with the prefix "` + requested + `" found.`)
	}
	return "", invalidArgument(`Master version "` + requested + `" is unsupported.`)
}

// ServerConfigValidImageTypes is ServerConfig.validImageTypes: the node images a
// Standard cluster accepts, the default first.
var ServerConfigValidImageTypes = []string{
	DefaultNodeImageType,
	"UBUNTU_CONTAINERD",
	"COS",
}

// ServerConfigChannels are the release channels ServerConfig.channels lists: the
// Discovery Cluster.Channel enum minus the deprecated UNSPECIFIED value.
var ServerConfigChannels = []string{"RAPID", "REGULAR", "STABLE", "EXTENDED"}

// GetServerConfig returns the Kubernetes Engine service configuration real GKE
// answers from projects.locations.getServerConfig (and the legacy
// projects.zones.getServerconfig).
//
// The emulator supports one Kubernetes version (DefaultMasterVersion) — the one
// its clusters and pools report — so it is both the default and the only valid
// master/node target. The channels name the same version: they are a documented
// shape clients (google_container_engine_versions) dereference, not a set of
// upgrade tracks the metadata-only mock can follow. Discovery's ServerConfig has
// no masterAuthorizedNetworksConfig member (that is a Cluster field, echoed on
// the cluster), so none is synthesized here.
func (s *Service) GetServerConfig(project, location string) (ServerConfig, error) {
	if project == "" || location == "" {
		return ServerConfig{}, invalidArgument("project and location are required")
	}
	return DefaultServerConfig(), nil
}

// DefaultServerConfig builds the service configuration the emulator reports. It
// is a function rather than a package var so the slices it returns are never
// shared with a response a transport renders.
func DefaultServerConfig() ServerConfig {
	cfg := ServerConfig{
		DefaultClusterVersion: DefaultMasterVersion,
		ValidMasterVersions:   []string{DefaultMasterVersion},
		ValidNodeVersions:     []string{DefaultMasterVersion},
		DefaultImageType:      DefaultNodeImageType,
		ValidImageTypes:       append([]string(nil), ServerConfigValidImageTypes...),
	}
	for _, ch := range ServerConfigChannels {
		cfg.Channels = append(cfg.Channels, ReleaseChannelConfig{
			Channel:        ch,
			DefaultVersion: DefaultMasterVersion,
			ValidVersions:  []string{DefaultMasterVersion},
		})
	}
	return cfg
}
