package container

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
