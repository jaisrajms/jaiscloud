package container

import (
	"strings"

	"jaiscloud/internal/gcp/resource"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// Values real GKE renders on every cluster read. They are GKE's documented
// defaults, not emulator inventions: a new Standard cluster is VPC-native and
// carries logging.googleapis.com/kubernetes + monitoring.googleapis.com/
// kubernetes, Shielded Nodes are enabled, the pod-per-node constraint is 110,
// and legacy ABAC is off (the message is still present in the response).
const (
	// DefaultMaxPodsPerNode is MaxPodsConstraint.maxPodsPerNode for a cluster
	// created without an explicit constraint.
	DefaultMaxPodsPerNode int64 = 110
	// LoggingServiceKubernetes is the cluster logging service GKE selects when
	// the create leaves loggingService empty.
	LoggingServiceKubernetes = "logging.googleapis.com/kubernetes"
	// MonitoringServiceKubernetes is the cluster monitoring service GKE selects
	// when the create leaves monitoringService empty.
	MonitoringServiceKubernetes = "monitoring.googleapis.com/kubernetes"
)

// NormalizeCluster fills the response messages real GKE always reports and a
// client may dereference unconditionally, so an omitted field never reaches the
// wire. It is the single owner of those literals: the create path persists its
// result (so a snapshot round-trip carries them) and WithReadDefaults applies it
// on read (so a record written before the fields were modelled — a snapshot or a
// --dsn import — answers exactly like a fresh one). Both callers use this
// function rather than restating the defaults.
func NormalizeCluster(c containerstore.Cluster) containerstore.Cluster {
	if c.ShieldedNodes == nil {
		c.ShieldedNodes = &containerstore.ShieldedNodes{Enabled: true}
	}
	if c.MaxPodsPerNode <= 0 {
		c.MaxPodsPerNode = DefaultMaxPodsPerNode
	}
	if c.LoggingService == "" {
		c.LoggingService = LoggingServiceKubernetes
	}
	if c.MonitoringService == "" {
		c.MonitoringService = MonitoringServiceKubernetes
	}
	if c.LegacyAbac == nil {
		c.LegacyAbac = &containerstore.LegacyAbac{}
	}
	return c
}

// WithReadDefaults renders a stored cluster the way a real GKE read answers:
// the NormalizeCluster messages plus the fields the API derives rather than
// stores. networkConfig is derived separately (NetworkConfigPaths), and
// masterAuth.clusterCaCertificate stays empty because the mock mints no CA.
//
// Applied on read only: it never changes what a caller persisted.
func WithReadDefaults(c containerstore.Cluster) containerstore.Cluster {
	c = NormalizeCluster(c)
	// Discovery on Cluster.nodeConfig: "For responses, this field will be
	// populated with the node configuration of the first node pool." The create
	// path applies a request nodeConfig to the generated default-pool, so the
	// echo is derived from the pool rather than stored twice (a --dsn import or
	// snapshot written before the field was modelled gets the same rendering).
	if len(c.NodePools) > 0 && c.NodePools[0].Config != nil {
		cfg := *c.NodePools[0].Config
		c.NodeConfig = &cfg
	}
	return c
}

// NetworkConfigPaths returns the NetworkConfig.network/subnetwork values real
// GKE reports: the relative resource names of the cluster's network and
// subnetwork (Discovery: "projects/my-project/global/networks/my-network" and
// "projects/my-project/regions/us-central1/subnetworks/my-subnet"). Cluster.
// network/subnetwork hold whatever the caller sent — a short name, a partial
// resource name or a Compute self-link (Terraform's google_container_cluster.network
// feeds google_compute_network.*.self_link) — so both are resolved to the
// relative name from the cluster record rather than stored twice.
func NetworkConfigPaths(c containerstore.Cluster) (network, subnetwork string) {
	return networkPath(c.ProjectID, c.Network), subnetworkPath(c.ProjectID, c.Location, c.Subnetwork)
}

func networkPath(project, network string) string {
	if network == "" {
		return resource.ComputePath(project, "compute-network", "default")
	}
	return resource.ComputePathFor(network, project, "compute-network", resource.LinkTail(network))
}

func subnetworkPath(project, location, subnetwork string) string {
	if subnetwork == "" {
		return resource.ComputePath(project, "compute-subnetwork", regionOf(location)+"/default")
	}
	// compute-subnetwork takes "region/name" and owns the project prefix; a
	// subnetwork link names its own region, and a short name uses the cluster's.
	region := resource.PathSegmentAfter(subnetwork, "regions")
	if region == "" {
		region = regionOf(location)
	}
	return resource.ComputePathFor(subnetwork, project, "compute-subnetwork", region+"/"+resource.LinkTail(subnetwork))
}

// regionOf maps a cluster location to its region: a location is either a region
// ("us-central1") or a zone ("us-central1-a"); a subnetwork path always names
// the region.
func regionOf(location string) string {
	if i := strings.LastIndex(location, "-"); i > 0 {
		if suffix := location[i+1:]; len(suffix) == 1 && suffix[0] >= 'a' && suffix[0] <= 'z' {
			return location[:i]
		}
	}
	return location
}
