package container

import (
	"testing"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"

	core "jaiscloud/internal/gcp/service/container"
)

// TestClusterAutopilotAndWorkloadIdentityTranscode verifies the gRPC transport
// carries the same two cluster settings the REST transport does, so a
// Dataproc-on-GKE create sees one emulated GKE cluster regardless of the client.
func TestClusterAutopilotAndWorkloadIdentityTranscode(t *testing.T) {
	pb := &containerpb.Cluster{
		Name:                   "gke-1",
		Autopilot:              &containerpb.Autopilot{Enabled: true},
		WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{WorkloadPool: "proj.svc.id.goog"},
	}
	stored := clusterFromProto(pb)
	if stored.Autopilot == nil || !stored.Autopilot.Enabled {
		t.Fatalf("autopilot not decoded: %+v", stored.Autopilot)
	}
	if stored.WorkloadIdentityConfig == nil || stored.WorkloadIdentityConfig.WorkloadPool != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not decoded: %+v", stored.WorkloadIdentityConfig)
	}

	back := clusterToProto(stored)
	if !back.GetAutopilot().GetEnabled() {
		t.Fatalf("autopilot not rendered: %v", back)
	}
	if back.GetWorkloadIdentityConfig().GetWorkloadPool() != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not rendered: %v", back)
	}
}

// TestClusterNestedReadMessagesTranscode verifies the gRPC transport renders the
// same nested messages real GKE always returns (legacyAbac, networkConfig,
// shieldedNodes, defaultMaxPodsConstraint) and decodes the two a create may set,
// so REST and gRPC cannot drift on the shape the hashicorp/google provider
// dereferences (DPG37).
func TestClusterNestedReadMessagesTranscode(t *testing.T) {
	stored := clusterFromProto(&containerpb.Cluster{
		Name:                     "gke-nested",
		Network:                  "vpc-1",
		Subnetwork:               "sn-1",
		ShieldedNodes:            &containerpb.ShieldedNodes{Enabled: false},
		DefaultMaxPodsConstraint: &containerpb.MaxPodsConstraint{MaxPodsPerNode: 64},
	})
	if stored.ShieldedNodes == nil || stored.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes not decoded: %+v", stored.ShieldedNodes)
	}
	if stored.MaxPodsPerNode != 64 {
		t.Fatalf("maxPodsPerNode not decoded: %d", stored.MaxPodsPerNode)
	}

	stored.ProjectID = "proj"
	stored.Location = "us-central1"
	back := clusterToProto(stored)
	if got := back.GetNetworkConfig().GetNetwork(); got != "projects/proj/global/networks/vpc-1" {
		t.Fatalf("networkConfig.network = %q", got)
	}
	if got := back.GetNetworkConfig().GetSubnetwork(); got != "projects/proj/regions/us-central1/subnetworks/sn-1" {
		t.Fatalf("networkConfig.subnetwork = %q", got)
	}
	if back.GetShieldedNodes().GetEnabled() {
		t.Fatalf("shieldedNodes.enabled = true, want false")
	}
	if got := back.GetDefaultMaxPodsConstraint().GetMaxPodsPerNode(); got != 64 {
		t.Fatalf("defaultMaxPodsConstraint.maxPodsPerNode = %d", got)
	}
	if back.GetLegacyAbac() == nil || back.GetLegacyAbac().GetEnabled() {
		t.Fatalf("legacyAbac = %v, want present and disabled", back.GetLegacyAbac())
	}
	if back.GetLoggingService() != core.LoggingServiceKubernetes || back.GetMonitoringService() != core.MonitoringServiceKubernetes {
		t.Fatalf("services = %q %q", back.GetLoggingService(), back.GetMonitoringService())
	}
}

// TestClusterUpdateDecodesShieldedNodesTranscode verifies the gRPC transport
// carries ClusterUpdate.desired_shielded_nodes like the REST transport does.
func TestClusterUpdateDecodesShieldedNodesTranscode(t *testing.T) {
	u := clusterUpdateFromProto(&containerpb.ClusterUpdate{
		DesiredShieldedNodes: &containerpb.ShieldedNodes{Enabled: false},
	})
	if u.DesiredShieldedNodes == nil || u.DesiredShieldedNodes.Enabled {
		t.Fatalf("desiredShieldedNodes = %+v, want disabled", u.DesiredShieldedNodes)
	}
}
