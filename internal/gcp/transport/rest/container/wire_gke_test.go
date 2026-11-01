package container

import (
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestClusterAutopilotAndWorkloadIdentityRoundTrip verifies the two cluster
// settings Dataproc-on-GKE validates are decoded from the request and rendered
// back on read.
func TestClusterAutopilotAndWorkloadIdentityRoundTrip(t *testing.T) {
	body := map[string]any{
		"name":    "gke-1",
		"network": "default",
		"autopilot": map[string]any{
			"enabled": true,
		},
		"workloadIdentityConfig": map[string]any{
			"workloadPool": "proj.svc.id.goog",
		},
	}
	c, err := clusterFromBody(body)
	if err != nil {
		t.Fatalf("clusterFromBody: %v", err)
	}
	if c.Autopilot == nil || !c.Autopilot.Enabled {
		t.Fatalf("autopilot not parsed: %+v", c.Autopilot)
	}
	if c.WorkloadIdentityConfig == nil || c.WorkloadIdentityConfig.WorkloadPool != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not parsed: %+v", c.WorkloadIdentityConfig)
	}

	out := clusterToJSON(containerstore.Cluster{
		Name:                   "gke-1",
		Autopilot:              c.Autopilot,
		WorkloadIdentityConfig: c.WorkloadIdentityConfig,
	})
	ap, _ := out["autopilot"].(map[string]any)
	if ap["enabled"] != true {
		t.Fatalf("autopilot not rendered: %v", out)
	}
	wi, _ := out["workloadIdentityConfig"].(map[string]any)
	if wi["workloadPool"] != "proj.svc.id.goog" {
		t.Fatalf("workloadIdentityConfig not rendered: %v", out)
	}
}

// TestClusterAutopilotAndWorkloadIdentityAbsent verifies neither field is
// invented for a cluster that carries them not at all.
func TestClusterAutopilotAndWorkloadIdentityAbsent(t *testing.T) {
	c, err := clusterFromBody(map[string]any{"name": "plain"})
	if err != nil {
		t.Fatalf("clusterFromBody: %v", err)
	}
	if c.Autopilot != nil || c.WorkloadIdentityConfig != nil {
		t.Fatalf("unexpected settings parsed: %+v %+v", c.Autopilot, c.WorkloadIdentityConfig)
	}
	out := clusterToJSON(c)
	if _, ok := out["autopilot"]; ok {
		t.Fatalf("autopilot must be absent: %v", out)
	}
	if _, ok := out["workloadIdentityConfig"]; ok {
		t.Fatalf("workloadIdentityConfig must be absent: %v", out)
	}
}

// TestClusterNestedReadMessages pins the nested response messages real GKE
// always renders — legacyAbac, networkConfig, shieldedNodes and
// defaultMaxPodsConstraint — and the create-time decode of the two a caller may
// set (the hashicorp/google provider dereferences all four unconditionally on
// its google_container_cluster read and aborts when one is absent: DPG37).
func TestClusterNestedReadMessages(t *testing.T) {
	body := map[string]any{
		"name":                     "gke-nested",
		"network":                  "vpc-1",
		"subnetwork":               "sn-1",
		"shieldedNodes":            map[string]any{"enabled": false},
		"defaultMaxPodsConstraint": map[string]any{"maxPodsPerNode": "64"},
	}
	c, err := clusterFromBody(body)
	if err != nil {
		t.Fatalf("clusterFromBody: %v", err)
	}
	if c.ShieldedNodes == nil || c.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes not decoded: %+v", c.ShieldedNodes)
	}
	if c.MaxPodsPerNode != 64 {
		t.Fatalf("defaultMaxPodsConstraint not decoded (int64-as-string): %d", c.MaxPodsPerNode)
	}

	out := clusterToJSON(containerstore.Cluster{
		ProjectID:      "proj",
		Location:       "us-central1",
		Name:           "gke-nested",
		Network:        c.Network,
		Subnetwork:     c.Subnetwork,
		ShieldedNodes:  c.ShieldedNodes,
		MaxPodsPerNode: c.MaxPodsPerNode,
	})
	nc, _ := out["networkConfig"].(map[string]any)
	if nc["network"] != "projects/proj/global/networks/vpc-1" ||
		nc["subnetwork"] != "projects/proj/regions/us-central1/subnetworks/sn-1" {
		t.Fatalf("networkConfig = %v", nc)
	}
	sn, _ := out["shieldedNodes"].(map[string]any)
	if sn["enabled"] != false {
		t.Fatalf("shieldedNodes = %v", sn)
	}
	dpc, _ := out["defaultMaxPodsConstraint"].(map[string]any)
	if dpc["maxPodsPerNode"] != "64" {
		t.Fatalf("defaultMaxPodsConstraint = %v (int64 must render as a JSON string)", dpc)
	}
	abac, ok := out["legacyAbac"].(map[string]any)
	if !ok || abac["enabled"] != false {
		t.Fatalf("legacyAbac must always render {enabled:...}: %v", out["legacyAbac"])
	}
	if out["loggingService"] != "logging.googleapis.com/kubernetes" {
		t.Fatalf("loggingService default = %v", out["loggingService"])
	}
	if out["monitoringService"] != "monitoring.googleapis.com/kubernetes" {
		t.Fatalf("monitoringService default = %v", out["monitoringService"])
	}
}

// TestClusterNestedReadMessagesDefault verifies a cluster that sets none of them
// still renders GKE's defaults (enabled shielded nodes, 110 pods, ABAC off),
// which is also what a record stored before DPG37 renders.
func TestClusterNestedReadMessagesDefault(t *testing.T) {
	out := clusterToJSON(containerstore.Cluster{ProjectID: "proj", Location: "us-central1-a", Name: "plain"})
	sn, _ := out["shieldedNodes"].(map[string]any)
	if sn["enabled"] != true {
		t.Fatalf("shieldedNodes default = %v", sn)
	}
	dpc, _ := out["defaultMaxPodsConstraint"].(map[string]any)
	if dpc["maxPodsPerNode"] != "110" {
		t.Fatalf("maxPodsPerNode default = %v", dpc)
	}
	nc, _ := out["networkConfig"].(map[string]any)
	if nc["subnetwork"] != "projects/proj/regions/us-central1/subnetworks/default" {
		t.Fatalf("zonal subnetwork path = %v", nc["subnetwork"])
	}
}

// TestClusterUpdateDecodesShieldedNodes verifies ClusterUpdate.desiredShieldedNodes
// reaches the core, so an update cannot report DONE while leaving the flag the
// read renders unchanged (DPG37).
func TestClusterUpdateDecodesShieldedNodes(t *testing.T) {
	u := clusterUpdateFromMap(map[string]any{
		"desiredShieldedNodes": map[string]any{"enabled": false},
	})
	if u.DesiredShieldedNodes == nil || u.DesiredShieldedNodes.Enabled {
		t.Fatalf("desiredShieldedNodes = %+v, want disabled", u.DesiredShieldedNodes)
	}
}
