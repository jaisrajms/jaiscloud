package container

import (
	"context"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestWithReadDefaultsFillsResponseMessages pins the four nested messages real
// GKE always renders and clients dereference unconditionally (the
// hashicorp/google provider's google_container_cluster read aborts on any of
// them being absent), for a record stored before they were modelled.
func TestWithReadDefaultsFillsResponseMessages(t *testing.T) {
	c := WithReadDefaults(containerstore.Cluster{ProjectID: "p", Location: "us-central1", Name: "c1"})
	if c.ShieldedNodes == nil || !c.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes = %+v, want enabled", c.ShieldedNodes)
	}
	if c.MaxPodsPerNode != DefaultMaxPodsPerNode {
		t.Fatalf("maxPodsPerNode = %d, want %d", c.MaxPodsPerNode, DefaultMaxPodsPerNode)
	}
	if c.LegacyAbac == nil || c.LegacyAbac.Enabled {
		t.Fatalf("legacyAbac = %+v, want present and disabled", c.LegacyAbac)
	}
	if c.LoggingService != LoggingServiceKubernetes {
		t.Fatalf("loggingService = %q", c.LoggingService)
	}
	if c.MonitoringService != MonitoringServiceKubernetes {
		t.Fatalf("monitoringService = %q", c.MonitoringService)
	}
}

// TestWithReadDefaultsKeepsCallerValues verifies the defaults never overwrite
// what a caller (or a setter) recorded.
func TestWithReadDefaultsKeepsCallerValues(t *testing.T) {
	c := WithReadDefaults(containerstore.Cluster{
		Name:              "c1",
		ShieldedNodes:     &containerstore.ShieldedNodes{Enabled: false},
		MaxPodsPerNode:    64,
		LegacyAbac:        &containerstore.LegacyAbac{Enabled: true},
		LoggingService:    "none",
		MonitoringService: "none",
	})
	if c.ShieldedNodes.Enabled {
		t.Fatalf("shieldedNodes rewritten: %+v", c.ShieldedNodes)
	}
	if c.MaxPodsPerNode != 64 {
		t.Fatalf("maxPodsPerNode rewritten: %d", c.MaxPodsPerNode)
	}
	if !c.LegacyAbac.Enabled {
		t.Fatalf("legacyAbac rewritten: %+v", c.LegacyAbac)
	}
	if c.LoggingService != "none" || c.MonitoringService != "none" {
		t.Fatalf("services rewritten: %q %q", c.LoggingService, c.MonitoringService)
	}
}

// TestNetworkConfigPathsDerivesResourceNames pins NetworkConfig.network and
// .subnetwork as the relative resource names Discovery documents, for every
// shape a caller can supply: a short name, a project-relative partial resource
// name, a full Compute path and a full Compute self-link (Terraform's
// google_container_cluster.network feeds google_compute_network.*.self_link).
// A link that names another project — a Shared-VPC host project — keeps that
// project instead of being rewritten into the cluster's own.
func TestNetworkConfigPathsDerivesResourceNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		location string
		network  string
		subnet   string
		wantNet  string
		wantSub  string
	}{
		{
			name:     "bare names take the cluster project and region",
			location: "us-central1", network: "default", subnet: "default",
			wantNet: "projects/p/global/networks/default",
			wantSub: "projects/p/regions/us-central1/subnetworks/default",
		},
		{
			name:     "zonal cluster reduces to its region",
			location: "us-central1-a", network: "vpc-1", subnet: "sn-1",
			wantNet: "projects/p/global/networks/vpc-1",
			wantSub: "projects/p/regions/us-central1/subnetworks/sn-1",
		},
		{
			name:     "project-relative partial paths pass through",
			location: "us-central1", network: "projects/p/global/networks/vpc-2", subnet: "projects/p/regions/us-east1/subnetworks/sn-2",
			wantNet: "projects/p/global/networks/vpc-2",
			wantSub: "projects/p/regions/us-east1/subnetworks/sn-2",
		},
		{
			name:     "compute self-links are not prefixed twice",
			location: "us-central1",
			network:  "https://www.googleapis.com/compute/v1/projects/p/global/networks/vpc-3",
			subnet:   "https://www.googleapis.com/compute/v1/projects/p/regions/us-east1/subnetworks/sn-3",
			wantNet:  "projects/p/global/networks/vpc-3",
			wantSub:  "projects/p/regions/us-east1/subnetworks/sn-3",
		},
		{
			name:     "shared-vpc host project is preserved",
			location: "us-central1",
			network:  "https://www.googleapis.com/compute/v1/projects/host-project/global/networks/shared-vpc",
			subnet:   "https://www.googleapis.com/compute/v1/projects/host-project/regions/europe-west4/subnetworks/shared-sn",
			wantNet:  "projects/host-project/global/networks/shared-vpc",
			wantSub:  "projects/host-project/regions/europe-west4/subnetworks/shared-sn",
		},
	} {
		c := containerstore.Cluster{ProjectID: "p", Location: tc.location, Network: tc.network, Subnetwork: tc.subnet}
		net, sub := NetworkConfigPaths(c)
		if net != tc.wantNet || sub != tc.wantSub {
			t.Fatalf("%s: got %q / %q, want %q / %q", tc.name, net, sub, tc.wantNet, tc.wantSub)
		}
	}
}

// TestNodePoolWithReadDefaults pins the node-config defaults real GKE renders for
// a pool the caller did not shape, captured from a real create without nodeConfig
// (docs/gcp/gke-node-pool-config-defaults.md, DPG46), and verifies a read never
// mutates the stored record.
func TestNodePoolWithReadDefaults(t *testing.T) {
	stored := &containerstore.NodeConfig{
		MachineType: "n1-standard-4",
		OauthScopes: []string{"scope-a"},
	}
	p := NodePoolWithReadDefaults(containerstore.NodePool{Name: "default-pool", Config: stored})
	if p.Config == nil {
		t.Fatal("defaulted pool has no config")
	}
	if p.Config.MachineType != "n1-standard-4" {
		t.Fatalf("caller machineType overwritten: %q", p.Config.MachineType)
	}
	if got := p.Config.OauthScopes; len(got) != 1 || got[0] != "scope-a" {
		t.Fatalf("caller oauthScopes overwritten: %v", got)
	}
	if p.Config.ImageType != DefaultNodeImageType || p.Config.DiskType != DefaultNodeDiskType {
		t.Fatalf("image/disk defaults = %q/%q", p.Config.ImageType, p.Config.DiskType)
	}
	if p.Config.DiskSizeGb != DefaultNodeDiskSizeGb || p.Config.ServiceAccount != DefaultNodeServiceAccount {
		t.Fatalf("disk size/service account defaults = %d/%q", p.Config.DiskSizeGb, p.Config.ServiceAccount)
	}
	// The stored config is untouched (the read copies before filling).
	if stored.ImageType != "" || stored.DiskSizeGb != 0 || len(stored.OauthScopes) != 1 {
		t.Fatalf("read mutated the stored config: %+v", stored)
	}

	// A pool with no config at all gets every default, including the six scopes.
	empty := NodePoolWithReadDefaults(containerstore.NodePool{Name: "pool-a"})
	if empty.Config == nil || len(empty.Config.OauthScopes) != len(DefaultNodeOauthScopes) {
		t.Fatalf("empty pool config = %+v", empty.Config)
	}
	if empty.Config.MachineType != DefaultNodeMachineType || empty.Config.DiskSizeGb != DefaultNodeDiskSizeGb {
		t.Fatalf("empty pool defaults = %+v", empty.Config)
	}
}

// TestNodePoolReadsCarryDefaultedConfig verifies both node-pool read paths (get
// and list) render the defaulted config, not just the cluster read (DPG46).
func TestNodePoolReadsCarryDefaultedConfig(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	got, err := s.GetNodePool(ctx, "p", "us-central1", "c1", DefaultNodePoolName)
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if got.Config == nil || got.Config.MachineType != DefaultNodeMachineType || got.Config.ImageType != DefaultNodeImageType {
		t.Fatalf("pool read config = %+v", got.Config)
	}
	list, err := s.ListNodePools(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("ListNodePools: %v", err)
	}
	if len(list) != 1 || list[0].Config == nil || list[0].Config.DiskType != DefaultNodeDiskType {
		t.Fatalf("pool list config = %+v", list)
	}
}

// TestWithReadDefaultsLegacyRecordMatchesFresh pins the single normaliser: a
// record written before the response messages were modelled (a snapshot or a
// --dsn import answers with the zero values) renders exactly like a fresh
// create, which persists NormalizeCluster's result.
func TestWithReadDefaultsLegacyRecordMatchesFresh(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "fresh"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	fresh, err := s.GetCluster(ctx, "p", "us-central1", "fresh")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	legacy := WithReadDefaults(containerstore.Cluster{ProjectID: "p", Location: "us-central1", Name: "fresh"})
	if fresh.ShieldedNodes == nil || legacy.ShieldedNodes == nil || *fresh.ShieldedNodes != *legacy.ShieldedNodes {
		t.Fatalf("shieldedNodes differ: fresh %+v, legacy %+v", fresh.ShieldedNodes, legacy.ShieldedNodes)
	}
	if fresh.MaxPodsPerNode != legacy.MaxPodsPerNode {
		t.Fatalf("maxPodsPerNode differ: fresh %d, legacy %d", fresh.MaxPodsPerNode, legacy.MaxPodsPerNode)
	}
	if fresh.LoggingService != legacy.LoggingService || fresh.MonitoringService != legacy.MonitoringService {
		t.Fatalf("services differ: fresh %q/%q, legacy %q/%q",
			fresh.LoggingService, fresh.MonitoringService, legacy.LoggingService, legacy.MonitoringService)
	}
	if fresh.LegacyAbac == nil || legacy.LegacyAbac == nil || *fresh.LegacyAbac != *legacy.LegacyAbac {
		t.Fatalf("legacyAbac differ: fresh %+v, legacy %+v", fresh.LegacyAbac, legacy.LegacyAbac)
	}
	// NormalizeCluster is idempotent, so applying it twice (create + read) cannot
	// drift from applying it once.
	twice := NormalizeCluster(NormalizeCluster(containerstore.Cluster{}))
	once := NormalizeCluster(containerstore.Cluster{})
	if twice.MaxPodsPerNode != once.MaxPodsPerNode || twice.LoggingService != once.LoggingService {
		t.Fatalf("normaliser is not idempotent: once %+v, twice %+v", once, twice)
	}
}

// TestCreateClusterStoresAndRendersReadDefaults verifies a fresh cluster
// persists the GKE defaults (so a snapshot carries them) and renders them on
// both the single read and the list.
func TestCreateClusterStoresAndRendersReadDefaults(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.ShieldedNodes == nil || !c.ShieldedNodes.Enabled || c.MaxPodsPerNode != 110 {
		t.Fatalf("stored defaults = %+v / %d", c.ShieldedNodes, c.MaxPodsPerNode)
	}
	if c.LoggingService != LoggingServiceKubernetes || c.MonitoringService != MonitoringServiceKubernetes {
		t.Fatalf("stored services = %q %q", c.LoggingService, c.MonitoringService)
	}
	list, err := s.ListClusters(ctx, "p", "us-central1")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 1 || list[0].ShieldedNodes == nil {
		t.Fatalf("list = %+v", list)
	}
}
