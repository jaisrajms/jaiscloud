package container

import (
	"context"
	"testing"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/container"
)

// TestGetServerConfigProtoShape pins the generated client's view of
// GetServerConfig: the canonical name form and the deprecated project_id/zone
// pair both resolve, and the response carries the documented members.
func TestGetServerConfigProtoShape(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)

	byName, err := svc.GetServerConfig(ctx, &containerpb.GetServerConfigRequest{
		Name: "projects/p/locations/us-central1",
	})
	if err != nil {
		t.Fatalf("GetServerConfig(name): %v", err)
	}
	if byName.GetDefaultClusterVersion() != core.DefaultMasterVersion {
		t.Fatalf("defaultClusterVersion = %q, want %q", byName.GetDefaultClusterVersion(), core.DefaultMasterVersion)
	}
	if got := byName.GetValidMasterVersions(); len(got) != 1 || got[0] != core.DefaultMasterVersion {
		t.Fatalf("validMasterVersions = %v", got)
	}
	if got := byName.GetValidNodeVersions(); len(got) != 1 || got[0] != core.DefaultMasterVersion {
		t.Fatalf("validNodeVersions = %v", got)
	}
	if byName.GetDefaultImageType() != core.DefaultNodeImageType {
		t.Fatalf("defaultImageType = %q", byName.GetDefaultImageType())
	}
	if got := byName.GetValidImageTypes(); len(got) == 0 || got[0] != byName.GetDefaultImageType() {
		t.Fatalf("validImageTypes = %v", got)
	}
	channels := byName.GetChannels()
	if len(channels) != 4 {
		t.Fatalf("channels = %v", channels)
	}
	want := []containerpb.ReleaseChannel_Channel{
		containerpb.ReleaseChannel_RAPID,
		containerpb.ReleaseChannel_REGULAR,
		containerpb.ReleaseChannel_STABLE,
		containerpb.ReleaseChannel_EXTENDED,
	}
	for i, ch := range channels {
		if ch.GetChannel() != want[i] {
			t.Fatalf("channels[%d].channel = %v, want %v", i, ch.GetChannel(), want[i])
		}
		if ch.GetDefaultVersion() != core.DefaultMasterVersion {
			t.Fatalf("channels[%d].defaultVersion = %q", i, ch.GetDefaultVersion())
		}
		if got := ch.GetValidVersions(); len(got) != 1 || got[0] != core.DefaultMasterVersion {
			t.Fatalf("channels[%d].validVersions = %v", i, got)
		}
	}

	// The deprecated legacy addressing resolves to the same catalog.
	byLegacy, err := svc.GetServerConfig(ctx, &containerpb.GetServerConfigRequest{
		ProjectId: "p", Zone: "us-central1-a",
	})
	if err != nil {
		t.Fatalf("GetServerConfig(legacy): %v", err)
	}
	if byLegacy.GetDefaultClusterVersion() != byName.GetDefaultClusterVersion() {
		t.Fatalf("legacy defaultClusterVersion = %q", byLegacy.GetDefaultClusterVersion())
	}
}

func TestGetServerConfigRejectsMalformedName(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	_, err := svc.GetServerConfig(ctx, &containerpb.GetServerConfigRequest{Name: "projects/p/zones/us-central1-a"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
	// Neither a name nor the legacy pair: InvalidArgument, not a panic.
	_, err = svc.GetServerConfig(ctx, &containerpb.GetServerConfigRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request code = %v, want InvalidArgument", status.Code(err))
	}
}

// TestNodeConfigAcceleratorsProtoRoundTrip is the gRPC half of the DPG25
// round-trip: accelerators and bootDiskKmsKey survive create -> get on the
// generated client's messages.
func TestNodeConfigAcceleratorsProtoRoundTrip(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	const kmsKey = "projects/p/locations/global/keyRings/ring/cryptoKeys/boot"
	parent := "projects/p/locations/us-central1"

	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent: parent,
		Cluster: &containerpb.Cluster{
			Name:             "gpu",
			InitialNodeCount: 1,
			NodeConfig: &containerpb.NodeConfig{
				MachineType: "n1-standard-1",
				Accelerators: []*containerpb.AcceleratorConfig{{
					AcceleratorCount: 1,
					AcceleratorType:  "nvidia-tesla-t4",
					GpuPartitionSize: "1g.5gb",
				}},
				BootDiskKmsKey: kmsKey,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	got, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/gpu"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	cfg := got.GetNodeConfig()
	if cfg.GetBootDiskKmsKey() != kmsKey {
		t.Fatalf("bootDiskKmsKey = %q", cfg.GetBootDiskKmsKey())
	}
	if len(cfg.GetAccelerators()) != 1 {
		t.Fatalf("accelerators = %v", cfg.GetAccelerators())
	}
	a := cfg.GetAccelerators()[0]
	if a.GetAcceleratorCount() != 1 || a.GetAcceleratorType() != "nvidia-tesla-t4" || a.GetGpuPartitionSize() != "1g.5gb" {
		t.Fatalf("accelerator = %+v", a)
	}
}

func TestCreateNodePoolRejectsMalformedKmsKey(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"
	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("c1")}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	_, err := svc.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
		Parent: parent + "/clusters/c1",
		NodePool: &containerpb.NodePool{
			Name:             "bad",
			InitialNodeCount: 1,
			Config:           &containerpb.NodeConfig{BootDiskKmsKey: "short-key"},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}
