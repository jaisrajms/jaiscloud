//go:build gcp_persistence

package container

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresMutateClusterAndNodePools verifies the Postgres backend's atomic
// MutateCluster (SELECT … FOR UPDATE) and node-pool/cluster-record persistence
// match the memory store, and that they survive a snapshot/restore round-trip.
// It runs under -tags gcp_persistence with JAISCLOUD_DSN set (make postgres-up).
func TestPostgresMutateClusterAndNodePools(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	if err := s.CreateCluster(ctx, "p", "l", Cluster{Name: "c1", Status: StatusRunning, NodePools: []NodePool{{Name: "default-pool"}}}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	err = s.MutateCluster(ctx, "p", "l", "c1", func(c *Cluster) error {
		c.AddonsConfig = &AddonsConfig{HttpLoadBalancing: &AddonConfig{Disabled: true}}
		c.NodePools = append(c.NodePools, NodePool{Name: "np1", Status: StatusRunning, Autoscaling: &NodePoolAutoscaling{Enabled: true, MaxNodeCount: 4}})
		return nil
	})
	if err != nil {
		t.Fatalf("MutateCluster: %v", err)
	}
	got, err := s.GetCluster(ctx, "p", "l", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(got.NodePools) != 2 || got.NodePools[1].Name != "np1" || got.NodePools[1].Autoscaling == nil || got.NodePools[1].Autoscaling.MaxNodeCount != 4 {
		t.Fatalf("node pools = %+v", got.NodePools)
	}
	if got.AddonsConfig == nil || got.AddonsConfig.HttpLoadBalancing == nil || !got.AddonsConfig.HttpLoadBalancing.Disabled {
		t.Fatalf("addonsConfig = %+v", got.AddonsConfig)
	}
	if err := s.MutateCluster(ctx, "p", "l", "missing", func(*Cluster) error { return nil }); !errors.Is(err, ErrNoSuchCluster) {
		t.Fatalf("MutateCluster missing err = %v, want ErrNoSuchCluster", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, err = s.GetCluster(ctx, "p", "l", "c1")
	if err != nil {
		t.Fatalf("GetCluster after restore: %v", err)
	}
	if len(got.NodePools) != 2 || got.NodePools[1].Autoscaling.MaxNodeCount != 4 {
		t.Fatalf("restored node pools = %+v", got.NodePools)
	}
}
