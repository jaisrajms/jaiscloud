package container

import (
	"context"
	"errors"
	"strings"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

func newTestService() *Service {
	return NewService(containerstore.NewMemoryStore())
}

func providerErr(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s error, got nil", wantCode)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != wantCode {
		t.Fatalf("error code = %q, want %q (%v)", pe.Code, wantCode, err)
	}
}

func TestCreateClusterReturnsOperationAndStoresCluster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	op, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", InitialNodeCount: 3})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if op.OperationType != containerstore.OperationCreateCluster || op.Status != containerstore.OperationStatusDone {
		t.Fatalf("operation = %+v", op)
	}
	if !strings.HasPrefix(op.Name, "operation-") {
		t.Fatalf("operation name = %q, want operation- prefix", op.Name)
	}
	if op.SelfLink != "projects/p/locations/us-central1/operations/"+op.Name {
		t.Fatalf("operation selfLink = %q", op.SelfLink)
	}

	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.Status != containerstore.StatusRunning {
		t.Fatalf("status = %q, want RUNNING", c.Status)
	}
	if c.CurrentMasterVersion != DefaultMasterVersion {
		t.Fatalf("master version = %q, want %q", c.CurrentMasterVersion, DefaultMasterVersion)
	}
	if c.Network != "default" || c.Subnetwork != "default" {
		t.Fatalf("network/subnetwork = %q/%q", c.Network, c.Subnetwork)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Name != "default-pool" || c.NodePools[0].InitialNodeCount != 3 {
		t.Fatalf("nodePools = %+v", c.NodePools)
	}
	if c.SelfLink != "projects/p/locations/us-central1/clusters/c1" {
		t.Fatalf("cluster selfLink = %q", c.SelfLink)
	}
}

func TestCreateClusterErrors(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	if _, err := s.CreateCluster(ctx, "", "l", containerstore.Cluster{Name: "c1"}); err == nil {
		t.Fatal("want InvalidArgument for empty project")
	}
	_, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{})
	providerErr(t, err, "InvalidArgument")

	_, err = s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "Bad_Name"})
	providerErr(t, err, "InvalidArgument")

	if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"})
	providerErr(t, err, "AlreadyExists")
}

func TestGetListDeleteCluster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.GetCluster(ctx, "p", "l", "missing")
	providerErr(t, err, "NotFound")

	for _, name := range []string{"b", "a", "c"} {
		if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: name}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	list, err := s.ListClusters(ctx, "p", "l")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 3 || list[0].Name != "a" || list[1].Name != "b" || list[2].Name != "c" {
		t.Fatalf("list order = %+v", list)
	}

	op, err := s.DeleteCluster(ctx, "p", "l", "a")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if op.OperationType != containerstore.OperationDeleteCluster || op.Status != containerstore.OperationStatusDone {
		t.Fatalf("delete operation = %+v", op)
	}
	if _, err := s.GetCluster(ctx, "p", "l", "a"); err == nil {
		t.Fatal("cluster still present after delete")
	}
	_, err = s.DeleteCluster(ctx, "p", "l", "a")
	providerErr(t, err, "NotFound")
}

func TestOperations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	if _, err := s.GetOperation(ctx, "p", "l", "missing"); err == nil {
		t.Fatal("want NotFound for missing operation")
	}
	createOp, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.DeleteCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	got, err := s.GetOperation(ctx, "p", "l", createOp.Name)
	if err != nil || got.OperationType != containerstore.OperationCreateCluster {
		t.Fatalf("GetOperation = %+v, %v", got, err)
	}
	ops, err := s.ListOperations(ctx, "p", "l")
	if err != nil || len(ops) != 2 {
		t.Fatalf("ListOperations = %d, %v", len(ops), err)
	}
}

func TestResetClearsState(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	s.Reset(ctx)
	if list, err := s.ListClusters(ctx, "p", "l"); err != nil || len(list) != 0 {
		t.Fatalf("ListClusters after reset = %d, %v", len(list), err)
	}
	if ops, err := s.ListOperations(ctx, "p", "l"); err != nil || len(ops) != 0 {
		t.Fatalf("ListOperations after reset = %d, %v", len(ops), err)
	}
}
