package container

import (
	"context"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

func TestClusterSettersRecordFields(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	op, err := s.SetAddonsConfig(ctx, "p", "us-central1", "c1", containerstore.AddonsConfig{
		HttpLoadBalancing: &containerstore.AddonConfig{Disabled: true},
	})
	if err != nil || op.OperationType != containerstore.OperationUpdateCluster {
		t.Fatalf("SetAddonsConfig = %+v, %v", op, err)
	}
	if _, err := s.SetLoggingService(ctx, "p", "us-central1", "c1", "logging.googleapis.com/kubernetes"); err != nil {
		t.Fatalf("SetLoggingService: %v", err)
	}
	if _, err := s.SetMonitoringService(ctx, "p", "us-central1", "c1", "monitoring.googleapis.com/kubernetes"); err != nil {
		t.Fatalf("SetMonitoringService: %v", err)
	}
	if _, err := s.SetLabels(ctx, "p", "us-central1", "c1", map[string]string{"team": "platform"}); err != nil {
		t.Fatalf("SetLabels: %v", err)
	}
	if _, err := s.SetLegacyAbac(ctx, "p", "us-central1", "c1", true); err != nil {
		t.Fatalf("SetLegacyAbac: %v", err)
	}
	if _, err := s.SetLocations(ctx, "p", "us-central1", "c1", []string{"us-central1-a"}); err != nil {
		t.Fatalf("SetLocations: %v", err)
	}
	if _, err := s.SetNetworkPolicy(ctx, "p", "us-central1", "c1", containerstore.NetworkPolicy{Enabled: true, Provider: "CALICO"}); err != nil {
		t.Fatalf("SetNetworkPolicy: %v", err)
	}
	if _, err := s.SetMaintenancePolicy(ctx, "p", "us-central1", "c1", containerstore.MaintenancePolicy{
		Window: &containerstore.MaintenanceWindow{DailyMaintenanceWindow: &containerstore.DailyMaintenanceWindow{StartTime: "03:00"}},
	}); err != nil {
		t.Fatalf("SetMaintenancePolicy: %v", err)
	}
	if _, err := s.SetMasterAuth(ctx, "p", "us-central1", "c1", "admin"); err != nil {
		t.Fatalf("SetMasterAuth: %v", err)
	}

	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.AddonsConfig == nil || c.AddonsConfig.HttpLoadBalancing == nil || !c.AddonsConfig.HttpLoadBalancing.Disabled {
		t.Fatalf("addonsConfig = %+v", c.AddonsConfig)
	}
	if c.LoggingService != "logging.googleapis.com/kubernetes" || c.MonitoringService != "monitoring.googleapis.com/kubernetes" {
		t.Fatalf("logging/monitoring = %q/%q", c.LoggingService, c.MonitoringService)
	}
	if c.ResourceLabels["team"] != "platform" {
		t.Fatalf("labels = %v", c.ResourceLabels)
	}
	if c.LegacyAbac == nil || !c.LegacyAbac.Enabled {
		t.Fatalf("legacyAbac = %+v", c.LegacyAbac)
	}
	if len(c.Locations) != 1 || c.Locations[0] != "us-central1-a" {
		t.Fatalf("locations = %v", c.Locations)
	}
	if c.NetworkPolicy == nil || !c.NetworkPolicy.Enabled || c.NetworkPolicy.Provider != "CALICO" {
		t.Fatalf("networkPolicy = %+v", c.NetworkPolicy)
	}
	if c.MaintenancePolicy == nil || c.MaintenancePolicy.Window == nil || c.MaintenancePolicy.Window.DailyMaintenanceWindow == nil {
		t.Fatalf("maintenancePolicy = %+v", c.MaintenancePolicy)
	}
	if c.AdminUsername != "admin" {
		t.Fatalf("adminUsername = %q", c.AdminUsername)
	}
}

func TestUpdateClusterAndMaster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := s.UpdateCluster(ctx, "p", "us-central1", "c1", ClusterUpdate{DesiredMonitoringService: "monitoring.googleapis.com/kubernetes"})
	if err != nil || op.OperationType != containerstore.OperationUpdateCluster {
		t.Fatalf("UpdateCluster = %+v, %v", op, err)
	}
	op, err = s.UpdateMaster(ctx, "p", "us-central1", "c1", "1.31.0-gke.100")
	if err != nil || op.OperationType != containerstore.OperationUpgradeMaster {
		t.Fatalf("UpdateMaster = %+v, %v", op, err)
	}
	c, _ := s.GetCluster(ctx, "p", "us-central1", "c1")
	if c.CurrentMasterVersion != "1.31.0-gke.100" || c.MonitoringService != "monitoring.googleapis.com/kubernetes" {
		t.Fatalf("cluster = %+v", c)
	}
}

func TestIPRotationAndCancel(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.StartIPRotation(ctx, "p", "us-central1", "c1"); err != nil {
		t.Fatalf("StartIPRotation: %v", err)
	}
	c, _ := s.GetCluster(ctx, "p", "us-central1", "c1")
	if !c.IPRotationEnabled {
		t.Fatal("ipRotation should be enabled after StartIPRotation")
	}
	if _, err := s.CompleteIPRotation(ctx, "p", "us-central1", "c1"); err != nil {
		t.Fatalf("CompleteIPRotation: %v", err)
	}
	c, _ = s.GetCluster(ctx, "p", "us-central1", "c1")
	if c.IPRotationEnabled {
		t.Fatal("ipRotation should be disabled after CompleteIPRotation")
	}

	op, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c2"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if err := s.CancelOperation(ctx, "p", "us-central1", op.Name); err != nil {
		t.Fatalf("CancelOperation: %v", err)
	}
	providerErr(t, s.CancelOperation(ctx, "p", "us-central1", "missing"), "NotFound")
}

func TestSettersOnMissingCluster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	providerErr(t, mustClusterOp(s.SetLoggingService(ctx, "p", "l", "missing", "x")), "NotFound")
	providerErr(t, mustClusterOp(s.UpdateMaster(ctx, "p", "l", "missing", "1.31.0")), "NotFound")
	providerErr(t, mustClusterOp(s.SetLabels(ctx, "p", "l", "missing", map[string]string{"a": "b"})), "NotFound")
}

func mustClusterOp(op containerstore.Operation, err error) error { return err }
