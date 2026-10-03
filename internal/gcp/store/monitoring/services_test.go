package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestMemoryStoreServiceCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	svc := Service{ID: "svc-1", DisplayName: "Checkout", Identifier: json.RawMessage(`{"cloudRun":{"serviceName":"checkout"}}`)}
	if err := s.CreateService(ctx, "p1", svc); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateService(ctx, "p1", svc); err != ErrServiceExists {
		t.Fatalf("duplicate service err = %v, want ErrServiceExists", err)
	}
	if _, err := s.GetService(ctx, "p1", "missing"); err != ErrServiceNotFound {
		t.Fatalf("get missing err = %v, want ErrServiceNotFound", err)
	}
	if _, err := s.GetService(ctx, "p2", "svc-1"); err != ErrServiceNotFound {
		t.Fatalf("get other project err = %v, want ErrServiceNotFound", err)
	}

	list, err := s.ListServices(ctx, "p1")
	if err != nil || len(list) != 1 || list[0].ID != "svc-1" {
		t.Fatalf("list = %+v, %v", list, err)
	}

	updated, err := s.UpdateServiceAtomic(ctx, "p1", "svc-1", func(cur Service) (Service, error) {
		cur.DisplayName = "Checkout v2"
		cur.UserLabels = map[string]string{"team": "payments"}
		return cur, nil
	})
	if err != nil || updated.DisplayName != "Checkout v2" || updated.UserLabels["team"] != "payments" {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	if _, err := s.UpdateServiceAtomic(ctx, "p1", "missing", func(cur Service) (Service, error) { return cur, nil }); err != ErrServiceNotFound {
		t.Fatalf("update missing err = %v, want ErrServiceNotFound", err)
	}

	if err := s.DeleteService(ctx, "p1", "svc-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteService(ctx, "p1", "svc-1"); err != ErrServiceNotFound {
		t.Fatalf("delete missing err = %v, want ErrServiceNotFound", err)
	}
}

func TestMemoryStoreServiceLevelObjectiveCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p1", Service{ID: "svc-1"})

	slo := ServiceLevelObjective{
		ID: "slo-1", ServiceID: "svc-1", DisplayName: "availability",
		ServiceLevelIndicator: json.RawMessage(`{"basicSli":{"availability":{}}}`),
		Goal:                  0.99, RollingPeriod: 30 * 24 * time.Hour,
	}
	if err := s.CreateServiceLevelObjective(ctx, "p1", slo); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateServiceLevelObjective(ctx, "p1", slo); err != ErrServiceLevelObjectiveExists {
		t.Fatalf("duplicate SLO err = %v, want ErrServiceLevelObjectiveExists", err)
	}
	if _, err := s.GetServiceLevelObjective(ctx, "p1", "svc-1", "missing"); err != ErrServiceLevelObjectiveNotFound {
		t.Fatalf("get missing err = %v, want ErrServiceLevelObjectiveNotFound", err)
	}
	// The same SLO id under a different service is a distinct resource.
	if _, err := s.GetServiceLevelObjective(ctx, "p1", "svc-2", "slo-1"); err != ErrServiceLevelObjectiveNotFound {
		t.Fatalf("get other service err = %v, want ErrServiceLevelObjectiveNotFound", err)
	}

	list, err := s.ListServiceLevelObjectives(ctx, "p1", "svc-1")
	if err != nil || len(list) != 1 || list[0].Goal != 0.99 || list[0].RollingPeriod != 30*24*time.Hour {
		t.Fatalf("list = %+v, %v", list, err)
	}

	updated, err := s.UpdateServiceLevelObjectiveAtomic(ctx, "p1", "svc-1", "slo-1", func(cur ServiceLevelObjective) (ServiceLevelObjective, error) {
		cur.Goal = 0.95
		return cur, nil
	})
	if err != nil || updated.Goal != 0.95 || updated.ID != "slo-1" || updated.ServiceID != "svc-1" {
		t.Fatalf("update = %+v, %v", updated, err)
	}

	if err := s.DeleteServiceLevelObjective(ctx, "p1", "svc-1", "slo-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteServiceLevelObjective(ctx, "p1", "svc-1", "slo-1"); err != ErrServiceLevelObjectiveNotFound {
		t.Fatalf("delete missing err = %v, want ErrServiceLevelObjectiveNotFound", err)
	}
}

func TestMemoryStoreDeleteServiceCascadesSLOs(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p1", Service{ID: "svc-1"})
	_ = s.CreateServiceLevelObjective(ctx, "p1", ServiceLevelObjective{ID: "slo-1", ServiceID: "svc-1"})
	_ = s.CreateServiceLevelObjective(ctx, "p1", ServiceLevelObjective{ID: "slo-2", ServiceID: "svc-1"})

	if err := s.DeleteService(ctx, "p1", "svc-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.ListServiceLevelObjectives(ctx, "p1", "svc-1"); len(got) != 0 {
		t.Fatalf("SLOs after cascade = %+v, want empty", got)
	}
}

func TestMemoryStoreServicesSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p1", Service{ID: "svc-1", DisplayName: "Checkout", Identifier: json.RawMessage(`{"custom":{"metricLabels":{"env":"prod"}}}`)})
	_ = s.CreateServiceLevelObjective(ctx, "p1", ServiceLevelObjective{
		ID: "slo-1", ServiceID: "svc-1", DisplayName: "latency",
		ServiceLevelIndicator: json.RawMessage(`{"requestBased":{"goodTotalRatio":{"totalServiceFilter":"metric.type=\"x\""}}}`),
		Goal:                  0.9, CalendarPeriod: 4,
	})

	if empty, err := s.IsEmpty(ctx); err != nil || empty {
		t.Fatalf("IsEmpty = %v, %v, want false", empty, err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	svcs, _ := s2.ListServices(ctx, "p1")
	if len(svcs) != 1 || svcs[0].DisplayName != "Checkout" || len(svcs[0].Identifier) == 0 {
		t.Fatalf("restored services = %+v", svcs)
	}
	slos, _ := s2.ListServiceLevelObjectives(ctx, "p1", "svc-1")
	if len(slos) != 1 || slos[0].Goal != 0.9 || slos[0].CalendarPeriod != 4 {
		t.Fatalf("restored SLOs = %+v", slos)
	}

	s2.Reset(ctx)
	if got, _ := s2.ListServices(ctx, "p1"); len(got) != 0 {
		t.Fatalf("services after reset = %+v", got)
	}
	if got, _ := s2.ListServiceLevelObjectives(ctx, "p1", "svc-1"); len(got) != 0 {
		t.Fatalf("SLOs after reset = %+v", got)
	}
}
