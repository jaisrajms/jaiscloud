//go:build gcp_persistence

package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresDistributionRoundTrip verifies a DISTRIBUTION point value survives
// a Postgres write/read and a Snapshot/Restore round trip (the points column is
// JSONB, so the new distribution shape must marshal through it intact).
func TestPostgresDistributionRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres monitoring test")
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
	defer s.Reset(ctx)

	base := time.Now().UTC().Truncate(time.Microsecond)
	dist := &Distribution{
		Count:                 6,
		Mean:                  3.5,
		SumOfSquaredDeviation: 17.5,
		Range:                 &DistributionRange{Min: 0, Max: 10},
		BucketOptions: &BucketOptions{
			Linear: &LinearBuckets{NumFiniteBuckets: 3, Width: 2, Offset: 1},
		},
		BucketCounts: []int64{1, 2, 2, 1, 0},
	}
	ts := TimeSeries{
		MetricType:   "custom.googleapis.com/dist",
		ResourceType: "global",
		Points:       []Point{{EndTime: base, Value: TypedValue{DistributionValue: dist}}},
	}
	if err := s.CreateTimeSeries(ctx, "p1", ts); err != nil {
		t.Fatalf("create: %v", err)
	}

	assertDist := func(t *testing.T, store *PostgresStore) {
		t.Helper()
		got, err := store.ListTimeSeries(ctx, "p1")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(got) != 1 || len(got[0].Points) != 1 {
			t.Fatalf("series = %+v", got)
		}
		if !reflect.DeepEqual(got[0].Points[0].Value.DistributionValue, dist) {
			t.Fatalf("distribution = %+v, want %+v", got[0].Points[0].Value.DistributionValue, dist)
		}
	}
	assertDist(t, s)

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Restore deletes and re-inserts the whole table.
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	assertDist(t, s)
}

// TestPostgresServicesRoundTrip verifies Service + ServiceLevelObjective CRUD,
// the DeleteService cascade, and a Snapshot/Restore round trip through the
// Postgres-backed store (migration 045).
func TestPostgresServicesRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres monitoring test")
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
	defer s.Reset(ctx)
	s.Reset(ctx)

	svc := Service{
		ID: "svc-1", DisplayName: "Checkout",
		Identifier:   json.RawMessage(`{"cloudRun":{"serviceName":"checkout"}}`),
		BasicService: json.RawMessage(`{"basicService":{"serviceType":"CLOUD_RUN"}}`),
		UserLabels:   map[string]string{"team": "payments"},
	}
	if err := s.CreateService(ctx, "p1", svc); err != nil {
		t.Fatalf("create service: %v", err)
	}
	if err := s.CreateService(ctx, "p1", svc); err != ErrServiceExists {
		t.Fatalf("duplicate service err = %v, want ErrServiceExists", err)
	}

	slo := ServiceLevelObjective{
		ID: "slo-1", ServiceID: "svc-1", DisplayName: "availability",
		ServiceLevelIndicator: json.RawMessage(`{"basicSli":{"availability":{}}}`),
		Goal:                  0.99, RollingPeriod: 30 * 24 * time.Hour,
	}
	if err := s.CreateServiceLevelObjective(ctx, "p1", slo); err != nil {
		t.Fatalf("create SLO: %v", err)
	}

	assertServices := func(t *testing.T, store *PostgresStore) {
		t.Helper()
		got, err := store.GetService(ctx, "p1", "svc-1")
		if err != nil {
			t.Fatalf("get service: %v", err)
		}
		if got.DisplayName != "Checkout" || got.UserLabels["team"] != "payments" ||
			string(got.Identifier) == "" || string(got.BasicService) == "" {
			t.Fatalf("service = %+v", got)
		}
		slos, err := store.ListServiceLevelObjectives(ctx, "p1", "svc-1")
		if err != nil || len(slos) != 1 || slos[0].RollingPeriod != 30*24*time.Hour {
			t.Fatalf("SLOs = %+v, %v", slos, err)
		}
	}
	assertServices(t, s)

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	assertServices(t, s)

	// DeleteService cascades the SLO.
	if err := s.DeleteService(ctx, "p1", "svc-1"); err != nil {
		t.Fatalf("delete service: %v", err)
	}
	if got, _ := s.ListServiceLevelObjectives(ctx, "p1", "svc-1"); len(got) != 0 {
		t.Fatalf("SLOs after cascade = %+v, want empty", got)
	}
}
