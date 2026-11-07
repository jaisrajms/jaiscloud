//go:build gcp_persistence

package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"sync"
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

// TestPostgresAlertPolicyStrategyRoundTrip verifies the alertStrategy object
// and the severity enum (TGM5) survive the Postgres columns (migration 060), an
// update and a Snapshot/Restore round trip — the memory/Postgres parity the
// store promises.
func TestPostgresAlertPolicyStrategyRoundTrip(t *testing.T) {
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

	enabled := true
	first := json.RawMessage(`{"autoClose":"18000s","notificationRateLimit":{"period":"300s"},"notificationChannelStrategy":[{"notificationChannelNames":["projects/p1/notificationChannels/nc1"],"renotifyInterval":"600s"}]}`)
	p := AlertPolicy{
		ID: "ap-strategy", DisplayName: "Uptime strategy", Combiner: 1, Enabled: &enabled,
		AlertStrategy: first, Severity: "WARNING", UserLabels: map[string]string{"team": "platform"},
	}
	if err := s.CreateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := s.GetAlertPolicy(ctx, "p1", "ap-strategy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !sameJSON(got.AlertStrategy, first) || got.Severity != "WARNING" || got.UserLabels["team"] != "platform" {
		t.Fatalf("get = %+v (strategy %s)", got, got.AlertStrategy)
	}

	// A masked update through the atomic path (the service's merge entry).
	second := json.RawMessage(`{"autoClose":"36000s","notificationRateLimit":{"period":"600s"}}`)
	updated, err := s.UpdateAlertPolicyAtomic(ctx, "p1", "ap-strategy", func(stored AlertPolicy) (AlertPolicy, error) {
		stored.AlertStrategy = second
		stored.Severity = "CRITICAL"
		stored.UserLabels = map[string]string{"team": "sre"}
		return stored, nil
	})
	if err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	if !sameJSON(updated.AlertStrategy, second) || updated.Severity != "CRITICAL" {
		t.Fatalf("updated = %+v (strategy %s)", updated, updated.AlertStrategy)
	}

	// Every declared severity round-trips through the TEXT column.
	for _, sev := range []string{"SEVERITY_UNSPECIFIED", "CRITICAL", "ERROR", "WARNING"} {
		p.AlertStrategy = second
		p.Severity = sev
		if err := s.UpdateAlertPolicy(ctx, "p1", p); err != nil {
			t.Fatalf("update severity %q: %v", sev, err)
		}
		got, _ := s.GetAlertPolicy(ctx, "p1", "ap-strategy")
		if got.Severity != sev {
			t.Fatalf("severity round-trip = %q, want %q", got.Severity, sev)
		}
	}

	// Clearing the strategy stores NULL, and a second policy keeps its own.
	p.AlertStrategy = nil
	p.Severity = ""
	p.UserLabels = nil
	if err := s.UpdateAlertPolicy(ctx, "p1", p); err != nil {
		t.Fatalf("clear strategy: %v", err)
	}
	cleared, _ := s.GetAlertPolicy(ctx, "p1", "ap-strategy")
	if len(cleared.AlertStrategy) != 0 || cleared.Severity != "" {
		t.Fatalf("cleared = %+v (strategy %s)", cleared, cleared.AlertStrategy)
	}
	if err := s.CreateAlertPolicy(ctx, "p1", AlertPolicy{
		ID: "ap-kept", DisplayName: "kept", AlertStrategy: first, Severity: "ERROR",
	}); err != nil {
		t.Fatalf("create second policy: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored, err := s.GetAlertPolicy(ctx, "p1", "ap-kept")
	if err != nil {
		t.Fatalf("restored policy: %v", err)
	}
	if !sameJSON(restored.AlertStrategy, first) || restored.Severity != "ERROR" {
		t.Fatalf("restored = %+v (strategy %s)", restored, restored.AlertStrategy)
	}
	restoredCleared, err := s.GetAlertPolicy(ctx, "p1", "ap-strategy")
	if err != nil {
		t.Fatalf("restored cleared policy: %v", err)
	}
	if len(restoredCleared.AlertStrategy) != 0 || restoredCleared.Severity != "" {
		t.Fatalf("restored cleared = %+v (strategy %s)", restoredCleared, restoredCleared.AlertStrategy)
	}
}

// TestPostgresAlertPolicyTimestampsRoundTrip verifies the create/update clock
// the policy's creationRecord/mutationRecord are rendered from survives the
// Postgres columns (migration 083), an atomic update and Snapshot/Restore —
// the memory/Postgres parity the store promises.
func TestPostgresAlertPolicyTimestampsRoundTrip(t *testing.T) {
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

	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	updatedAt := createdAt.Add(time.Hour)
	if err := s.CreateAlertPolicy(ctx, "p1", AlertPolicy{
		ID: "ap-times", DisplayName: "timed", CreateTime: createdAt, UpdateTime: createdAt,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetAlertPolicy(ctx, "p1", "ap-times")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.CreateTime.Equal(createdAt) || !got.UpdateTime.Equal(createdAt) {
		t.Fatalf("get times = %v/%v, want %v/%v", got.CreateTime, got.UpdateTime, createdAt, createdAt)
	}

	advanced, err := s.UpdateAlertPolicyAtomic(ctx, "p1", "ap-times", func(stored AlertPolicy) (AlertPolicy, error) {
		stored.UpdateTime = updatedAt
		return stored, nil
	})
	if err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	if !advanced.CreateTime.Equal(createdAt) || !advanced.UpdateTime.Equal(updatedAt) {
		t.Fatalf("updated times = %v/%v, want %v/%v", advanced.CreateTime, advanced.UpdateTime, createdAt, updatedAt)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := s.GetAlertPolicy(ctx, "p1", "ap-times")
	if err != nil {
		t.Fatalf("restored policy: %v", err)
	}
	if !restored.CreateTime.Equal(createdAt) || !restored.UpdateTime.Equal(updatedAt) {
		t.Fatalf("restored times = %v/%v, want %v/%v", restored.CreateTime, restored.UpdateTime, createdAt, updatedAt)
	}
}

// TestPostgresAtomicRetriesSerializationConflict drives many concurrent
// read-modify-writes of the same alert policy. Under SERIALIZABLE an unlucky
// interleaving aborts one transaction with SQLSTATE 40001; before SPK8 the
// store returned that raw (surfacing as HTTP 500), now it retries the whole
// transaction with a jittered backoff, so every call must succeed and the
// counter must reflect all increments (no lost update).
func TestPostgresAtomicRetriesSerializationConflict(t *testing.T) {
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

	enabled := true
	if err := s.CreateAlertPolicy(ctx, "p1", AlertPolicy{ID: "ap-1", DisplayName: "concurrent", Enabled: &enabled}); err != nil {
		t.Fatalf("create alert policy: %v", err)
	}

	const workers = 8
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.UpdateAlertPolicyAtomic(ctx, "p1", "ap-1", func(p AlertPolicy) (AlertPolicy, error) {
				n := 0
				if v := p.UserLabels["n"]; v != "" {
					n, _ = strconv.Atoi(v)
				}
				if p.UserLabels == nil {
					p.UserLabels = map[string]string{}
				}
				p.UserLabels["n"] = strconv.Itoa(n + 1)
				return p, nil
			})
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent atomic update failed (serialization conflict not retried?): %v", err)
	}

	got, err := s.GetAlertPolicy(ctx, "p1", "ap-1")
	if err != nil {
		t.Fatalf("get alert policy: %v", err)
	}
	if got.UserLabels["n"] != strconv.Itoa(workers) {
		t.Fatalf("counter = %q, want %d (lost update)", got.UserLabels["n"], workers)
	}
}
