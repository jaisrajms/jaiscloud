//go:build gcp_persistence

package monitoring

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestUptimeCheckMemoryPostgresParity runs the same uptime-check lifecycle
// through the in-memory ResourceStore and the PostgreSQL one, asserting the
// stored documents are identical. The caller-chosen name makes the server
// id assignment deterministic, so the two documents can be compared byte for
// byte rather than field by field.
func TestUptimeCheckMemoryPostgresParity(t *testing.T) {
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
	pgRes := store.ResourceStore(pg)
	pgRes.Reset(ctx)
	defer pgRes.Reset(ctx)

	mem := NewService(nil, "parity", WithResources(store.NewMemoryResourceStore()))
	psql := NewService(nil, "parity", WithResources(pgRes))

	const body = `{
		"name": "projects/parity/uptimeCheckConfigs/parity-check",
		"displayName": "parity check",
		"monitoredResource": {"type": "uptime_url", "labels": {"host": "parity.example.com"}},
		"httpCheck": {"path": "/healthz", "useSsl": true, "requestMethod": "GET"},
		"timeout": "10s",
		"period": "300s",
		"selectedRegions": ["USA", "EUROPE", "ASIA_PACIFIC"],
		"userLabels": {"env": "parity"}
	}`

	memCreated, err := mem.CreateUptimeCheckConfig(ctx, "parity", json.RawMessage(body))
	if err != nil {
		t.Fatalf("memory create: %v", err)
	}
	pgCreated, err := psql.CreateUptimeCheckConfig(ctx, "parity", json.RawMessage(body))
	if err != nil {
		t.Fatalf("postgres create: %v", err)
	}
	assertSameUptimeDoc(t, "create", memCreated.Document, pgCreated.Document)

	maskBody := `{"displayName":"parity renamed","httpCheck":{"path":"/ready"}}`
	memUpdated, err := mem.UpdateUptimeCheckConfig(ctx, "parity", "parity-check", json.RawMessage(maskBody), []string{"displayName", "httpCheck.path"})
	if err != nil {
		t.Fatalf("memory masked update: %v", err)
	}
	pgUpdated, err := psql.UpdateUptimeCheckConfig(ctx, "parity", "parity-check", json.RawMessage(maskBody), []string{"displayName", "httpCheck.path"})
	if err != nil {
		t.Fatalf("postgres masked update: %v", err)
	}
	assertSameUptimeDoc(t, "masked update", memUpdated.Document, pgUpdated.Document)

	memList, _, err := mem.ListUptimeCheckConfigs(ctx, "parity", `display_name = "parity renamed"`, 0, "")
	if err != nil {
		t.Fatalf("memory list: %v", err)
	}
	pgList, _, err := psql.ListUptimeCheckConfigs(ctx, "parity", `display_name = "parity renamed"`, 0, "")
	if err != nil {
		t.Fatalf("postgres list: %v", err)
	}
	if len(memList) != 1 || len(pgList) != 1 {
		t.Fatalf("list lengths = %d/%d, want 1/1", len(memList), len(pgList))
	}
	assertSameUptimeDoc(t, "list", memList[0].Document, pgList[0].Document)

	// A fresh --dsn read returns the same declaration the update wrote (no extra
	// fields materialize from JSONB storage).
	pgGot, err := psql.GetUptimeCheckConfig(ctx, "parity", "parity-check")
	if err != nil {
		t.Fatalf("postgres get: %v", err)
	}
	assertSameUptimeDoc(t, "postgres get", pgUpdated.Document, pgGot.Document)

	for _, s := range []*Service{mem, psql} {
		if err := s.DeleteUptimeCheckConfig(ctx, "parity", "parity-check"); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if _, err := s.GetUptimeCheckConfig(ctx, "parity", "parity-check"); err == nil {
			t.Fatal("get after delete should fail")
		}
	}
}

// assertSameUptimeDoc compares two stored documents canonically: PostgreSQL
// JSONB re-renders the bytes it was given (key order and spacing), so the two
// backends are compared on the decoded value rather than on the raw text.
func assertSameUptimeDoc(t *testing.T, step string, want, got json.RawMessage) {
	t.Helper()
	var wantVal, gotVal any
	if err := json.Unmarshal(want, &wantVal); err != nil {
		t.Fatalf("%s: decode memory document %s: %v", step, want, err)
	}
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("%s: decode postgres document %s: %v", step, got, err)
	}
	wantCanon, _ := json.Marshal(wantVal)
	gotCanon, _ := json.Marshal(gotVal)
	if string(wantCanon) != string(gotCanon) {
		t.Fatalf("%s: memory document %s != postgres document %s", step, wantCanon, gotCanon)
	}
}
