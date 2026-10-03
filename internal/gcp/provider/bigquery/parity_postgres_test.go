//go:build gcp_persistence

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/store"
)

// TestSQLParityPostgres runs the identical SQL workload against the Postgres
// backend and asserts the encoded provider responses match the memory store's
// after JSON canonicalization (random job IDs are masked; timestamps are frozen,
// so structure/rows/types/statuses/errors are compared, not bytes). This is
// BQ4's cross-backend parity proof: because SQLite is disposable per-query
// scratch and both stores sit behind the same Catalog, only the store can make
// the results diverge — so a fingerprint diff localizes any regression to a
// store method.
//
// Like every gcp_persistence test, this shares one database and wipes the
// bigquery tables, so the gate must run with -p 1 (see Makefile / CI).
func TestSQLParityPostgres(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres SQL parity test")
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

	want := sqlParityFingerprint(t, bqstore.NewMemoryStore())
	got := sqlParityFingerprint(t, bqstore.NewPostgresStore(pg.Pool()))

	if got == want {
		return
	}
	if diff := firstParityDiff(want, got); diff != "" {
		t.Fatalf("memory and --dsn SQL results diverge: %s", diff)
	}
	t.Fatalf("memory and --dsn SQL fingerprints differ")
}

// firstParityDiff decodes both fingerprints and reports the first op whose
// canonical response or error differs, so a failure names the offending step.
func firstParityDiff(want, got string) string {
	var w, g []parityStep
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		return fmt.Sprintf("decode memory fingerprint: %v", err)
	}
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		return fmt.Sprintf("decode postgres fingerprint: %v", err)
	}
	if len(w) != len(g) {
		return fmt.Sprintf("step count memory=%d postgres=%d", len(w), len(g))
	}
	for i := range w {
		wb, _ := json.Marshal(w[i])
		gb, _ := json.Marshal(g[i])
		if string(wb) != string(gb) {
			return fmt.Sprintf("step %d (%s):\n  memory:   %s\n  postgres: %s", i, w[i].Op, wb, gb)
		}
	}
	return ""
}
