//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// testRunID is a random 6-hex-char string unique to this test binary invocation.
// All Hive DB names and GCS paths are scoped under this ID so that concurrent
// or back-to-back test runs never share state.
var testRunID string

// TestMain creates the shared Iceberg infrastructure once before all tests.
// Shared resources (scoped to this run):
//   - GCS bucket "iceberg-warehouse"
//
// Divergence from the AWS harness: the AWS TestMain also pre-creates the Glue
// database via the Go Glue SDK. There is no Go-side Hive metastore client, so
// the run-scoped Hive database is instead created idempotently from Spark SQL
// (ensureDatabaseSQL) inside each test — see helpers_test.go.
//
// The lock manager is left at Spark's default (InMemoryLockManager): each
// Docker container runs its own JVM, and the HiveCatalog lock path runs against
// the real jc_hms_locks state machine on the default lock-enabled=true.
func TestMain(m *testing.M) {
	testRunID = fmt.Sprintf("%06x", rand.Uint32())

	host := jaiscloudHost()

	// 1. Create GCS warehouse bucket (idempotent — error ignored if already exists)
	createGCSBucket(host, "iceberg-warehouse")

	code := m.Run()

	// Teardown: GCS objects and any Hive tables are left intact for debugging
	// (no Go-side metastore client exists to drop them).
	os.Exit(code)
}
