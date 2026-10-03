//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"math"
	"os"
	"testing"
)

// TestIceberg_Persistence_AcrossRestart verifies that Iceberg table data and
// metadata survive a JaisCloud GCP process restart.
//
// The test manages its own jaiscloud-gcp server instance on a separate port so
// it can control the lifecycle independently of the shared test server. It is
// built directly from --dsn + --blob-dir (the GCP binary has no --mode flag).
//
// Required env vars:
//
//	SPARK_E2E_ICEBERG_GCP_IMAGE — Docker image with Spark + Iceberg (same as other tests)
//	JAISCLOUD_DSN               — PostgreSQL DSN for persistence
//
// Optional env vars:
//
//	JAISCLOUD_GCP_BIN            — path to jaiscloud-gcp binary (default: ../../../../jaiscloud-gcp)
//	JAISCLOUD_GCP_PERSIST_PORT   — port for the managed server (default: 8098)
//	SPARK_E2E_HMS_ENDPOINT       — Thrift HMS endpoint (default: host.docker.internal:9083)
func TestIceberg_Persistence_AcrossRestart(t *testing.T) {
	requireIcebergEnv(t)

	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)

	// Blob storage must survive across restarts — use t.TempDir() which persists
	// for the duration of the test function (cleanup runs after the function returns).
	blobDir := t.TempDir()

	const (
		bucket    = "persist-warehouse"
		hmsDB     = "persist_db"
		tableName = "checkpoints"
		rowCount  = 200
	)

	// ── Phase 1: start server, create infrastructure, insert data ────────────

	proc1 := startGCPProcess(t, port, dsn, blobDir)
	waitForHealth(t, host)

	// Create shared infrastructure for this test's server instance.
	createGCSBucket(host, bucket)

	overrideConf := []string{
		fmt.Sprintf("spark.sql.catalog.hms.warehouse=gs://%s/", bucket),
	}

	// Create table and insert rowCount rows. The Hive database is created
	// idempotently from Spark SQL (no Go-side metastore client exists — §7 D4).
	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-write",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS hms.%s;
CREATE TABLE IF NOT EXISTS hms.%s.%s (
  id    INT,
  value STRING
)
USING iceberg
LOCATION 'gs://%s/%s';

%s
`, hmsDB, hmsDB, tableName, bucket, tableName, buildPersistInsertSQL(hmsDB, tableName, rowCount)),
	})

	// Read summary stats before restart: MIN(id), MAX(id), SUM(id), COUNT(*).
	// SUM of 1..N = N*(N+1)/2 — used to detect data loss or duplication.
	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-stats-before",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY 'gs://%s/stats-before/'
USING JSON
SELECT
  MIN(id)               AS min_id,
  MAX(id)               AS max_id,
  SUM(CAST(id AS LONG)) AS sum_id,
  COUNT(*)              AS cnt
FROM hms.%s.%s;
`, bucket, hmsDB, tableName),
	})

	statsBefore := findGCSJSON(t, host, bucket, "stats-before/")
	assertPersistStats(t, "before restart", statsBefore, rowCount)

	// ── Kill server and restart ───────────────────────────────────────────────

	t.Log("killing server for restart test")
	stopProcess(t, proc1)

	t.Log("restarting server")
	proc2 := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc2)
	waitForHealth(t, host)

	// ── Phase 2: verify data survived the restart ─────────────────────────────

	runSparkSQLOnHost(t, host, SparkJob{
		Name:      "persist-stats-after",
		ExtraConf: overrideConf,
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY 'gs://%s/stats-after/'
USING JSON
SELECT
  MIN(id)               AS min_id,
  MAX(id)               AS max_id,
  SUM(CAST(id AS LONG)) AS sum_id,
  COUNT(*)              AS cnt
FROM hms.%s.%s;
`, bucket, hmsDB, tableName),
	})

	statsAfter := findGCSJSON(t, host, bucket, "stats-after/")
	assertPersistStats(t, "after restart", statsAfter, rowCount)

	// Compare before/after to confirm exact match (no data loss or duplication).
	if statsBefore["sum_id"] != statsAfter["sum_id"] {
		t.Errorf("sum_id mismatch: before=%v after=%v", statsBefore["sum_id"], statsAfter["sum_id"])
	}
}

// buildPersistInsertSQL generates an INSERT of n rows with id 1..n.
func buildPersistInsertSQL(hmsDB, tableName string, n int) string {
	return fmt.Sprintf(
		"INSERT INTO hms.%s.%s SELECT id, CONCAT('row-', CAST(id AS STRING)) FROM range(1, %d);",
		hmsDB, tableName, n+1,
	)
}

// assertPersistStats verifies that a summary-stats JSON row matches expectations
// for a table with sequential ids 1..n.
func assertPersistStats(t *testing.T, label string, stats map[string]any, n int) {
	t.Helper()

	cnt, _ := stats["cnt"].(float64)
	if int(cnt) != n {
		t.Errorf("[%s] expected cnt=%d, got %v", label, n, cnt)
	}

	minID, _ := stats["min_id"].(float64)
	if int(minID) != 1 {
		t.Errorf("[%s] expected min_id=1, got %v", label, minID)
	}

	maxID, _ := stats["max_id"].(float64)
	if int(maxID) != n {
		t.Errorf("[%s] expected max_id=%d, got %v", label, n, maxID)
	}

	expectedSum := float64(n) * float64(n+1) / 2
	sumID, _ := stats["sum_id"].(float64)
	if math.Abs(sumID-expectedSum) > 0.5 {
		t.Errorf("[%s] expected sum_id=%.0f, got %v", label, expectedSum, sumID)
	}
}
