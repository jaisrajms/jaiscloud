//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"testing"
)

// TestIceberg_HiveCatalog_WriteAndRead creates an Iceberg table, inserts 100 rows,
// reads them back via a COUNT(*) job, and verifies the result via GCS object
// assertions.
func TestIceberg_HiveCatalog_WriteAndRead(t *testing.T) {
	requireIcebergEnv(t)

	host := jaiscloudHost()

	// Job 1 — Write: create table and insert 100 rows
	runSparkSQL(t, SparkJob{
		Name: "write-events",
		SQL: fmt.Sprintf(`
%s
CREATE TABLE IF NOT EXISTS hms.%s.events (
  id         INT,
  name       STRING,
  amount     DOUBLE,
  created_at TIMESTAMP
)
USING iceberg
LOCATION '%s';

%s
`, ensureDatabaseSQL(), icebergDB(), tableLocation("events"), buildInsertSQL(icebergDB(), "events", 100)),
	})

	// Job 2 — Read: COUNT(*) and write result to GCS
	runSparkSQL(t, SparkJob{
		Name: "read-events-count",
		SQL: fmt.Sprintf(`
SELECT COUNT(*) AS cnt FROM hms.%s.events;
-- Write result to GCS as JSON
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.events;
`, icebergDB(), outputLoc("events-count"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. GCS metadata directory contains at least 1 JSON file
	if !hasGCSObjects(t, host, "iceberg-warehouse", tablePrefix("events", "metadata/")) {
		t.Error("expected metadata files in gs://iceberg-warehouse/.../events/metadata/")
	}

	// b. GCS data directory contains at least 1 Parquet file
	if !hasGCSObjects(t, host, "iceberg-warehouse", tablePrefix("events", "data/")) {
		t.Error("expected data files in gs://iceberg-warehouse/.../events/data/")
	}

	// c. COUNT result = 100
	result := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("events-count"))
	cnt, _ := result["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected cnt=100, got %v", cnt)
	}
}

// buildInsertSQL generates a bulk INSERT of n rows into the given db.table.
func buildInsertSQL(db, table string, n int) string {
	sql := fmt.Sprintf("INSERT INTO hms.%s.%s VALUES\n", db, table)
	for i := 1; i <= n; i++ {
		comma := ","
		if i == n {
			comma = ";"
		}
		sql += fmt.Sprintf("  (%d, 'user%d', %.2f, current_timestamp())%s\n", i, i, float64(i)*0.99, comma)
	}
	return sql
}
