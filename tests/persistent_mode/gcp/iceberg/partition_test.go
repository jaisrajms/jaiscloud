//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"testing"
)

// TestIceberg_HiveCatalog_PartitionedTable creates a DATE-partitioned Iceberg table,
// inserts 300 rows across 3 dates, and verifies that a date-filtered query returns
// only the matching partition.
func TestIceberg_HiveCatalog_PartitionedTable(t *testing.T) {
	requireIcebergEnv(t)

	host := jaiscloudHost()

	// Job 1 — Create partitioned table and insert 300 rows (100 per date)
	runSparkSQL(t, SparkJob{
		Name: "create-sales",
		SQL: fmt.Sprintf(`
%s
CREATE TABLE IF NOT EXISTS hms.%s.sales (
  id        INT,
  region    STRING,
  amount    DOUBLE,
  sale_date DATE
)
USING iceberg
PARTITIONED BY (days(sale_date))
LOCATION '%s';

INSERT INTO hms.%s.sales
SELECT
  id,
  CASE WHEN id %% 3 = 0 THEN 'us-east' WHEN id %% 3 = 1 THEN 'us-west' ELSE 'eu-central' END AS region,
  CAST(id * 1.5 AS DOUBLE) AS amount,
  DATE '2026-01-01' AS sale_date
FROM range(1, 101);

INSERT INTO hms.%s.sales
SELECT
  id + 100,
  'us-east',
  CAST(id * 2.0 AS DOUBLE),
  DATE '2026-01-02'
FROM range(1, 101);

INSERT INTO hms.%s.sales
SELECT
  id + 200,
  'eu-central',
  CAST(id * 2.5 AS DOUBLE),
  DATE '2026-01-03'
FROM range(1, 101);
`, ensureDatabaseSQL(), icebergDB(), tableLocation("sales"), icebergDB(), icebergDB(), icebergDB()),
	})

	// Job 2 — Filtered read for 2026-01-02
	runSparkSQL(t, SparkJob{
		Name: "count-by-date",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt
FROM hms.%s.sales
WHERE sale_date = DATE '2026-01-02';
`, outputLoc("sales-count"), icebergDB()),
	})

	// Job 3 — Full count
	runSparkSQL(t, SparkJob{
		Name: "count-total",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.sales;
`, outputLoc("sales-total"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// Note: Iceberg tables do NOT register partitions via the Hive metastore
	// partition API. Partition metadata is embedded in Iceberg's own metadata
	// files (in GCS) — this is the correct behavior in both real GCP and
	// JaisCloud.

	// a. GCS has 3 partition directories
	if countGCSObjects(t, host, "iceberg-warehouse", tablePrefix("sales", "data/")) < 3 {
		t.Error("expected at least 3 data files (one per partition) in sales/data/")
	}

	// b. Filtered count = 100 (partition pruning on 2026-01-02)
	filteredResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("sales-count"))
	cnt, _ := filteredResult["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected filtered cnt=100, got %v", cnt)
	}

	// c. Total count = 300
	totalResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("sales-total"))
	total, _ := totalResult["cnt"].(float64)
	if int(total) != 300 {
		t.Errorf("expected total cnt=300, got %v", total)
	}
}
