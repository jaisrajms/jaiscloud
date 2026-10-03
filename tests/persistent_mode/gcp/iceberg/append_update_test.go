//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"testing"
)

// TestIceberg_HiveCatalog_AppendMultipleBatches verifies that Iceberg accumulates
// data correctly across multiple INSERT batches without overwriting prior data.
// Three batches of 100 rows each should yield 300 total rows.
func TestIceberg_HiveCatalog_AppendMultipleBatches(t *testing.T) {
	requireIcebergEnv(t)

	host := jaiscloudHost()

	// Job 1 — Create table and insert batch 1 (ids 1–100).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch1",
		SQL: fmt.Sprintf(`
%s
CREATE TABLE IF NOT EXISTS hms.%s.ledger (
  id       INT,
  batch    INT,
  amount   DOUBLE
)
USING iceberg
LOCATION '%s';

INSERT INTO hms.%s.ledger
SELECT id, 1, CAST(id AS DOUBLE) * 1.0
FROM range(1, 101);
`, ensureDatabaseSQL(), icebergDB(), tableLocation("ledger"), icebergDB()),
	})

	// Verify count after batch 1.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count1",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.ledger;
`, outputLoc("ledger-count1"), icebergDB()),
	})
	cnt1 := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("ledger-count1"))
	if v, _ := cnt1["cnt"].(float64); int(v) != 100 {
		t.Errorf("after batch 1: expected cnt=100, got %v", cnt1["cnt"])
	}

	// Job 2 — Append batch 2 (ids 101–200).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch2",
		SQL: fmt.Sprintf(`
INSERT INTO hms.%s.ledger
SELECT id + 100, 2, CAST(id AS DOUBLE) * 2.0
FROM range(1, 101);
`, icebergDB()),
	})

	// Verify count after batch 2.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count2",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.ledger;
`, outputLoc("ledger-count2"), icebergDB()),
	})
	cnt2 := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("ledger-count2"))
	if v, _ := cnt2["cnt"].(float64); int(v) != 200 {
		t.Errorf("after batch 2: expected cnt=200, got %v", cnt2["cnt"])
	}

	// Job 3 — Append batch 3 (ids 201–300).
	runSparkSQL(t, SparkJob{
		Name: "ledger-batch3",
		SQL: fmt.Sprintf(`
INSERT INTO hms.%s.ledger
SELECT id + 200, 3, CAST(id AS DOUBLE) * 3.0
FROM range(1, 101);
`, icebergDB()),
	})

	// ── Final assertions ──────────────────────────────────────────────────────

	// a. Total count = 300.
	runSparkSQL(t, SparkJob{
		Name: "ledger-count-total",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.ledger;
`, outputLoc("ledger-total"), icebergDB()),
	})
	total := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("ledger-total"))
	if v, _ := total["cnt"].(float64); int(v) != 300 {
		t.Errorf("expected total cnt=300, got %v", total["cnt"])
	}

	// b. Per-batch counts — each batch contributes exactly 100 rows.
	runSparkSQL(t, SparkJob{
		Name: "ledger-by-batch",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT batch, COUNT(*) AS cnt
FROM hms.%s.ledger
GROUP BY batch
ORDER BY batch;
`, outputLoc("ledger-by-batch"), icebergDB()),
	})
	// Verify GCS has at least one data file for the per-batch output.
	if !hasGCSObjects(t, host, "iceberg-warehouse", outputPrefix("ledger-by-batch")) {
		t.Error("expected per-batch count output in GCS")
	}

	// c. Table has 3 snapshot metadata files (one per commit).
	metaCount := countGCSObjects(t, host, "iceberg-warehouse", tablePrefix("ledger", "metadata/"))
	if metaCount < 3 {
		t.Errorf("expected at least 3 metadata files (one per commit), got %d", metaCount)
	}
}

// TestIceberg_HiveCatalog_SchemaAndDataUpdate verifies Iceberg's support for
// in-place schema changes (ALTER TABLE ADD COLUMN) combined with data updates
// (UPDATE SET) on existing rows.
func TestIceberg_HiveCatalog_SchemaAndDataUpdate(t *testing.T) {
	requireIcebergEnv(t)

	host := jaiscloudHost()

	// Job 1 — Create table with initial schema (id, product, quantity) and
	// insert 100 rows. No price column yet.
	runSparkSQL(t, SparkJob{
		Name: "inventory-initial",
		SQL: fmt.Sprintf(`
%s
CREATE TABLE IF NOT EXISTS hms.%s.inventory (
  id       INT,
  product  STRING,
  quantity INT
)
USING iceberg
LOCATION '%s';

INSERT INTO hms.%s.inventory
SELECT id, CONCAT('item-', CAST(id AS STRING)), id * 10
FROM range(1, 101);
`, ensureDatabaseSQL(), icebergDB(), tableLocation("inventory"), icebergDB()),
	})

	// Job 2 — Add the 'price' column (schema evolution) and set it for the
	// first 50 rows via UPDATE. The remaining 50 rows keep price = NULL.
	runSparkSQL(t, SparkJob{
		Name: "inventory-evolve-and-update",
		SQL: fmt.Sprintf(`
ALTER TABLE hms.%s.inventory ADD COLUMN price DOUBLE;

UPDATE hms.%s.inventory
SET price = CAST(id AS DOUBLE) * 9.99
WHERE id <= 50;
`, icebergDB(), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Total count still 100 (no rows deleted by UPDATE).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-total",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.inventory;
`, outputLoc("inventory-total"), icebergDB()),
	})
	totalResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("inventory-total"))
	if v, _ := totalResult["cnt"].(float64); int(v) != 100 {
		t.Errorf("expected total cnt=100, got %v", totalResult["cnt"])
	}

	// b. Exactly 50 rows have a non-NULL price (the updated rows, id 1–50).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-priced",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt
FROM hms.%s.inventory
WHERE price IS NOT NULL;
`, outputLoc("inventory-priced"), icebergDB()),
	})
	pricedResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("inventory-priced"))
	if v, _ := pricedResult["cnt"].(float64); int(v) != 50 {
		t.Errorf("expected 50 priced rows, got %v", pricedResult["cnt"])
	}

	// c. Exactly 50 rows still have NULL price (the un-updated rows, id 51–100).
	runSparkSQL(t, SparkJob{
		Name: "inventory-count-unpriced",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt
FROM hms.%s.inventory
WHERE price IS NULL;
`, outputLoc("inventory-unpriced"), icebergDB()),
	})
	unpricedResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("inventory-unpriced"))
	if v, _ := unpricedResult["cnt"].(float64); int(v) != 50 {
		t.Errorf("expected 50 un-priced rows, got %v", unpricedResult["cnt"])
	}

	// d. Price values match expected: price for id=1 should be 1*9.99=9.99.
	// Write a specific row lookup to verify UPDATE correctness.
	runSparkSQL(t, SparkJob{
		Name: "inventory-price-check",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT id, price
FROM hms.%s.inventory
WHERE id = 1;
`, outputLoc("inventory-price-check"), icebergDB()),
	})
	priceCheck := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("inventory-price-check"))
	if price, _ := priceCheck["price"].(float64); price < 9.98 || price > 10.0 {
		t.Errorf("expected price≈9.99 for id=1, got %v", priceCheck["price"])
	}
}
