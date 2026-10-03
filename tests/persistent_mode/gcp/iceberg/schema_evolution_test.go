//go:build iceberg_e2e

package iceberg_test

import (
	"fmt"
	"testing"
)

// TestIceberg_HiveCatalog_SchemaEvolution adds a column to an existing table and
// verifies old rows return NULL for the new column while new rows have values.
func TestIceberg_HiveCatalog_SchemaEvolution(t *testing.T) {
	requireIcebergEnv(t)

	host := jaiscloudHost()

	// Job 1 — Create table with initial schema and insert 50 rows
	runSparkSQL(t, SparkJob{
		Name: "products-initial",
		SQL: fmt.Sprintf(`
%s
CREATE TABLE IF NOT EXISTS hms.%s.products (
  id   INT,
  name STRING
)
USING iceberg
LOCATION '%s';

INSERT INTO hms.%s.products
SELECT id, CONCAT('product-', CAST(id AS STRING))
FROM range(1, 51);
`, ensureDatabaseSQL(), icebergDB(), tableLocation("products"), icebergDB()),
	})

	// Job 2 — Add column and insert 50 more rows with price values
	runSparkSQL(t, SparkJob{
		Name: "products-evolve",
		SQL: fmt.Sprintf(`
ALTER TABLE hms.%s.products ADD COLUMN price DOUBLE;

INSERT INTO hms.%s.products (id, name, price)
SELECT id + 50, CONCAT('evolved-', CAST(id AS STRING)), CAST(id AS DOUBLE) * 9.99
FROM range(1, 51);
`, icebergDB(), icebergDB()),
	})

	// Job 3 — Total count
	runSparkSQL(t, SparkJob{
		Name: "products-count",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.products;
`, outputLoc("products-count"), icebergDB()),
	})

	// Job 4 — Rows without price (old rows)
	runSparkSQL(t, SparkJob{
		Name: "products-null-price-count",
		SQL: fmt.Sprintf(`
INSERT OVERWRITE DIRECTORY '%s'
USING JSON
SELECT COUNT(*) AS cnt FROM hms.%s.products WHERE price IS NULL;
`, outputLoc("products-nullprice"), icebergDB()),
	})

	// ── Assertions ────────────────────────────────────────────────────────────

	// a. Total count = 100
	countResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("products-count"))
	cnt, _ := countResult["cnt"].(float64)
	if int(cnt) != 100 {
		t.Errorf("expected total cnt=100, got %v", cnt)
	}

	// b. Exactly 50 rows have NULL price (the original rows)
	nullResult := findGCSJSON(t, host, "iceberg-warehouse", outputPrefix("products-nullprice"))
	nullCnt, _ := nullResult["cnt"].(float64)
	if int(nullCnt) != 50 {
		t.Errorf("expected 50 null-price rows, got %v", nullCnt)
	}
}
