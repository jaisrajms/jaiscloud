//go:build iceberg_k3d_e2e

package icebergk3d_test

import (
	"fmt"
	"testing"
	"time"
)

// TestDataprocIcebergK3d proves a Dataproc-submitted Spark job can use the
// emulator's Hive Metastore as its Iceberg catalog and write a real Iceberg
// table through the wired GCS connector (SPK1).
//
// Flow:
//
//   - stage the Iceberg Spark runtime on the emulator's GCS and pass it in the
//     job's jarFileUris (the deployed image has only the GCS connector)
//   - create a Metastore service and a GKE-backed cluster attached to it
//   - submit a pysparkJob that CREATE DATABASE/TABLE ... USING iceberg with
//     HiveCatalog (thrift) + HadoopFileIO, inserts rows and prints the count
//   - submit a second pysparkJob that reads the table back (count + snapshots)
//   - assert Iceberg metadata JSON and Parquet data objects really landed in
//     GCS under the declared table LOCATION
func TestDataprocIcebergK3d(t *testing.T) {
	requireK3d(t)

	base, stop := startPortForward(t)
	// Register the port-forward teardown first so it runs last (t.Cleanup is
	// LIFO): the resource cleanups below still need the forward.
	t.Cleanup(stop)

	// Unique per run so re-runs never collide with a prior run's bucket, HMS
	// database/table, cluster or job id in the shared Postgres-backed store.
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	bucket := "iceberg-k3d-" + run
	db := "iceberg_k3d_" + run
	table := "trades"
	cluster := "iceberg-cluster-" + run
	writeJob := "iceberg-write-" + run
	readJob := "iceberg-read-" + run
	location := fmt.Sprintf("gs://%s/%s/%s", bucket, run, table)

	ensureBucket(t, base, bucket)
	t.Cleanup(func() { deleteBucketObjects(t, base, bucket) })

	jarURI := stageIcebergJar(t, base, bucket)

	msRef := createMetastoreService(t, base)
	createGkeCluster(t, base, cluster, msRef)
	t.Cleanup(func() { deleteCluster(t, base, cluster) })

	props := icebergCatalogProps(bucket, run)

	// Job 1 — create the Iceberg table through the HMS catalog and insert rows.
	uploadText(t, base, bucket, "write.py", writeScript(db, table, location))
	submitPySparkJob(t, base, cluster, writeJob, fmt.Sprintf("gs://%s/write.py", bucket), jarURI, props)
	waitForState(t, base, writeJob, "DONE", 10*time.Minute)
	writeOut := driverOutput(t, base, writeJob)
	if got := parseMetric(t, writeOut, "ICEBERG_ROWS"); got != 3 {
		t.Fatalf("write job row count = %d, want 3\n%s", got, writeOut)
	}

	// Job 2 — read the table back: proves HMS registration persisted and the
	// Iceberg snapshot log is readable, not just that the write returned.
	uploadText(t, base, bucket, "read.py", readScript(db, table))
	submitPySparkJob(t, base, cluster, readJob, fmt.Sprintf("gs://%s/read.py", bucket), jarURI, props)
	waitForState(t, base, readJob, "DONE", 10*time.Minute)
	readOut := driverOutput(t, base, readJob)
	if got := parseMetric(t, readOut, "ICEBERG_ROWS"); got != 3 {
		t.Fatalf("read-back row count = %d, want 3\n%s", got, readOut)
	}
	if got := parseMetric(t, readOut, "ICEBERG_SNAPSHOTS"); got < 1 {
		t.Fatalf("read-back snapshot count = %d, want >= 1\n%s", got, readOut)
	}

	// Real bytes in GCS under the table LOCATION: Iceberg writes metadata JSON
	// and Parquet data files through the HadoopFileIO gs:// path.
	metadata := listObjects(t, base, bucket, fmt.Sprintf("%s/%s/metadata/", run, table))
	hasMetadataJSON := false
	for _, name := range metadata {
		if len(name) > 5 && name[len(name)-5:] == ".json" {
			hasMetadataJSON = true
			break
		}
	}
	if !hasMetadataJSON {
		t.Fatalf("no Iceberg metadata JSON objects under gs://%s/%s/%s/metadata/ (got %v)",
			bucket, run, table, metadata)
	}
	data := listObjects(t, base, bucket, fmt.Sprintf("%s/%s/data/", run, table))
	hasParquet := false
	for _, name := range data {
		if len(name) > 8 && name[len(name)-8:] == ".parquet" {
			hasParquet = true
			break
		}
	}
	if !hasParquet {
		t.Fatalf("no Iceberg Parquet data objects under gs://%s/%s/%s/data/ (got %v)",
			bucket, run, table, data)
	}

	t.Logf("Dataproc Iceberg table OK: rows=3 snapshots>=1 metadata=%d data=%d (gs://%s/%s/%s)",
		len(metadata), len(data), bucket, run, table)
}

// writeScript is the PySpark driver that creates and populates the Iceberg
// table through the HiveCatalog. It mirrors the SQL the external Spark gate
// uses (CREATE DATABASE IF NOT EXISTS / CREATE TABLE ... USING iceberg LOCATION
// / INSERT), printing the resulting row count.
func writeScript(db, table, location string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS hms.%s")
spark.sql(
    "CREATE TABLE IF NOT EXISTS hms.%s.%s "
    "(symbol string, price double, size int) "
    "USING iceberg LOCATION '%s'"
)
spark.sql(
    "INSERT INTO hms.%s.%s VALUES "
    "('BTC', 100.5, 2), ('ETH', 50.25, 3), ('SOL', 20.0, 5)"
)
rows = spark.sql("SELECT count(*) AS c FROM hms.%s.%s").collect()[0]["c"]
print(f"ICEBERG_ROWS={rows}")
spark.stop()
`, db, db, table, location, db, table, db, table)
}

// readScript is the PySpark driver that reads the table back from a fresh job:
// the row count and the Iceberg snapshot count.
func readScript(db, table string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
rows = spark.sql("SELECT count(*) AS c FROM hms.%s.%s").collect()[0]["c"]
snaps = spark.sql("SELECT count(*) AS c FROM hms.%s.%s.snapshots").collect()[0]["c"]
print(f"ICEBERG_ROWS={rows}")
print(f"ICEBERG_SNAPSHOTS={snaps}")
spark.stop()
`, db, table, db, table)
}
