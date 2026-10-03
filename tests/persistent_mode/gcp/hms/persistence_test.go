//go:build gcp_persistence

// Package hms_test exercises the Dataproc Metastore Hive Metastore Thrift
// serving plane (:hms-port) through an independent, off-the-shelf Go HMS client
// (github.com/slachiewicz/hms-client-go) — the GCP analogue of driving the AWS
// Glue emulator with the official AWS SDK.
//
// Coverage: database / table / partition CRUD, the Hive-3.x get_table_meta and
// alter_table_with_cascade paths (via the client's generated Thrift package),
// and survival of the whole catalog across a jaiscloud-gcp restart backed by
// PostgreSQL (--dsn).
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN (e.g. postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud)
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN          — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_PERSIST_PORT — control-plane port for the managed server (default 8099)
//	JAISCLOUD_GCP_HMS_PORT     — Thrift serving-plane port (default 9084)
package hms_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	hms "github.com/slachiewicz/hms-client-go"
	hive_metastore "github.com/slachiewicz/hms-client-go/gen/hive_metastore"
)

// TestHMSLifecyclePersistence drives the HMS serving plane end-to-end with the
// external client, then restarts the server against the same DSN and verifies
// the catalog survived.
func TestHMSLifecyclePersistence(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping HMS persistence test")
	}

	port := persistPort()
	hport := hmsPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	uri := fmt.Sprintf("thrift://127.0.0.1:%d", hport)
	blobDir := t.TempDir()

	// The HMS catalog is single-global, so scope every name to this run to stay
	// idempotent across re-runs.
	db := fmt.Sprintf("hms_persist_%06x", rand.Uint32())
	const tbl = "events"
	const renamed = "events_v2"
	location := "gs://bucket/wh/" + db + "/" + tbl

	// ── Phase 1: start the server and run the lifecycle ────────────────────
	proc1 := startGCPProcess(t, port, hport, dsn, blobDir)
	waitForHealth(t, host)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Deliberately no WithoutUGI(): a real client issues set_ugi on connect, so
	// this also exercises the emulator's identity RPC.
	c, err := hms.New(ctx, uri, hms.WithTimeout(15*time.Second))
	if err != nil {
		t.Fatalf("hms.New: %v", err)
	}

	if err := c.CreateDatabase(ctx, &hms.Database{Name: db, LocationURI: "gs://bucket/wh/" + db}); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if got, err := c.GetDatabase(ctx, db); err != nil {
		t.Fatalf("GetDatabase: %v", err)
	} else if got.Name != db {
		t.Fatalf("GetDatabase name=%q want %q", got.Name, db)
	}

	table := hms.NewIcebergTable(db, tbl, location, location+"/metadata/v1.metadata.json",
		[]*hms.FieldSchema{{Name: "id", Type: "bigint"}})
	table.PartitionKeys = []*hms.FieldSchema{{Name: "ds", Type: "string"}}
	if err := c.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// GetTable exercises get_table_req (no legacy fallback in Hive-4-IDL clients).
	if got, err := c.GetTable(ctx, db, tbl); err != nil {
		t.Fatalf("GetTable: %v", err)
	} else if got.TableName != tbl {
		t.Fatalf("GetTable name=%q want %q", got.TableName, tbl)
	}
	// GetTables exercises get_table_objects_by_name_req; a missing name is skipped.
	if got, err := c.GetTables(ctx, db, []string{tbl, "missing"}); err != nil {
		t.Fatalf("GetTables: %v", err)
	} else if len(got) != 1 || got[0].TableName != tbl {
		t.Fatalf("GetTables=%v want exactly [%s]", got, tbl)
	}

	wantParts := []string{"ds=2024", "ds=2025"}
	if err := c.AddPartitions(ctx, db, tbl, []*hms.Partition{
		{Values: []string{"2024"}},
		{Values: []string{"2025"}},
	}, false); err != nil {
		t.Fatalf("AddPartitions: %v", err)
	}
	if got, err := c.GetPartitions(ctx, db, tbl, -1); err != nil {
		t.Fatalf("GetPartitions: %v", err)
	} else if len(got) != 2 {
		t.Fatalf("GetPartitions len=%d want 2", len(got))
	}
	if names, err := c.GetPartitionNames(ctx, db, tbl, -1); err != nil {
		t.Fatalf("GetPartitionNames: %v", err)
	} else if len(names) != 2 || names[0] != wantParts[0] || names[1] != wantParts[1] {
		t.Fatalf("GetPartitionNames=%v want %v", names, wantParts)
	}

	// Hive-3.x paths via the generated Thrift client: get_table_meta and
	// alter_table_with_cascade (the clean hms API exposes neither).
	gen, closeGen := dialGenerated(t, fmt.Sprintf("127.0.0.1:%d", hport))
	if metas, err := gen.GetTableMeta(ctx, db, tbl, nil); err != nil {
		t.Fatalf("get_table_meta: %v", err)
	} else if len(metas) != 1 || metas[0].TableName != tbl || metas[0].TableType != "EXTERNAL_TABLE" {
		t.Fatalf("get_table_meta=%+v want one EXTERNAL_TABLE %s", metas, tbl)
	}

	// Cascade rename: fetch the stored Table, rename it, and alter through the
	// cascade path (cascade=true).
	req := hive_metastore.NewGetTableRequest()
	req.DbName, req.TblName = db, tbl
	res, err := gen.GetTableReq(ctx, req)
	if err != nil {
		t.Fatalf("gen GetTableReq: %v", err)
	}
	stored := res.Table
	stored.TableName = renamed
	if err := gen.AlterTableWithCascade(ctx, db, tbl, stored, true); err != nil {
		t.Fatalf("alter_table_with_cascade: %v", err)
	}
	if _, err := c.GetTable(ctx, db, tbl); err == nil {
		t.Fatalf("old table name %q still resolves after cascade rename", tbl)
	}
	if got, err := c.GetTable(ctx, db, renamed); err != nil {
		t.Fatalf("GetTable(%s) after cascade rename: %v", renamed, err)
	} else if got.TableName != renamed {
		t.Fatalf("renamed table name=%q want %q", got.TableName, renamed)
	}
	closeGen()

	c.Close()

	// ── Phase 2: restart against the same DSN and verify survival ──────────
	stopProcess(t, proc1)

	proc2 := startGCPProcess(t, port, hport, dsn, blobDir)
	defer stopProcess(t, proc2)
	waitForHealth(t, host)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()

	c2, err := hms.New(ctx2, uri, hms.WithTimeout(15*time.Second))
	if err != nil {
		t.Fatalf("hms.New after restart: %v", err)
	}
	defer c2.Close()

	if got, err := c2.GetDatabase(ctx2, db); err != nil {
		t.Fatalf("GetDatabase after restart: %v", err)
	} else if got.Name != db {
		t.Fatalf("GetDatabase after restart name=%q want %q", got.Name, db)
	}
	if got, err := c2.GetTable(ctx2, db, renamed); err != nil {
		t.Fatalf("GetTable after restart: %v", err)
	} else if got.TableName != renamed {
		t.Fatalf("GetTable after restart name=%q want %q", got.TableName, renamed)
	}
	if got, err := c2.GetPartitions(ctx2, db, renamed, -1); err != nil {
		t.Fatalf("GetPartitions after restart: %v", err)
	} else if len(got) != 2 {
		t.Fatalf("GetPartitions after restart len=%d want 2", len(got))
	}
	if names, err := c2.GetPartitionNames(ctx2, db, renamed, -1); err != nil {
		t.Fatalf("GetPartitionNames after restart: %v", err)
	} else if len(names) != 2 {
		t.Fatalf("GetPartitionNames after restart len=%d want 2", len(names))
	}

	// Clean up (best-effort: names are run-scoped, so a failure is not fatal).
	if err := c2.DropTable(ctx2, db, renamed, false, true); err != nil {
		t.Logf("cleanup DropTable: %v", err)
	}
	if err := c2.DropDatabase(ctx2, db, false, true, true); err != nil {
		t.Logf("cleanup DropDatabase: %v", err)
	}
}
