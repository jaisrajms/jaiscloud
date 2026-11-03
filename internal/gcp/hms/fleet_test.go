package hms

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	hmsstore "jaiscloud/internal/gcp/store/hms"
)

// freePort returns a currently free localhost TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// TestFleetServesOneCatalogPerService is the serving-plane acceptance test: two
// Services in different projects/locations — each with its own listener on its
// own port — create the same database and table names without colliding, get
// their own `default`, and do not see each other's Iceberg locks.
func TestFleetServesOneCatalogPerService(t *testing.T) {
	cats := hmsstore.NewMemoryCatalogs(nil)
	fleet := NewFleet(cats)
	defer fleet.Stop()

	idA := hmsstore.CatalogID("proj-a", "us-central1", "hms")
	idB := hmsstore.CatalogID("proj-b", "europe-west1", "hms")
	portA, portB := freePort(t), freePort(t)
	if portA == portB {
		t.Skip("free ports collided")
	}
	for id, port := range map[string]int{idA: portA, idB: portB} {
		if err := fleet.Open(id, port); err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
	}
	addrA := net.JoinHostPort("127.0.0.1", strconv.Itoa(portA))
	addrB := net.JoinHostPort("127.0.0.1", strconv.Itoa(portB))

	createDefault := func(addr, location string) {
		t.Helper()
		args := NewBuilder().Struct(1, NewBuilder().
			Str(dbName, "default").
			Str(dbLocationURI, location).
			Build()).Build()
		if r := hmsCall(t, addr, "create_database", args); r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
			t.Fatalf("create_database on %s: type=%d result=%+v", addr, r.msgType, r.result)
		}
	}
	createDefault(addrA, "gs://a/default")
	// The second Service's `default` must not collide with the first's.
	createDefault(addrB, "gs://b/default")

	createTable := func(addr, metadataLocation string) {
		t.Helper()
		args := NewBuilder().Struct(1, testTable("default", "t1", metadataLocation)).Build()
		if r := hmsCall(t, addr, "create_table", args); r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
			t.Fatalf("create_table on %s: type=%d result=%+v", addr, r.msgType, r.result)
		}
	}
	// Same database and table name in both catalogs.
	createTable(addrA, "gs://a/default/t1/metadata/00001.metadata.json")
	createTable(addrB, "gs://b/default/t1/metadata/00001.metadata.json")

	getTable := func(addr string) *Struct {
		t.Helper()
		args := NewBuilder().Str(1, "default").Str(2, "t1").Build()
		r := hmsCall(t, addr, "get_table", args)
		if r.msgType != thrift.REPLY {
			t.Fatalf("get_table on %s: type=%d result=%+v", addr, r.msgType, r.result)
		}
		return r.result.Struct(0)
	}
	tblA, tblB := getTable(addrA), getTable(addrB)
	if tblA == nil || tblB == nil {
		t.Fatalf("get_table returned no table (a=%v b=%v)", tblA, tblB)
	}
	ma := tblA.MapStrStr(9)["metadata_location"]
	mb := tblB.MapStrStr(9)["metadata_location"]
	if ma == mb {
		t.Fatalf("both Services read the same table record: %q", ma)
	}

	// A database only one Service created is invisible to the other.
	dbOnlyA := NewBuilder().Struct(1, NewBuilder().
		Str(dbName, "analytics").
		Str(dbLocationURI, "gs://a/analytics").
		Build()).Build()
	if r := hmsCall(t, addrA, "create_database", dbOnlyA); r.msgType != thrift.REPLY || len(r.result.Fields) != 0 {
		t.Fatalf("create analytics on a: %+v", r.result)
	}
	r := hmsCall(t, addrB, "get_database", NewBuilder().Str(1, "analytics").Build())
	if r.msgType != thrift.REPLY {
		t.Fatalf("get_database(analytics) on b msgType=%d", r.msgType)
	}
	if f := r.result.Fields; len(f) == 0 || f[0].ID != 1 {
		t.Fatalf("b saw a's database: %+v", f)
	}

	// Iceberg commit locks are per catalog: a lock held on one Service does not
	// block the other (it is not even visible).
	comp := NewBuilder().I32(1, 3).I32(2, 2).Str(3, "default").Str(4, "t1").Build()
	lockReq := NewBuilder().Add(1, ListV(thrift.STRUCT, []Value{StructV(comp)})).Str(3, "spark").Str(4, "host").Build()
	lr := hmsCall(t, addrA, "lock", NewBuilder().Struct(1, lockReq).Build())
	lockID := lr.result.Struct(0).I64(lockRespLockID)
	if lockID == 0 {
		t.Fatalf("lock on a did not return an id: %+v", lr.result)
	}
	check := NewBuilder().I64(1, lockID).Build()
	if got := hmsCall(t, addrA, "check_lock", NewBuilder().Struct(1, check).Build()).result.Struct(0).I32(lockRespState); got != int32(hmsstore.LockStateAcquired) {
		t.Fatalf("a's own lock state = %d, want ACQUIRED", got)
	}
	if got := hmsCall(t, addrB, "check_lock", NewBuilder().Struct(1, check).Build()).result.Struct(0).I32(lockRespState); got != int32(hmsstore.LockStateNotAcquired) {
		t.Fatalf("b sees a's lock as %d, want NOT_ACQUIRED", got)
	}

	// Deleting a Service closes its listener and drops its catalog; the other
	// Service keeps serving.
	fleet.Close(idA)
	cats.DropCatalog(context.Background(), idA)
	if _, err := net.Dial("tcp", addrA); err == nil {
		t.Fatal("closed catalog still accepts connections")
	}
	if tbl := getTable(addrB); tbl == nil {
		t.Fatal("closing a's catalog affected b")
	}
}
